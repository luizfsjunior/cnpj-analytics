"""DAG da carga mensal do cnpj-analytics.

Contrato em [`airflow/spec-dag-carga.md`](../spec-dag-carga.md); testes em
`airflow/tests/`. Esta DAG **chama**, não reescreve: o retry do download é do
`watcher.py`, as seis fases são do `load.sh`, e as regras de sanitização são do
transform. Qualquer lógica de carga que apareça aqui é um contrato com dois
donos — leia a spec antes.

Um módulo só, de propósito: as decisões puras no topo, os operators embaixo.

Fluxo:

    listar_meses ──▶ detectar_mes ──▶ baixar_zips ──▶ carregar ──▶ conferir_desfecho
                          │                                             │
                          └─(sem mês novo: pula)                        │
                                                                        ▼
                                                     recuperar_indices  (all_done)
"""

from __future__ import annotations

import json
import logging
import os
from datetime import timedelta

import pendulum
from airflow.sdk import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.smtp.notifications.smtp import send_smtp_notification
from airflow.providers.standard.operators.python import (
    PythonOperator,
    ShortCircuitOperator,
)
from docker.types import Mount

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constantes
#
# O orçamento é constante, NÃO `os.getenv`: o `.env` do servidor não é visível
# para a DAG, e herdar dele faria a carga rodar com o orçamento da máquina de
# desenvolvimento (16 GB) num host compartilhado e sem swap, onde passar do teto
# não é lentidão — é OOM kill, e a vítima pode ser o Postgres de outra stack.
# (spec R3)
# ---------------------------------------------------------------------------

# Os quatro valores abaixo são CONSTANTES, e não `os.getenv`. Já foram
# `os.getenv("CNPJ_*")` por um dia (18/09 a 22/09/2026), para medir a carga numa
# máquina de desenvolvimento sem teto; a brecha foi fechada porque ela falhava
# calada nos dois sentidos:
#
#   * quem tivesse `CNPJ_ORCAMENTO_RAM_MB` no ambiente do Airflow mandaria esse
#     número para o container da carga sem que nada no log dissesse de onde ele
#     veio — num host de 16 GB sem swap, passar do teto não é lentidão, é OOM
#     kill, e a vítima pode ser o Postgres de outra stack;
#   * o próprio T8, que existe para guardar o R3, deixava de pegar o desvio: ele
#     lê estas constantes, que já viriam contaminadas pelo ambiente. Foi assim
#     que a brecha apareceu — o T8 ficou vermelho dentro do Airflow local, que
#     define 18432.
#
# Para medir numa máquina sem teto, edite aqui e não comite. É chato de
# propósito.
ORCAMENTO_RAM_MB = "3072"
ORCAMENTO_VCPU = "4"

# Os dois defaults do `load.sh`, repetidos aqui para serem passados
# EXPLICITAMENTE (R3: o que a carga usa não se herda do ambiente de quem a
# chamou). `SHARED_BUFFERS_MB` não configura nada — é o quanto o script DESCONTA
# do orçamento por conta do shared_buffers do Postgres, então ele tem de
# ESPELHAR o valor real do banco (1 GB no compose deste repo). Mentir aqui é
# convite a OOM.
MAX_PARALLEL_MAINT = "1"
SHARED_BUFFERS_MB = "1024"

DB_DESTINO = os.getenv("CNPJ_DB", "cnpj_full")
DATA_DIR_CONTAINER = os.getenv("CNPJ_DATA_DIR", "/data")

# A imagem que o CI/CD do repo constrói. A DAG fixa uma tag imutável (o SHA do
# commit) em vez de `latest`: um deploy no meio de uma carga não afeta o
# container em execução, mas um retry que pegasse `latest` rodaria outro código
# no meio do mesmo mês.
IMAGEM_CARGA = os.getenv("CNPJ_CARGA_IMAGE", "cnpj-carga:latest")

# A rede onde vive o `postgres-cnpj-rfb`. No servidor é a `services-net`, criada
# fora deste projeto; no Airflow local é a rede do compose do repo.
REDE_CARGA = os.getenv("CNPJ_REDE", "services-net")

# Pasta dos zips no HOST, montada como /data no container da carga. É a mesma
# `CNPJ_HOST_DATA_DIR` do compose do repo — fora da árvore de deploy, para o
# `rsync --delete` do CI/CD não encostar nela.
HOST_DATA_DIR = os.getenv("CNPJ_HOST_DATA_DIR", "/opt/applications/cnpj-analytics/data")

POOL_CARGA = "cnpj_carga"

# ---------------------------------------------------------------------------
# Alerta de falha (R8, decidido em 22/09/2026 — seção 9.4 da spec)
#
# O canal é o que a instalação do Airflow já tem, não um novo: a conexão SMTP
# `email_notificacao` e o precedente da DAG do comparador Protheus × Receita
# (`send_smtp_notification` em `on_failure_callback`).
# ---------------------------------------------------------------------------

CONEXAO_SMTP = "email_notificacao"

# O destinatário fica numa Airflow Variable, não em código: um e-mail escrito
# aqui continua avisando quem já saiu da equipe.
VARIABLE_EMAIL_AVISOS = "cnpj_carga_email_avisos"

# ---------------------------------------------------------------------------
# Params — a única entrada variável da run
#
# São os defaults de produção; `--conf` os sobrescreve numa run específica
# (`core.dag_run_conf_overrides_params`, ligado por default). Existem por causa
# do T10, que roda a DAG inteira contra um banco descartável e em amostra — sem
# isto, o único jeito de testar ponta a ponta seria reiniciar o Airflow com
# outro ambiente, e o teste não teria como variar o parâmetro numa sessão.
#
# Servem também para o disparo à mão do cutover (spec, seção 8.2).
#
# `data_dir` é a pasta dos zips no HOST (fonte do bind mount). Dentro do
# container ela é sempre `/data` — `DATA_DIR_CONTAINER` não é parametrizável,
# porque quem a lê é o `load.sh`, que não tem por que saber onde o host guarda
# as coisas.
#
# O orçamento NÃO está aqui: é constante por decisão (spec R3). Um param de
# RAM é um param que alguém sobe "só desta vez" num host sem swap.
# ---------------------------------------------------------------------------

PARAMS_PADRAO = {
    "db": DB_DESTINO,
    "data_dir": HOST_DATA_DIR,
    # Vazio = base completa. O `load.sh` faz `SAMPLE="${SAMPLE:-0}"`, então
    # string vazia e ausência dão no mesmo.
    "sample": "",
}

# 20h é o `LOAD_TIMEOUT_H` que o watcher usava. A carga completa medida é 3h38;
# o teto existe para o caso patológico, não é a expectativa.
TIMEOUT_CARGA = timedelta(hours=20)


# ---------------------------------------------------------------------------
# Decisões puras — sem rede, sem disco, sem Airflow
# ---------------------------------------------------------------------------

class CargaFalhou(Exception):
    """A carga do mês não terminou em estado aproveitável."""


# A pergunta "qual foi o último mês carregado" é respondida pelo próprio banco.
# Antes vinha de um `state.json` num volume nomeado — e um volume recriado vazio
# fazia o watcher disparar uma carga completa do nada. `carga.resumo` já existe,
# já é escrita pela carga e é o mesmo dado que a auditoria usa. (spec R6)
SQL_ULTIMA_COMPETENCIA = """
    SELECT max(competencia)
      FROM carga.resumo
     WHERE desfecho IN ('sucesso', 'degradado')
"""

SQL_DESFECHO_DA_COMPETENCIA = """
    SELECT desfecho
      FROM carga.resumo
     WHERE competencia = %s
     ORDER BY inicio DESC
     LIMIT 1
"""

DESFECHOS_OK = ("sucesso", "degradado")


def proxima_competencia(disponiveis: list[str], ultima: str | None) -> str | None:
    """O mês a carregar, ou None quando não há nada novo.

    `disponiveis` vem do share da Receita, `ultima` de `carga.resumo`. A
    comparação é lexicográfica porque o formato é AAAA-MM, em que ordem
    alfabética e cronológica coincidem.
    """
    if not disponiveis:
        return None
    mais_recente = max(disponiveis)
    if ultima is not None and mais_recente <= ultima:
        return None
    return mais_recente


def avaliar_desfecho(desfecho: str | None, competencia: str) -> str:
    """Traduz `carga.resumo.desfecho` no resultado da run.

    `degradado` é **sucesso**: é o mês em que a Receita publicou chave natural
    repetida. O índice sai não-único, as chaves vão para `carga.duplicata` e a
    carga terminou. Não é motivo para recarregar — a carga seguinte volta ao
    índice único sozinha se o mês vier limpo. (spec R5)

    Ausência de linha também reprova: significa que a carga morreu antes de
    fechar o resumo, e um resumo em aberto não é sucesso silencioso.
    """
    if desfecho not in DESFECHOS_OK:
        raise CargaFalhou(
            f"a carga de {competencia} terminou em {desfecho!r}; "
            f"esperado um de {DESFECHOS_OK}"
        )
    if desfecho == "degradado":
        log.warning(
            "carga de %s terminou DEGRADADA: a Receita publicou chave natural "
            "repetida e algum índice ficou não-único. As chaves estão em "
            "carga.duplicata. Não recarregue — o mês seguinte se corrige "
            "sozinho se vier limpo.",
            competencia,
        )
    return desfecho


def env_carga(competencia: str, params: dict[str, str] | None = None) -> dict[str, str]:
    """O ambiente do container da carga.

    `competencia` é obrigatória e vem da detecção, nunca do relógio: uma carga
    que começa às 22h do dia 30 atravessa a meia-noite, e o default "mês
    corrente" do `load.sh` rotularia a carga no mês errado — a run seguinte
    acharia que o mês novo ainda não foi carregado. (spec T4)

    `params` sobrescreve `db` e `sample`. Na DAG ele chega como expressões
    Jinja (`{{ params.db }}`), resolvidas pelo Airflow no momento da execução —
    é assim que o `--conf` do T10 chega ao container. Chamado sem `params`,
    devolve os defaults de produção.
    """
    p = {**PARAMS_PADRAO, **(params or {})}
    return {
        # Os dois nomes da MESMA pasta, e ambos são obrigatórios: o `load.sh`
        # lê `DATA_DIR`, o `watcher.py` lê `CNPJ_DATA_DIR` (e cai num default
        # `../minha-receita/data` quando ela falta — fora do bind mount, o que
        # faria `baixar_zips` gravar 37 zips num lugar que morre com o
        # container). Mesma história do `CNPJ_DB`.
        "DB": p["db"],
        "CNPJ_DB": p["db"],
        "DATA_DIR": DATA_DIR_CONTAINER,
        "CNPJ_DATA_DIR": DATA_DIR_CONTAINER,
        "SAMPLE": p["sample"],
        "COMPETENCIA": competencia,
        "ORCAMENTO_RAM_MB": ORCAMENTO_RAM_MB,
        "ORCAMENTO_VCPU": ORCAMENTO_VCPU,
        # Os dois defaults do `load.sh`, passados EXPLICITAMENTE (R3: o que a
        # carga usa não se herda do ambiente de quem a chamou). `SHARED_BUFFERS_MB`
        # não configura nada — é o quanto o script DESCONTA do orçamento por
        # conta do shared_buffers do Postgres, então mentir aqui é convite a OOM.
        "MAX_PARALLEL_MAINT": MAX_PARALLEL_MAINT,
        "SHARED_BUFFERS_MB": SHARED_BUFFERS_MB,
        "PGHOST": os.getenv("PGHOST", "postgres-cnpj-rfb"),
        "PGPORT": os.getenv("PGPORT", "5432"),
        "PGUSER": os.getenv("PGUSER", "cnpj"),
        "PGPASSWORD": os.getenv("PGPASSWORD", "cnpj"),
    }


# ---------------------------------------------------------------------------
# Acesso ao banco e ao repo
# ---------------------------------------------------------------------------

def _conectar(dbname: str | None = None):
    """Conexão com o banco da carga. Importa psycopg2 tarde para que o parse da
    DAG não dependa dele.

    `dbname` vem dos params da run: o T10 carrega num banco descartável, e ler
    `carga.resumo` do `cnpj_full` enquanto a amostra foi para outro lugar daria
    um verde que não prova nada.
    """
    import psycopg2

    return psycopg2.connect(
        host=os.getenv("PGHOST", "postgres-cnpj-rfb"),
        port=int(os.getenv("PGPORT", "5432")),
        user=os.getenv("PGUSER", "cnpj"),
        password=os.getenv("PGPASSWORD", "cnpj"),
        dbname=dbname or DB_DESTINO,
        connect_timeout=10,
    )


# ---------------------------------------------------------------------------
# Callbacks
# ---------------------------------------------------------------------------

# Falha tem de chegar onde a equipe lê — não ao `journalctl`. Vai em
# `default_args`, e não task a task: posto task a task vira uma lista que
# alguém esquece de repetir na task seguinte, e a task esquecida é justamente a
# que falha calada.
#
# O remetente é dito EXPLICITAMENTE: o `extra` da conexão `email_notificacao`
# está vazio (medido em 10/09/2026, na mesma instalação, pela DAG do
# comparador) e o SmtpHook não tem fallback — sem `from_email`, o envio morre
# em "You should provide `from_email`", e o alerta falharia na hora de
# alertar. `{{ conn.email_notificacao.login }}` resolve para a própria caixa
# autenticada da conexão, no runtime da task: se a caixa mudar, muda num lugar
# só (a connection), sem tocar este arquivo.
ALERTA_FALHA = send_smtp_notification(
    smtp_conn_id=CONEXAO_SMTP,
    from_email="{{ conn.email_notificacao.login }}",
    to="{{ var.value.cnpj_carga_email_avisos }}",
    subject="[CNPJ] falha em {{ ti.task_id }} — {{ ti.dag_id }}",
    html_content=(
        "A task <b>{{ ti.task_id }}</b> da DAG {{ ti.dag_id }} falhou na "
        "execução de {{ ts }}.<br>"
        "Log: {{ ti.log_url }}<br><br>"
        "Se a falha foi na task <code>carregar</code>, a base pode ter passado "
        "pela janela sem índice — confira se <code>recuperar_indices</code> "
        "rodou."
    ),
)


# ---------------------------------------------------------------------------
# Callables das tasks
# ---------------------------------------------------------------------------

def detectar_mes_novo(**context) -> str | None:
    """Devolve a competência a carregar, ou None para curto-circuitar a run.

    A lista de meses do share chega pronta, pelo XCom de `listar_meses` (9.2):
    quem fala com o share é a imagem da carga, não o worker — esta função só
    lê `carga.resumo` e compara. Continua em processo porque é onde vive o
    `ShortCircuitOperator`; um `DockerOperator` não pula tasks a jusante.

    Retornar None **pula** as tasks seguintes em vez de falhar: um alerta por
    dia em que a Receita não publicou nada treina a equipe a ignorar o alerta
    que importa.
    """
    # O XCom do DockerOperator é a última linha do STDOUT — uma STRING, não a
    # lista já desserializada. Sem o json.loads, `disponiveis` seria a string
    # '["2026-09"]', e `max()` sobre ela devolveria o CARACTERE de maior
    # código, não o mês mais recente — bug real, pego rodando o T10 com dado
    # de verdade em 22/09/2026 (o download foi pedido para o mês "]").
    bruto = context["ti"].xcom_pull(task_ids="listar_meses")
    disponiveis = json.loads(bruto) if bruto else []
    log.info("meses no share: %s", disponiveis or "(nenhum)")

    banco = context["params"]["db"]
    with _conectar(banco) as conn, conn.cursor() as cur:
        cur.execute(SQL_ULTIMA_COMPETENCIA)
        (ultima,) = cur.fetchone()
    log.info("último mês carregado (carga.resumo de %s): %s", banco, ultima)

    mes = proxima_competencia(disponiveis, ultima)
    if mes is None:
        log.info("nada novo a carregar — run encerrada sem erro.")
        return None

    log.info("mês novo detectado: %s", mes)
    return mes


def conferir_desfecho_da_carga(**context) -> str:
    """Lê `carga.resumo` e decide o resultado da run.

    É proibido decidir pelo código de saída do container: `degradado` sai com 0
    e significa outra coisa, e um container morto por fora não deixa código
    algum. (spec R5)
    """
    competencia = context["ti"].xcom_pull(task_ids="detectar_mes")

    with _conectar(context["params"]["db"]) as conn, conn.cursor() as cur:
        cur.execute(SQL_DESFECHO_DA_COMPETENCIA, (competencia,))
        linha = cur.fetchone()

    desfecho = linha[0] if linha else None
    log.info("carga.resumo de %s: desfecho=%r", competencia, desfecho)
    return avaliar_desfecho(desfecho, competencia)


# ---------------------------------------------------------------------------
# A DAG
# ---------------------------------------------------------------------------

_COMPETENCIA = "{{ ti.xcom_pull(task_ids='detectar_mes') }}"

# Os params como Jinja: `image`, `command`, `environment` e `mounts` são campos
# templated do DockerOperator, então o valor efetivo é resolvido na execução —
# é o que deixa o `--conf` de uma run chegar ao container sem que a DAG tenha
# de ser reparseada com outro ambiente.
_PARAMS_JINJA = {chave: "{{ params.%s }}" % chave for chave in PARAMS_PADRAO}

class CargaDockerOperator(DockerOperator):
    """`DockerOperator` sem `template_ext`.

    O `DockerOperator` declara `template_ext = ('.sh', '.bash', '.env')`, e
    `command` é campo templated. Com isso o Airflow lê `"analytics/load.sh"`
    como **caminho de um arquivo de template** relativo à pasta de dags — a
    task morre com `TemplateNotFound` onde o script não existe (o servidor, já
    que a DAG mora fora do repo) e, onde existir, o conteúdo inteiro do script
    entraria no lugar do argumento.

    Zerar `template_ext` mantém `command` como argumento e preserva o Jinja dos
    params, que é o que importa aqui. Descoberto renderizando a task de fato —
    nenhum dos testes de forma pega isto.
    """

    template_ext = ()


_montagens = [
    Mount(source=_PARAMS_JINJA["data_dir"], target=DATA_DIR_CONTAINER, type="bind")
]

_docker_comum = dict(
    image=IMAGEM_CARGA,
    network_mode=REDE_CARGA,
    mounts=_montagens,
    # `success` e não `force`: um container que falhou fica de pé para autópsia.
    # Numa carga de horas, perder o container é perder a única pista.
    auto_remove="success",
    working_dir="/app",
    # Nenhuma task usa o scratch dir que o DockerOperator monta por padrão (o
    # XCom sai do stdout, não de arquivo). Explícito, e não o default: contra
    # um engine remoto (o Airflow local fala com o Docker Desktop por
    # named pipe/TCP, não socket local), o fallback automático do provider foi
    # medido como INCONSISTENTE em 22/09/2026 — o aviso de "Falling back to
    # mount_tmp_dir=False" apareceu em toda task, mas só ALGUMAS de fato
    # honraram o fallback; as outras tentaram montar um dir temporário do host
    # que não existia e morreram com "bind source path does not exist".
    mount_tmp_dir=False,
)

with DAG(
    dag_id="cnpj_carga_mensal",
    description="Carga mensal dos dados abertos de CNPJ da Receita Federal",
    # Substitui o par CHECK_INTERVAL_H=24 + LOAD_AFTER_HOUR=22 do watcher, que
    # era um cron artesanal: às 22h, para não pesar no horário comercial.
    schedule="0 22 * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="America/Sao_Paulo"),
    # Ligar a DAG com start_date antigo enfileiraria uma carga de horas por dia
    # perdido — e cada uma delas carregaria o MESMO mês corrente.
    catchup=False,
    # Duas cargas concorrentes disputam o schema `staging`, o
    # `carga.indice_salvo` (única cópia da DDL dos 212 índices) e o orçamento de
    # RAM de um host sem swap. (spec R1)
    max_active_runs=1,
    params=PARAMS_PADRAO,
    default_args={"on_failure_callback": ALERTA_FALHA},
    tags=["cnpj", "carga"],
    doc_md=__doc__,
) as dag:

    # Decidida em 22/09/2026 (9.2): a ida ao share sai do worker e vai para a
    # imagem da carga — o mesmo lugar de onde vêm `baixar_zips` e `carregar`,
    # com a mesma tag de SHA. Elimina a dependência de um repo montado no
    # Airflow (que o servidor não tem) e a possibilidade de a detecção rodar
    # uma versão do watcher e a carga, outra. Chama `fetch_available_months`
    # (R2) — nada da listagem WebDAV reescrito aqui.
    #
    # Sem `environment`: fetch_available_months() não fala com o banco nem
    # precisa do orçamento — só do share, cuja URL e token são constantes do
    # próprio watcher.py.
    #
    # Gotcha: o XCom do DockerOperator é a ÚLTIMA LINHA do stdout. O comando
    # não pode imprimir nada depois do JSON.
    listar_meses = CargaDockerOperator(
        task_id="listar_meses",
        command=[
            "python",
            "-c",
            "import json; from watcher.watcher import fetch_available_months; "
            "print(json.dumps(fetch_available_months()))",
        ],
        do_xcom_push=True,
        retries=3,
        **_docker_comum,
    )

    detectar_mes = ShortCircuitOperator(
        task_id="detectar_mes",
        python_callable=detectar_mes_novo,
        # Sem isto, o curto-circuito pularia também a `recuperar_indices`,
        # ignorando o `all_done` dela. Ela é barata e idempotente; deixá-la
        # rodar é mais seguro do que confiar que nada ficou pela metade.
        ignore_downstream_trigger_rules=False,
        retries=3,
    )

    # O download NÃO é reimplementado aqui. Ele roda na imagem da carga, que tem
    # o watcher e o volume dos zips, e o que se chama é a mesma `download_month`
    # de sempre: 7 tentativas por requisição, retomada do que já veio, e uma 2ª
    # passada no fim. O share da Receita derruba de 22% a 35% das conexões, e o
    # retry de task inteira é grosso demais para 37 arquivos. (spec R2)
    baixar_zips = CargaDockerOperator(
        task_id="baixar_zips",
        command=[
            "python",
            "-c",
            "import sys; from watcher.watcher import download_month; "
            f"sys.exit(0 if download_month('{_COMPETENCIA}') else 1)",
        ],
        environment=env_carga(_COMPETENCIA, _PARAMS_JINJA),
        retries=1,
        **_docker_comum,
    )

    # Container próprio, não o worker do Airflow: reiniciar ou implantar o
    # Airflow no meio de uma carga de 4h não pode matá-la — e matá-la entre as
    # Fases 2 e 4 deixa a base sem índice. (spec R7)
    carregar = CargaDockerOperator(
        task_id="carregar",
        command=["bash", "analytics/load.sh"],
        environment=env_carga(_COMPETENCIA, _PARAMS_JINJA),
        pool=POOL_CARGA,
        execution_timeout=TIMEOUT_CARGA,
        # Zero, e é deliberado: retentar 4 horas de carga às 2 da manhã pode ser
        # pior do que não retentar. Falhou, alerta e espera a próxima janela.
        # (spec R8)
        retries=0,
        **_docker_comum,
    )

    conferir_desfecho = PythonOperator(
        task_id="conferir_desfecho",
        python_callable=conferir_desfecho_da_carga,
    )

    # A rede da rede. O `trap EXIT` do `load.sh` já chama este script, mas um
    # trap é garantia de PROCESSO: se o Airflow matar o container (timeout,
    # zombie, restart), ele pode não completar — e a base ficaria em seq scan
    # sobre 73 milhões de linhas até alguém agir. Idempotente, ~1s quando não há
    # o que recuperar. (spec R4)
    recuperar_indices = CargaDockerOperator(
        task_id="recuperar_indices",
        command=["bash", "analytics/recuperar_indices.sh"],
        environment=env_carga(_COMPETENCIA, _PARAMS_JINJA),
        trigger_rule="all_done",
        retries=2,
        **_docker_comum,
    )

    listar_meses >> detectar_mes >> baixar_zips >> carregar >> conferir_desfecho
    carregar >> recuperar_indices


# ---------------------------------------------------------------------------
# Requisitos de implantação
#
# 1. A imagem `CNPJ_CARGA_IMAGE` tem de existir no daemon que o Airflow usa. O
#    CI/CD do repo a constrói no runner self-hosted, que é o próprio servidor —
#    por isso não há registry.
# 2. O Airflow precisa alcançar o socket do Docker (DockerOperator) e a rede
#    `CNPJ_REDE` (a task que lê `carga.resumo`).
# 3. O pool `cnpj_carga` tem de existir com 1 slot:
#       airflow pools import airflow/pools.json
# 4. A Airflow Variable `cnpj_carga_email_avisos` tem de existir, com o e-mail
#    (ou lista) que recebe o alerta de falha (R8). A conexão SMTP
#    `email_notificacao` já existe na instalação.
#
# Nenhum repo precisa estar montado no Airflow: todo o código do watcher (o que
# lista os meses, o que baixa e o que carrega) mora na imagem da carga —
# decidido em 22/09/2026 depois de medir que o Airflow do servidor não monta
# repo nenhum (spec, seção 9.2).
# ---------------------------------------------------------------------------
