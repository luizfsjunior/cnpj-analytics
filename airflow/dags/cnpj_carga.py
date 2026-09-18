"""DAG da carga mensal do cnpj-analytics.

Contrato em [`airflow/spec-dag-carga.md`](../spec-dag-carga.md); testes em
`airflow/tests/`. Esta DAG **chama**, não reescreve: o retry do download é do
`watcher.py`, as seis fases são do `load.sh`, e as regras de sanitização são do
transform. Qualquer lógica de carga que apareça aqui é um contrato com dois
donos — leia a spec antes.

Um módulo só, de propósito: as decisões puras no topo, os operators embaixo.

Fluxo:

    detectar_mes ──▶ baixar_zips ──▶ carregar ──▶ conferir_desfecho
          │                                             │
          └─(sem mês novo: pula)                        │
                                                        ▼
                                         recuperar_indices  (all_done)
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import timedelta

import pendulum
from airflow.sdk import DAG
from airflow.providers.docker.operators.docker import DockerOperator
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

ORCAMENTO_RAM_MB = "3072"
ORCAMENTO_VCPU = "4"

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

# Onde o repo está montado DENTRO do Airflow. `detectar_mes` roda em processo e
# importa o watcher daqui; as demais tasks usam a imagem da carga, que já tem o
# código. Ver "Requisitos de implantação" no fim do arquivo.
REPO_DIR = os.getenv("CNPJ_REPO_DIR", "/opt/cnpj-analytics")

POOL_CARGA = "cnpj_carga"

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


def env_carga(competencia: str) -> dict[str, str]:
    """O ambiente do container da carga.

    `competencia` é obrigatória e vem da detecção, nunca do relógio: uma carga
    que começa às 22h do dia 30 atravessa a meia-noite, e o default "mês
    corrente" do `load.sh` rotularia a carga no mês errado — a run seguinte
    acharia que o mês novo ainda não foi carregado. (spec T4)
    """
    return {
        "DB": DB_DESTINO,
        "DATA_DIR": DATA_DIR_CONTAINER,
        "COMPETENCIA": competencia,
        "ORCAMENTO_RAM_MB": ORCAMENTO_RAM_MB,
        "ORCAMENTO_VCPU": ORCAMENTO_VCPU,
        "PGHOST": os.getenv("PGHOST", "postgres-cnpj-rfb"),
        "PGPORT": os.getenv("PGPORT", "5432"),
        "PGUSER": os.getenv("PGUSER", "cnpj"),
        "PGPASSWORD": os.getenv("PGPASSWORD", "cnpj"),
    }


# ---------------------------------------------------------------------------
# Acesso ao banco e ao repo
# ---------------------------------------------------------------------------

def _conectar():
    """Conexão com o banco da carga. Importa psycopg2 tarde para que o parse da
    DAG não dependa dele."""
    import psycopg2

    return psycopg2.connect(
        host=os.getenv("PGHOST", "postgres-cnpj-rfb"),
        port=int(os.getenv("PGPORT", "5432")),
        user=os.getenv("PGUSER", "cnpj"),
        password=os.getenv("PGPASSWORD", "cnpj"),
        dbname=DB_DESTINO,
        connect_timeout=10,
    )


def _importar_watcher():
    """O watcher como BIBLIOTECA.

    Import tardio e por caminho: se o repo não estiver montado, quem falha é a
    task — não o parse da DAG, que derrubaria a DAG inteira da UI por um
    problema de implantação.
    """
    if REPO_DIR not in sys.path:
        sys.path.insert(0, REPO_DIR)
    from watcher import watcher

    return watcher


# ---------------------------------------------------------------------------
# Callbacks
# ---------------------------------------------------------------------------

def alertar_falha(context) -> None:
    """Falha tem de chegar onde a equipe lê — não ao `journalctl`.

    O canal ainda está em aberto (seção 8 da spec). Até ele ser decidido, isto
    grava um ERROR nomeado, que é o que o log agrega. Trocar por webhook/e-mail
    é mudança de uma função.
    """
    ti = context.get("task_instance")
    log.error(
        "CARGA CNPJ: a task %s falhou (tentativa %s). Se a falha foi na task "
        "`carregar`, a base pode ter passado pela janela sem índice — confira "
        "se `recuperar_indices` rodou.",
        getattr(ti, "task_id", "?"),
        getattr(ti, "try_number", "?"),
    )


# ---------------------------------------------------------------------------
# Callables das tasks
# ---------------------------------------------------------------------------

def detectar_mes_novo() -> str | None:
    """Devolve a competência a carregar, ou None para curto-circuitar a run.

    Retornar None **pula** as tasks seguintes em vez de falhar: um alerta por
    dia em que a Receita não publicou nada treina a equipe a ignorar o alerta
    que importa.
    """
    watcher = _importar_watcher()

    disponiveis = watcher.fetch_available_months()
    log.info("meses no share: %s", disponiveis or "(nenhum)")

    with _conectar() as conn, conn.cursor() as cur:
        cur.execute(SQL_ULTIMA_COMPETENCIA)
        (ultima,) = cur.fetchone()
    log.info("último mês carregado (carga.resumo): %s", ultima)

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

    with _conectar() as conn, conn.cursor() as cur:
        cur.execute(SQL_DESFECHO_DA_COMPETENCIA, (competencia,))
        linha = cur.fetchone()

    desfecho = linha[0] if linha else None
    log.info("carga.resumo de %s: desfecho=%r", competencia, desfecho)
    return avaliar_desfecho(desfecho, competencia)


# ---------------------------------------------------------------------------
# A DAG
# ---------------------------------------------------------------------------

_COMPETENCIA = "{{ ti.xcom_pull(task_ids='detectar_mes') }}"

_montagens = [Mount(source=HOST_DATA_DIR, target=DATA_DIR_CONTAINER, type="bind")]

_docker_comum = dict(
    image=IMAGEM_CARGA,
    network_mode=REDE_CARGA,
    mounts=_montagens,
    # `success` e não `force`: um container que falhou fica de pé para autópsia.
    # Numa carga de horas, perder o container é perder a única pista.
    auto_remove="success",
    working_dir="/app",
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
    default_args={"on_failure_callback": alertar_falha},
    tags=["cnpj", "carga"],
    doc_md=__doc__,
) as dag:

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
    baixar_zips = DockerOperator(
        task_id="baixar_zips",
        command=[
            "python",
            "-c",
            "import sys; from watcher.watcher import download_month; "
            f"sys.exit(0 if download_month('{_COMPETENCIA}') else 1)",
        ],
        environment=env_carga(_COMPETENCIA),
        retries=1,
        **_docker_comum,
    )

    # Container próprio, não o worker do Airflow: reiniciar ou implantar o
    # Airflow no meio de uma carga de 4h não pode matá-la — e matá-la entre as
    # Fases 2 e 4 deixa a base sem índice. (spec R7)
    carregar = DockerOperator(
        task_id="carregar",
        command=["bash", "analytics/load.sh"],
        environment=env_carga(_COMPETENCIA),
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
    recuperar_indices = DockerOperator(
        task_id="recuperar_indices",
        command=["bash", "analytics/recuperar_indices.sh"],
        environment=env_carga(_COMPETENCIA),
        trigger_rule="all_done",
        retries=2,
        **_docker_comum,
    )

    detectar_mes >> baixar_zips >> carregar >> conferir_desfecho
    carregar >> recuperar_indices


# ---------------------------------------------------------------------------
# Requisitos de implantação
#
# 1. A imagem `CNPJ_CARGA_IMAGE` tem de existir no daemon que o Airflow usa. O
#    CI/CD do repo a constrói no runner self-hosted, que é o próprio servidor —
#    por isso não há registry.
# 2. O Airflow precisa alcançar o socket do Docker (DockerOperator) e a rede
#    `CNPJ_REDE` (a task que lê `carga.resumo`).
# 3. O repo tem de estar montado em `CNPJ_REPO_DIR`, somente leitura: é de lá
#    que `detectar_mes` importa o watcher. As outras tasks não precisam — usam a
#    imagem da carga, que já traz o código.
# 4. O pool `cnpj_carga` tem de existir com 1 slot:
#       airflow pools import airflow/pools.json
# ---------------------------------------------------------------------------
