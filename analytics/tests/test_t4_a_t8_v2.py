"""T4 a T8 — o que a v2 tem de fazer e hoje não faz (spec-carga.md).

Estes testes nasceram **vermelhos de propósito**, cada um marcado com
`xfail(strict=True)`: enquanto a v2 não existia, o pytest reportava xfail e a
suíte ficava verde; no dia em que a implementação fizesse um deles passar, o
`strict` quebraria a suíte até o marcador ser removido.

Foi o que aconteceu em 16/09/2026. A v2 existe, os marcadores saíram, e estes
testes passaram de "o que falta fazer" para "o que não pode regredir".

Quem quebrar um deles não quebrou um teste: quebrou a parte da spec que ele
guarda — a auditoria do rejeito (T4), a rede que impede a API de passar a
madrugada sem índice (T5), o orçamento de um host sem swap (T6/T7/T12), a
quarentena que evita recarregar 6 horas por uma linha (T8), o determinismo dos
blocos (T10/T11) e a robustez a qualquer coisa que a Receita publique (T13/T14).
"""

import re
import subprocess
from pathlib import Path

import pytest

from fixture_carga import RAIZ, SQL, preparar_banco, rodar_transform

LOAD_SH_PATH = RAIZ / "analytics" / "load.sh"
LOAD_SH = LOAD_SH_PATH.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# T4 — o rejeito é contabilizado (R2)
# ---------------------------------------------------------------------------

def test_t4_linha_rejeitada_vai_para_carga_rejeito(psql_db):
    """As 8 linhas que a fixture rejeita têm de aparecer, com a regra certa.

    Hoje elas somem sem deixar registro — foi assim que 26.442 linhas foram
    descartadas em 2026-09 sem ninguém saber o que eram (seção 2.5).
    """
    preparar_banco(psql_db)
    rodar_transform(psql_db)

    r = psql_db.sql("SELECT regra, count(*) FROM carga.rejeito GROUP BY regra ORDER BY regra")
    assert r.returncode == 0, "schema carga.rejeito não existe"
    assert "S4" in r.stdout, "os cnpj_basico inválidos deveriam estar registrados como S4"
    assert "S5" in r.stdout, "os códigos de dimensão inválidos deveriam estar registrados como S5"
    assert "S9" in r.stdout, "o CNAE não numérico deveria estar registrado como S9"


def test_t4_resumo_tem_contador_por_regra(psql_db):
    preparar_banco(psql_db)
    rodar_transform(psql_db)
    r = psql_db.sql("SELECT count(*) FROM carga.resumo WHERE competencia IS NOT NULL")
    assert r.returncode == 0, "schema carga.resumo não existe"


def test_t4_rejeito_acima_do_limiar_avisa_mas_nao_aborta(rodar_load):
    """Acima de 0,1% de rejeito a carga avisa — e termina assim mesmo (7.1)."""
    # `com_rg=1` = stub de ripgrep que diz "nenhum match". Sem ele o script para
    # no check_deps e o teste mediria a ausência do rg, não o limiar.
    r = rodar_load(com_rg=1, extra_env={"REJEITO_LIMIAR_PCT": "0.0001"})
    assert r.returncode == 0, "a carga não pode abortar por excesso de rejeito"
    assert re.search(r"rejeito.*limiar", r.stdout, re.I), "faltou o aviso de limiar"


# ---------------------------------------------------------------------------
# T5 — recuperação pelo trap (7.1)
# ---------------------------------------------------------------------------

def test_t5_trap_recria_indices_em_saida_anormal(psql_db, bash_exe):
    """Matar a carga entre as Fases 2 e 4 não pode deixar a API sem índice.

    É o cenário que a 7.1 chama de único ponto em que o desenho piora o estado
    atual: falha depois do drop, com o watcher só retentando em 24 h e ninguém
    de madrugada para agir.
    """
    preparar_banco(psql_db)
    r = psql_db.arquivo(SQL / "04_indexes.sql")
    assert r.returncode == 0, r.stderr

    antes = psql_db.sql(
        "SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid=i.indrelid "
        "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='analytics'")
    assert antes.returncode == 0

    fase2 = SQL / "fase2_drop_indices.sql"
    assert fase2.exists(), "a Fase 2 da v2 não existe"
    psql_db.arquivo(fase2)

    recuperar = RAIZ / "analytics" / "recuperar_indices.sh"
    assert recuperar.exists(), "o passo de recuperação do trap não existe"
    # Duas correções de encanamento, ambas obrigatórias e nenhuma delas afeta o
    # que este teste afirma:
    #  - `DB`: sem ele o script recuperaria o banco `cnpj` de verdade, não o
    #    descartável deste teste — e a asserção abaixo nunca poderia passar;
    #  - `bash_exe`: `bash` puro no Windows é o launcher do WSL, que não herda o
    #    ambiente (a armadilha que o conftest documenta em detalhe).
    import os
    r = subprocess.run(
        [bash_exe, "analytics/recuperar_indices.sh"], cwd=str(RAIZ), timeout=300,
        env={**os.environ, "DB": psql_db.nome_banco},
        capture_output=True, text=True, encoding="utf-8", errors="replace")
    assert r.returncode == 0, f"a recuperação falhou:\n{r.stdout}\n{r.stderr}"

    depois = psql_db.sql(
        "SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid=i.indrelid "
        "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='analytics'")
    assert depois.stdout == antes.stdout, "os índices não voltaram ao estado anterior"


def test_t5_trap_do_load_sh_tem_fase_de_recuperacao():
    """Guarda estrutural: o `trap EXIT` precisa chamar a recuperação.

    Barato, e pega a regressão mais provável — alguém mexer no trap e tirar a
    recuperação sem perceber que ela era a única defesa da madrugada.
    """
    trap = re.search(r"trap\s+[\"']?([^\"'\n]+)[\"']?\s+EXIT", LOAD_SH)
    assert trap, "não há trap EXIT em load.sh"
    assert re.search(r"recupera|recria.*indice", trap.group(1), re.I), (
        f"o trap EXIT não chama a recuperação de índices: {trap.group(1)!r}")


# ---------------------------------------------------------------------------
# T6 / T7 — orçamento de recursos (R3, seção 7.3)
# ---------------------------------------------------------------------------

def test_t7_load_jobs_deriva_do_orcamento_e_nao_de_constante():
    """`LOAD_JOBS` tem de sair de nproc/memória, com teto 3 (seção 2.8)."""
    assert "LOAD_JOBS" in LOAD_SH, "load.sh não conhece LOAD_JOBS"
    bloco = LOAD_SH[LOAD_SH.index("LOAD_JOBS"):][:800]
    assert "nproc" in bloco or "vcpu" in bloco.lower(), "LOAD_JOBS não olha para a CPU disponível"
    assert re.search(r"\b3\b", bloco), "falta o teto de 3 jobs medido na seção 2.8"


def test_t7_work_mem_sai_do_teto_de_memoria():
    """`work_mem` é por OPERAÇÃO: com 3 jobs o pico multiplica, e não há swap."""
    assert "TUNE_RAM_GB" in LOAD_SH
    bloco = LOAD_SH[LOAD_SH.index("WORK_MEM"):][:400] if "WORK_MEM" in LOAD_SH else ""
    assert "LOAD_JOBS" in bloco, "work_mem não considera o número de jobs simultâneos"


def test_t6_host_acima_do_orcamento_degrada_e_nao_aborta(rodar_load):
    """Host ocupado: reduz LOAD_JOBS até sequencial, mas termina (7.1)."""
    r = rodar_load(com_rg=1, extra_env={"ORCAMENTO_VCPU": "1", "LOAD_JOBS": "3"})
    assert r.returncode == 0, "a carga abortou em vez de degradar"
    assert re.search(r"degrad|reduzindo.*LOAD_JOBS", r.stdout, re.I), (
        "a carga não avisou que degradou o paralelismo")


# ---------------------------------------------------------------------------
# T8 — duplicata vira quarentena, não parada (7.1)
# ---------------------------------------------------------------------------

def test_t8_duplicata_gera_indice_nao_unico_e_quarentena(psql_db):
    """Um mês com duplicata em `estabelecimento` não pode derrubar a carga.

    O desfecho contratado (7.1): índice **não-único**, chaves em
    `carga.duplicata`, carga termina em sucesso degradado — e a API continua
    respondendo com o mesmo plano de acesso.

    O cenário passou a incluir a **Fase 2** (16/09/2026). Antes, o teste ia
    direto do transform para a Fase 4, com a PK de `estabelecimento` ainda viva
    — situação que a v2 nunca produz, porque a Fase 2 dropa exatamente essa PK.
    Com ela viva, o `ON CONFLICT` do transform dedupe e a tabela final chega
    limpa; a duplicata só sobrevive até a Fase 4 quando o índice não existe.

    Isso importa porque a descoberta da duplicata agora é feita pelo próprio
    `CREATE UNIQUE INDEX` ao falhar — de graça, em vez dos 51 minutos de
    varredura preventiva que a carga de ensaio mediu. O que o teste afirma não
    mudou; mudou o setup, que passou a ser o caminho que a carga realmente faz.
    """
    preparar_banco(psql_db)
    r = psql_db.sql(
        "INSERT INTO staging.estabelecimentos (cnpj_basico, cnpj_ordem, cnpj_dv, uf) "
        "VALUES ('00000001', '0001', '91', 'SP')")
    assert r.returncode == 0, r.stderr

    fase2 = SQL / "fase2_drop_indices.sql"
    fase4 = SQL / "fase4_indices.sql"
    assert fase2.exists() and fase4.exists(), "as Fases 2 e 4 da v2 não existem"

    r = psql_db.arquivo(fase2)
    assert r.returncode == 0, r.stderr
    rodar_transform(psql_db)
    r = psql_db.arquivo(fase4)
    assert r.returncode == 0, r.stderr

    q = psql_db.sql("SELECT count(*) FROM carga.duplicata WHERE tabela='estabelecimento'")
    assert q.returncode == 0 and "0" not in q.stdout.split()[:1], "a duplicata não foi posta em quarentena"

    idx = psql_db.sql(
        "SELECT indisunique FROM pg_index WHERE indexrelid = "
        "'analytics.estabelecimento_pkey'::regclass")
    assert "f" in idx.stdout, "o índice deveria ter sido criado NÃO-único neste mês"


# ---------------------------------------------------------------------------
# T10 / T11 / T12 — blocos paralelos (Fase 3)
#
# Entraram na spec em 16/09/2026. O risco que eles trazem não é desempenho, é
# **determinismo**: com blocos concorrentes, "a primeira linha do arquivo vence"
# deixa de ser definido, e o dado do mês passa a depender do escalonamento.
# ---------------------------------------------------------------------------

def test_t10_blocos_produzem_o_mesmo_conteudo_da_carga_sequencial(psql_db):
    """Três execuções em blocos têm de dar o MESMO hash da sequencial.

    Três, não uma: um teste de determinismo que passa uma vez não provou nada —
    ele só não sorteou a ordem ruim ainda.
    """
    from fixture_carga import hashes_do_contrato

    preparar_banco(psql_db)
    rodar_transform(psql_db)
    sequencial = hashes_do_contrato(psql_db)

    v2 = SQL / "03_transform_v2.sql"
    assert v2.exists(), "o transform em blocos não existe"

    for tentativa in range(3):
        preparar_banco(psql_db)
        r = psql_db.arquivo(v2)
        assert r.returncode == 0, r.stderr
        atual = hashes_do_contrato(psql_db)
        divergentes = {t: (sequencial[t], atual.get(t)) for t in sequencial
                       if sequencial[t] != atual.get(t)}
        assert not divergentes, (
            f"tentativa {tentativa + 1}: blocos produziram conteúdo diferente "
            f"da carga sequencial em {divergentes}"
        )


def test_t11_empresa_e_carregada_por_um_bloco_unico():
    """`empresa` tem PK viva na Fase 3 — logo NÃO pode ser fatiada.

    É a tabela em que o `ON CONFLICT DO NOTHING` arbitra entre duplicatas, e com
    blocos concorrentes o vencedor viraria sorteio (o caso 08314885 da seção
    2.6). As outras quatro entram sem índice vivo e podem ser fatiadas.
    """
    v2 = SQL / "03_transform_v2.sql"
    assert v2.exists(), "o transform em blocos não existe"
    texto = v2.read_text(encoding="utf-8")

    bloco_empresa = texto[texto.index("empresa"):][:1500]
    assert "ctid" not in bloco_empresa, (
        "empresa está sendo fatiada por ctid — com a PK viva, isso torna o "
        "resultado do ON CONFLICT dependente de escalonamento"
    )


def test_t12_pico_teorico_cabe_no_orcamento(rodar_load):
    """`work_mem` é por OPERAÇÃO: com N blocos o pico multiplica, e não há swap.

    O teto da seção 7.3 é 3 GB para a carga inteira. Estourar num host sem swap
    não é lentidão, é OOM kill — e a vítima pode ser o Postgres de outro serviço.

    O que este teste mede, e o que ele NÃO mede. Ele confere o pico TEÓRICO que a
    Fase 0 deriva: LOAD_JOBS × ~3 operações × work_mem, mais o shared_buffers de
    1 GB da decisão 3. É a conta que a seção 7.3 faz, e é a que se pode conferir
    sem uma carga de 20 horas.

    O pico REAL do servidor não é visível daqui: o consumo que importa é o do
    Postgres, que no servidor roda noutro container. O load.sh grava o que
    consegue medir (`pico_rss_mb`, do próprio processo de carga) e o teórico
    (`parametros->pico_teorico_mb`); a conferência do real contra os 3 GB fica
    para a primeira carga completa no servidor. Enquanto isso, o número fica
    conservador de propósito — que é o que a spec pede em 7.3.
    """
    r = rodar_load(com_rg=1, extra_env={"ORCAMENTO_RAM_MB": "3072"})
    assert r.returncode == 0, f"{r.stdout[-1500:]}\n{r.stderr[-1500:]}"

    m = re.search(r"pico teórico no servidor=(\d+)MB", r.stdout)
    assert m, f"o load.sh não informou o pico teórico:\n{r.stdout[-2000:]}"
    pico = int(m.group(1))

    # +1024: o shared_buffers da decisão 3, que sai do mesmo teto de 3 GB.
    assert pico + 1024 <= 3072, (
        f"pico teórico de {pico} MB + 1 GB de shared_buffers passa do teto de "
        f"3 GB da seção 7.3 — num host sem swap isso é OOM, não lentidão"
    )


def test_t12_load_sh_registra_o_pico_em_carga_resumo():
    """Guarda estrutural: a carga tem de DEIXAR REGISTRADO o que consumiu.

    Barato, e pega a regressão mais provável — alguém mexer no fechamento e
    perder a única evidência que existe para calibrar o orçamento no mês
    seguinte.
    """
    assert "pico_rss_mb" in LOAD_SH, (
        "o load.sh não grava pico_rss_mb em carga.resumo — sem isso não há como "
        "saber, depois, se o orçamento da Fase 0 estava certo"
    )


# ---------------------------------------------------------------------------
# T13 / T14 — robustez: nenhuma linha derruba a carga (R2.1 e R2.2)
#
# As cinco entradas abaixo foram verificadas em 16/09/2026 contra o Postgres 18
# do projeto: HOJE, cada uma delas mata a carga inteira. O objetivo da v2 é que
# todas virem NULL + rejeito contado.
# ---------------------------------------------------------------------------

LIXO_QUE_HOJE_MATA_A_CARGA = [
    # (id da regra, coluna de staging, valor, erro que o Postgres dá hoje)
    ("S6", "data_situacao_cadastral", "20200231", "date/time field value out of range"),
    ("S11", "capital_social", "1.234,56", "invalid input syntax for type numeric"),
    ("S11", "porte", "99999", "out of range for type smallint"),
    ("S11", "cnae_principal", "99999999999", "out of range for type integer"),
    ("S12", "cnpj_ordem", "00015", "value too long for type character"),
]


@pytest.mark.parametrize("regra,coluna,valor,erro_hoje", LIXO_QUE_HOJE_MATA_A_CARGA,
                         ids=[f"{r}-{c}" for r, c, _, _ in LIXO_QUE_HOJE_MATA_A_CARGA])
def test_t13_lixo_vira_null_e_rejeito_sem_derrubar_a_carga(psql_db, regra, coluna, valor, erro_hoje):
    """Uma célula malformada em 73 milhões de linhas não pode matar 20 horas.

    O teste insere UMA linha com lixo numa coluna e exige que o transform
    **termine**. O valor ruim vira NULL e aparece em `carga.rejeito` com a regra
    que o pegou.
    """
    preparar_banco(psql_db)

    if coluna in ("capital_social", "porte"):
        alvo = ("staging.empresas", "cnpj_basico", "00009999")
    else:
        alvo = ("staging.estabelecimentos", "cnpj_basico", "00009999")

    tabela, chave, valor_chave = alvo
    extra = ", cnpj_ordem, cnpj_dv" if tabela.endswith("estabelecimentos") and coluna != "cnpj_ordem" else ""
    valores_extra = ", '0001', '00'" if extra else ""
    r = psql_db.sql(
        f"INSERT INTO {tabela} ({chave}, {coluna}{extra}) "
        f"VALUES ('{valor_chave}', '{valor}'{valores_extra})")
    assert r.returncode == 0, r.stderr

    saida = rodar_transform(psql_db)
    assert saida.returncode == 0, (
        f"a carga morreu por causa de uma célula ({regra}: {coluna}={valor!r}).\n"
        f"Erro de hoje: {erro_hoje}"
    )

    rej = psql_db.sql(
        f"SELECT count(*) FROM carga.rejeito WHERE regra = '{regra}'")
    assert rej.returncode == 0, "carga.rejeito não existe"
    assert "0" not in rej.stdout.split()[:1], f"o valor ruim não foi registrado como {regra}"


def test_t14_layout_diferente_falha_na_fase_1_e_nao_na_hora_16(rodar_load):
    """Mudança de layout da Receita tem de ser detectada ANTES do COPY grande.

    Não é sanitizável: se o arquivo tem outro número de colunas, não há regra que
    salve a linha. O que a v2 controla é **quando** se descobre. Falhar em 2
    minutos é recuperável; falhar na hora 16 queima a janela inteira e deixa a
    base pela metade.
    """
    r = rodar_load(com_rg=1, extra_env={"SIMULAR_LAYOUT_INVALIDO": "1"})
    assert r.returncode != 0, "layout inválido deveria reprovar"
    assert re.search(r"layout|colunas", r.stdout + r.stderr, re.I), (
        "a carga não disse que o layout mudou")
    assert not re.search(r"transform", r.stdout, re.I), (
        "a carga chegou até o transform antes de reprovar o layout")
