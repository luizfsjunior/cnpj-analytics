"""Bug 2 — regime_transform.sql se diz idempotente, mas quebra em base antiga.

O arquivo abre com "num único arquivo idempotente" e cria a tabela com
`CREATE TABLE IF NOT EXISTS`. Quando o grão mudou (de `cnpj_basico` para
`(id, cnpj, ...)`), bases já carregadas ficaram com o schema velho: o
`IF NOT EXISTS` vê a tabela, não recria, e o INSERT seguinte morre em

    ERROR: column "cnpj" of relation "regime_tributario" does not exist

Isso derrubou o smoke test no banco `cnpj` — e, numa carga completa, derrubaria
na ÚLTIMA fase, depois de todo o trabalho pesado.

Schemas envolvidos:
  antigo : cnpj_basico, ano, forma_de_tributacao, qtd_escrituracoes, cnpj_da_scp
  atual  : id, cnpj, cnpj_basico, ano, forma_de_tributacao, qtd_escrituracoes, cnpj_da_scp

Estes testes precisam do container do compose de pé; pulam sozinhos se não.
"""

from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent.parent
REGIME_SQL = RAIZ / "analytics" / "regime_transform.sql"

COLUNAS_ATUAIS = [
    "id", "cnpj", "cnpj_basico", "ano",
    "forma_de_tributacao", "qtd_escrituracoes", "cnpj_da_scp",
]

# Staging que o regime_transform.sql consome (criada pelo load.sh, não pelo .sql).
STAGING = """
CREATE SCHEMA IF NOT EXISTS staging;
DROP TABLE IF EXISTS staging.regime_tributario;
CREATE TABLE staging.regime_tributario (
    ano                         text,
    cnpj                        text,
    cnpj_da_scp                 text,
    forma_de_tributacao         text,
    quantidade_de_escrituracoes text
);
INSERT INTO staging.regime_tributario VALUES
    ('2024', '00.000.000/0001-91', '0', 'LUCRO REAL', '1');
"""

TABELA_ANTIGA = """
CREATE SCHEMA IF NOT EXISTS analytics;
DROP TABLE IF EXISTS analytics.regime_tributario;
CREATE TABLE analytics.regime_tributario (
    cnpj_basico         char(8) NOT NULL,
    ano                 smallint NOT NULL,
    forma_de_tributacao text NOT NULL,
    qtd_escrituracoes   integer,
    cnpj_da_scp         char(14)
);
"""


def test_converge_schema_antigo_para_o_atual(psql_db):
    """Rodar sobre uma base no grão ANTIGO deve funcionar, não explodir."""
    pre = psql_db.sql(TABELA_ANTIGA)
    assert pre.returncode == 0, pre.stderr
    pre = psql_db.sql(STAGING)
    assert pre.returncode == 0, pre.stderr

    r = psql_db.arquivo(REGIME_SQL)

    assert r.returncode == 0, (
        "regime_transform.sql não converge uma base com o schema antigo — "
        "numa carga completa isso falha na última fase.\n"
        f"stderr:\n{r.stderr[-1500:]}"
    )
    assert psql_db.colunas("regime_tributario") == COLUNAS_ATUAIS


def test_carrega_a_linha_apos_converter(psql_db):
    """Converger o schema não basta: os dados da staging têm de entrar."""
    psql_db.sql(TABELA_ANTIGA)
    psql_db.sql(STAGING)

    psql_db.arquivo(REGIME_SQL)

    r = psql_db.sql(
        "SELECT cnpj, cnpj_basico, ano, forma_de_tributacao "
        "FROM analytics.regime_tributario"
    )
    assert r.returncode == 0, r.stderr
    assert "00000000000191" in r.stdout
    assert "LUCRO REAL" in r.stdout


def test_reexecucao_seguida_continua_funcionando(psql_db):
    """Duas execuções em sequência no schema atual (idempotência básica).

    Hoje isto passa; o teste existe para que a correção do caso anterior não
    quebre o caminho normal — inclusive o `REGIME_ONLY=1`, que roda exatamente
    este .sql sobre uma base já carregada.
    """
    psql_db.sql("CREATE SCHEMA IF NOT EXISTS analytics;")
    psql_db.sql(STAGING)
    primeira = psql_db.arquivo(REGIME_SQL)
    assert primeira.returncode == 0, primeira.stderr

    psql_db.sql(STAGING)          # o .sql dropa a staging ao terminar
    segunda = psql_db.arquivo(REGIME_SQL)

    assert segunda.returncode == 0, (
        f"segunda execução falhou:\n{segunda.stderr[-1500:]}"
    )
    assert psql_db.colunas("regime_tributario") == COLUNAS_ATUAIS
