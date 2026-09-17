"""T3 — as regras de sanitização S1 a S10 (R2 da spec-carga.md).

Um teste por regra, cada um com o caso que a dispara e o caso que não dispara.
Todos descrevem o comportamento de HOJE: a v2 não pode mudar o resultado, só
passar a contabilizar o que descarta (isso é o T4).

S1 e S2 acontecem no pipe do shell (`tr -d` e `ENCODING` do COPY), antes de o SQL
existir — para essas duas o teste é estrutural, e diz isso na cara.
"""

from pathlib import Path

import pytest

from fixture_carga import RAIZ as RAIZ_FIXT, preparar_banco, rodar_transform

RAIZ = Path(__file__).resolve().parent.parent.parent
LOAD_SH = (RAIZ / "analytics" / "load.sh").read_text(encoding="utf-8")


@pytest.fixture
def carregado(psql_db):
    preparar_banco(psql_db)
    rodar_transform(psql_db)
    return psql_db


def um(psql_db, sql):
    r = psql_db.sql(sql)
    assert r.returncode == 0, r.stderr
    linhas = [l.strip() for l in r.stdout.splitlines()]
    corpo = [l for l in linhas if l and not l.startswith("-") and "row" not in l]
    return corpo[1] if len(corpo) > 1 else (corpo[0] if corpo else "")


# ---------------------------------------------------------------------------
# S1 / S2 — shell, não SQL (teste estrutural, assumidamente)
# ---------------------------------------------------------------------------

def test_s1_todo_copy_passa_por_tr_d_nul():
    """Todo caminho que alimenta um \\copy tem de remover bytes NUL.

    Estrutural de propósito: o NUL é removido no pipe, antes de o Postgres ver o
    byte. Um COPY que escape deste filtro quebra a carga com 'unquoted carriage
    return' ou 'invalid byte sequence' no meio da madrugada.
    """
    copys = [l for l in LOAD_SH.splitlines() if "\\copy staging." in l]
    assert copys, "nenhum \\copy encontrado em load.sh — o teste precisa ser revisto"
    for linha in copys:
        contexto = LOAD_SH[max(0, LOAD_SH.index(linha) - 400):LOAD_SH.index(linha) + len(linha)]
        assert "tr -d" in contexto or "ibge" in linha or "tabmun" in linha, (
            f"\\copy sem 'tr -d' no pipe:\n{linha.strip()}")


def test_s2_copy_dos_zips_declara_latin9():
    """O layout da Receita é LATIN9; sem declarar, acentos viram lixo silencioso."""
    assert "ENCODING 'LATIN9'" in LOAD_SH, "COPY_OPTS deveria declarar ENCODING 'LATIN9'"


# ---------------------------------------------------------------------------
# S3 — vazio vira NULL, nunca string vazia
# ---------------------------------------------------------------------------

def test_s3_texto_vazio_vira_null(carregado):
    assert um(carregado, "SELECT razao_social IS NULL FROM analytics.empresa WHERE cnpj_basico='00000004'") == "t"


def test_s3_ddd_sem_telefone_e_telefone_sem_ddd(carregado):
    """A concatenação ddd||telefone com btrim: sobra o que existir, ou NULL."""
    assert um(carregado, "SELECT ddd_telefone_1 FROM analytics.estabelecimento WHERE cnpj_basico='00000006'") == "999999999"
    assert um(carregado, "SELECT ddd_telefone_2 FROM analytics.estabelecimento WHERE cnpj_basico='00000006'") == "11"
    assert um(carregado, "SELECT ddd_fax IS NULL FROM analytics.estabelecimento WHERE cnpj_basico='00000006'") == "t"


# ---------------------------------------------------------------------------
# S4 — cnpj_basico tem de ser 8 dígitos
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("tabela,coluna", [
    ("empresa", "cnpj_basico"),
    ("estabelecimento", "cnpj_basico"),
    ("socio", "cnpj_basico"),
    ("simples", "cnpj_basico"),
])
def test_s4_cnpj_invalido_nao_entra(carregado, tabela, coluna):
    assert um(carregado, f"SELECT count(*) FROM analytics.{tabela} WHERE {coluna} !~ '^[0-9]{{8}}$'") == "0"


def test_s4_cnpj_valido_entra(carregado):
    assert um(carregado, "SELECT count(*) FROM analytics.empresa WHERE cnpj_basico='00000001'") == "1"


# ---------------------------------------------------------------------------
# S5 — código de dimensão tem de ser numérico
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("tabela,invalidos", [
    ("dim_cnae", 2), ("dim_natureza_juridica", 2), ("dim_qualificacao", 2),
    ("dim_pais", 1), ("dim_motivo_situacao", 1), ("dim_municipio", 1),
])
def test_s5_dimensao_descarta_codigo_nao_numerico(carregado, tabela, invalidos):
    assert int(um(carregado, f"SELECT count(*) FROM analytics.{tabela}")) == invalidos


# ---------------------------------------------------------------------------
# S6 — datas
# ---------------------------------------------------------------------------

def test_s6_sentinelas_viram_null(carregado):
    assert um(carregado, "SELECT data_situacao_cadastral IS NULL FROM analytics.estabelecimento WHERE cnpj_basico='00000004'") == "t"
    assert um(carregado, "SELECT data_inicio_atividade IS NULL FROM analytics.estabelecimento WHERE cnpj_basico='00000004'") == "t"


def test_s6_data_valida_atravessa(carregado):
    assert um(carregado, "SELECT data_inicio_atividade FROM analytics.estabelecimento WHERE cnpj_basico='00000001'") == "2010-01-01"


def test_s6_data_no_limite_do_mes_atravessa(carregado):
    """29/02/2020 existe (ano bissexto) e tem de entrar como está."""
    assert um(carregado, "SELECT data_situacao_cadastral FROM analytics.estabelecimento WHERE cnpj_basico='00000005'") == "2020-02-29"


def test_s6_data_impossivel_vira_null_e_rejeito(psql_db):
    """UMA data impossível não pode mais matar a carga inteira.

    Este teste foi escrito ao contrário, em 16/09/2026: ele fixava o
    comportamento de então, em que `analytics.parse_date` tratava as sentinelas
    ('', '0', '00000000') e delegava o resto ao `to_date` — que no PostgreSQL 18
    **estoura** em data inexistente:

        ERROR: date/time field value out of range: "20200231"

    Não rola para 02/03, como se poderia supor. Um `20200231` ou um `20201332`
    vindos da Receita derrubavam uma carga de 20 horas na fase de transform, e
    pela 7.1 não há ninguém lá para reagir.

    A decisão 6 da spec (tomada em 16/09/2026) mandou virar este teste, e ele
    virou: agora a data impossível vira NULL mais um rejeito S6 contado, e a
    carga TERMINA. A troca é segura pelo contrato de equivalência — se um mês
    tivesse uma data assim, não haveria carga bem-sucedida com que comparar.
    """
    preparar_banco(psql_db)
    r = psql_db.sql(
        "INSERT INTO staging.estabelecimentos (cnpj_basico, cnpj_ordem, cnpj_dv, uf,"
        " data_situacao_cadastral, data_inicio_atividade)"
        " VALUES ('00000099', '0001', '00', 'SP', '20200231', '20100101')")
    assert r.returncode == 0, r.stderr

    saida = psql_db.arquivo(RAIZ_FIXT / "analytics" / "03_transform.sql")

    assert saida.returncode == 0, (
        "uma data impossível derrubou a carga inteira — a decisão 6 da spec "
        f"regrediu.\n{saida.stderr[-800:]}"
    )
    assert um(psql_db, "SELECT data_situacao_cadastral IS NULL FROM"
                       " analytics.estabelecimento WHERE cnpj_basico='00000099'") == "t"
    assert um(psql_db, "SELECT data_inicio_atividade FROM analytics.estabelecimento"
                       " WHERE cnpj_basico='00000099'") == "2010-01-01", (
        "só a data ruim vira NULL; a boa da mesma linha tem de atravessar")
    assert int(um(psql_db, "SELECT count(*) FROM carga.rejeito WHERE regra='S6'"
                           " AND coluna='data_situacao_cadastral'")) == 1


def test_s6_data_fora_do_formato_aaaammdd_vira_null(psql_db):
    """Desvio nomeado da v2: fora de `^\\d{8}$` e das sentinelas, é NULL.

    O `to_date` antigo era leniente e devolvia uma data para entradas como
    `'2020-01-01'`. A pré-validação em SQL puro exigida pela R2.1 precisa de um
    formato fixo para conferir mês e dia antes do cast, então esse caso passou a
    ser rejeito contado em vez de adivinhação.

    Está registrado como exceção nomeada na spec (S6). Nenhuma linha da fixture
    cai aqui, e o golden do T2 não muda — mas o caso existe e este teste o fixa.
    """
    preparar_banco(psql_db)
    r = psql_db.sql(
        "INSERT INTO staging.estabelecimentos (cnpj_basico, cnpj_ordem, cnpj_dv, uf,"
        " data_situacao_cadastral) VALUES ('00000098', '0001', '00', 'SP', '2020-01-01')")
    assert r.returncode == 0, r.stderr
    assert psql_db.arquivo(RAIZ_FIXT / "analytics" / "03_transform.sql").returncode == 0
    assert um(psql_db, "SELECT data_situacao_cadastral IS NULL FROM"
                       " analytics.estabelecimento WHERE cnpj_basico='00000098'") == "t"


# ---------------------------------------------------------------------------
# S7 — capital social
# ---------------------------------------------------------------------------

def test_s7_virgula_decimal_vira_numeric(carregado):
    assert um(carregado, "SELECT capital_social FROM analytics.empresa WHERE cnpj_basico='00000001'") == "1234.56"


def test_s7_zero_e_zero_e_vazio_e_null(carregado):
    assert um(carregado, "SELECT capital_social FROM analytics.empresa WHERE cnpj_basico='00000002'") == "0.00"
    assert um(carregado, "SELECT capital_social IS NULL FROM analytics.empresa WHERE cnpj_basico='00000003'") == "t"


# ---------------------------------------------------------------------------
# S8 — uf ausente cai na partição DEFAULT como '??'
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("cnpj", ["00000002", "00000003"])
def test_s8_uf_vazia_ou_em_branco_vira_interrogacao(carregado, cnpj):
    assert um(carregado, f"SELECT uf FROM analytics.estabelecimento WHERE cnpj_basico='{cnpj}'") == "??"


def test_s8_linhas_sem_uf_ficam_na_particao_default(carregado):
    assert int(um(carregado, "SELECT count(*) FROM analytics.estabelecimento_default")) == 2


def test_s8_uf_valida_vai_para_a_particao_certa(carregado):
    assert int(um(carregado, "SELECT count(*) FROM analytics.estabelecimento_sp")) == 1


# ---------------------------------------------------------------------------
# S9 — CNAE secundário: unnest + filtro numérico
# ---------------------------------------------------------------------------

def test_s9_cnae_repetido_entra_uma_vez(carregado):
    """O mesmo código 6× na lista: hoje o ON CONFLICT deixa uma linha só."""
    assert int(um(carregado, "SELECT count(*) FROM analytics.estabelecimento_cnae_secundario WHERE cnpj='00000001000191'")) == 1


def test_s9_cnae_nao_numerico_e_descartado(carregado):
    """'4711302,ABC,6201501' entra com dois códigos, não três."""
    assert int(um(carregado, "SELECT count(*) FROM analytics.estabelecimento_cnae_secundario WHERE cnpj='00000007000100'")) == 2


# ---------------------------------------------------------------------------
# S10 — duplicatas de chave natural
# ---------------------------------------------------------------------------

def test_s10_empresa_mantem_a_primeira_linha_do_arquivo(carregado):
    """O caso 08314885 da seção 2.6: a primeira linha vence, e hoje é a boa.

    Fixa o comportamento — e a fragilidade. Não há regra de desempate: se a ordem
    do arquivo virar, o lixo vence em silêncio. É a decisão 2 da seção 5 da spec.
    """
    assert int(um(carregado, "SELECT count(*) FROM analytics.empresa WHERE cnpj_basico='08314885'")) == 1
    assert um(carregado, "SELECT razao_social FROM analytics.empresa WHERE cnpj_basico='08314885'") == "FLAVIO PAVAO DE SOUZA"


def test_s10_simples_mantem_a_primeira(carregado):
    assert int(um(carregado, "SELECT count(*) FROM analytics.simples WHERE cnpj_basico='00000001'")) == 1
    assert um(carregado, "SELECT opcao_simples FROM analytics.simples WHERE cnpj_basico='00000001'") == "t"


def test_s10_socio_duplicado_entra_duas_vezes(carregado):
    """Hoje `socio` não tem chave natural: as duas linhas idênticas entram.

    É o comportamento atual e o teste o fixa. Mudar isso é a decisão 1 da seção 5
    da spec — e, se for tomada, vira exceção nomeada no contrato de equivalência.
    """
    assert int(um(carregado, "SELECT count(*) FROM analytics.socio WHERE cnpj_basico='00000001'")) == 2
