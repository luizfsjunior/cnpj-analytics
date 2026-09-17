"""Fixture sintética e utilitários do contrato da carga (spec-carga.md).

Este módulo é a base dos testes T1, T2 e T3. Ele monta uma staging minúscula
(algumas dezenas de linhas) em que **cada linha existe para disparar uma regra
nomeada** da tabela S da spec — e nada mais. Rodar o `03_transform.sql` sobre ela
leva segundos, e é isso que torna o contrato utilizável como TDD.

Convenção importante: os valores esperados aqui descrevem o **comportamento de
hoje**, não o desejado. A spec diz que a v2 não pode mudar o resultado; logo, um
teste que falha aqui depois da v2 significa regressão, e um teste que precisou
ser editado para a v2 passar significa que a v2 mudou o contrato — que é
exatamente o que não pode acontecer em silêncio.
"""

import hashlib
import subprocess
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent.parent
SQL = RAIZ / "analytics"
PG_SERVICE = "postgres-cnpj-rfb"

# ---------------------------------------------------------------------------
# A fixture
#
# Cada tupla carrega um comentário com o ID da regra (S4, S6, ...) que ela
# exercita. Não acrescente linha sem dizer qual regra ela testa: uma fixture que
# cresce sem critério deixa de caber na cabeça e volta a esconder o que descarta.
# ---------------------------------------------------------------------------

EMPRESAS = [
    # cnpj_basico, razao, natureza, qualif, capital, porte, ente
    # S10 — duplicata de cnpj_basico. A PRIMEIRA linha do arquivo vence hoje
    # (ON CONFLICT DO NOTHING). É o caso 08314885 real da seção 2.6.
    ("08314885", "FLAVIO PAVAO DE SOUZA", "4120", "59", "1234,56", "05", ""),
    ("08314885", "", "0000", "00", "", "", ""),
    # S7 — capital com vírgula decimal, zero e vazio
    ("00000001", "CAPITAL COM VIRGULA", "2062", "49", "1234,56", "01", ""),
    ("00000002", "CAPITAL ZERO", "2062", "49", "0", "01", ""),
    ("00000003", "CAPITAL VAZIO", "2062", "49", "", "01", ""),
    # S3 — razão social vazia vira NULL (não string vazia)
    ("00000004", "", "2062", "49", "10,00", "01", ""),
    # S4 — rejeitados: 7 dígitos e dígito+letra
    ("1234567", "CNPJ CURTO", "2062", "49", "1,00", "01", ""),
    ("1234567A", "CNPJ COM LETRA", "2062", "49", "1,00", "01", ""),
]

ESTABELECIMENTOS = [
    # 30 colunas; ver staging.estabelecimentos em 02_staging.sql
    # S9/S10 — o mesmo CNAE repetido 6× dentro da lista (o caso da seção 2.6)
    dict(cnpj_basico="00000001", cnpj_ordem="0001", cnpj_dv="91", uf="SP",
         cnae_secundaria="4711302,4711302,4711302,4711302,4711302,4711302",
         data_situacao_cadastral="20200101", data_inicio_atividade="20100101"),
    # S8 — uf vazia cai na partição DEFAULT como '??'
    dict(cnpj_basico="00000002", cnpj_ordem="0001", cnpj_dv="00", uf="",
         data_situacao_cadastral="20200101", data_inicio_atividade="20100101"),
    # S8 — uf só com espaços: btrim antes do coalesce, também vira '??'
    dict(cnpj_basico="00000003", cnpj_ordem="0001", cnpj_dv="00", uf="   ",
         data_situacao_cadastral="20200101", data_inicio_atividade="20100101"),
    # S6 — sentinelas de data: '00000000' e '0' viram NULL
    dict(cnpj_basico="00000004", cnpj_ordem="0001", cnpj_dv="00", uf="MG",
         data_situacao_cadastral="00000000", data_inicio_atividade="0",
         data_situacao_especial=""),
    # S6 — data no limite do mês, válida (28/02 de ano bissexto: 29/02 existe)
    dict(cnpj_basico="00000005", cnpj_ordem="0001", cnpj_dv="00", uf="RJ",
         data_situacao_cadastral="20200229", data_inicio_atividade="20100101"),
    # NOTA: a data IMPOSSÍVEL (ex.: 20200231) não mora nesta fixture de
    # propósito — hoje ela **derruba a carga inteira**, e não há como fixar isso
    # numa fixture compartilhada sem inviabilizar todos os outros testes. O caso
    # tem teste próprio: test_s6_data_impossivel_derruba_a_carga_hoje.
    # S3 — DDD sem telefone e telefone sem DDD (a concatenação + btrim)
    dict(cnpj_basico="00000006", cnpj_ordem="0001", cnpj_dv="00", uf="BA",
         ddd_1="", telefone_1="999999999", ddd_2="11", telefone_2="",
         data_situacao_cadastral="20200101", data_inicio_atividade="20100101"),
    # S9 — CNAE não numérico na lista: entra na lista, mas é descartado
    dict(cnpj_basico="00000007", cnpj_ordem="0001", cnpj_dv="00", uf="PR",
         cnae_secundaria="4711302,ABC,6201501",
         data_situacao_cadastral="20200101", data_inicio_atividade="20100101"),
    # S4 — rejeitado pelo filtro de cnpj_basico
    dict(cnpj_basico="1234567", cnpj_ordem="0001", cnpj_dv="00", uf="SP",
         data_situacao_cadastral="20200101", data_inicio_atividade="20100101"),
]

SOCIOS = [
    # cnpj_basico, ident, nome, cnpj_cpf, qualif, data_entrada, pais,
    # cpf_repr, nome_repr, qualif_repr, faixa
    # S10 — duas linhas 100% idênticas (o padrão das 22 da seção 2.6).
    # Hoje AMBAS entram: socio só tem PK sintética.
    ("00000001", "3", "SOCIO ESTRANGEIRO", "", "37", "20200101", "105", "", "", "", "0"),
    ("00000001", "3", "SOCIO ESTRANGEIRO", "", "37", "20200101", "105", "", "", "", "0"),
    ("00000002", "2", "SOCIA PESSOA FISICA", "***123456**", "49", "20190101", "105", "", "", "", "5"),
    # S4 — rejeitado
    ("1234567", "2", "CNPJ CURTO", "", "49", "20190101", "105", "", "", "", "5"),
]

SIMPLES = [
    # cnpj_basico, opcao_simples, data_opcao, data_exclusao, opcao_mei, data_mei, data_excl_mei
    ("00000001", "S", "20200101", "", "N", "", ""),
    # S10 — duplicata de cnpj_basico: hoje o ON CONFLICT mantém a primeira
    ("00000001", "N", "20210101", "", "S", "20210101", ""),
    ("00000002", "N", "", "", "N", "", ""),
    # S4 — rejeitado
    ("1234567", "S", "20200101", "", "N", "", ""),
]

# S5 — código não numérico é descartado em cada dimensão
DIMENSOES = {
    "cnaes": [("4711302", "COMERCIO VAREJISTA"), ("6201501", "DESENVOLVIMENTO DE SOFTWARE"), ("ABC", "CODIGO INVALIDO")],
    "naturezas": [("4120", "SOCIEDADE EMPRESARIA LIMITADA"), ("2062", "SOCIEDADE EMPRESARIA LIMITADA"), ("N/A", "INVALIDO")],
    "qualificacoes": [("49", "SOCIO ADMINISTRADOR"), ("59", "PRODUTOR RURAL"), ("-", "INVALIDO")],
    "paises": [("105", "BRASIL"), ("xx", "INVALIDO")],
    "motivos": [("00", "SEM MOTIVO"), ("??", "INVALIDO")],
    "municipios": [("7107", "SAO PAULO"), ("", "VAZIO")],
}

COLUNAS_ESTAB = [
    "cnpj_basico", "cnpj_ordem", "cnpj_dv", "identificador_matriz_filial",
    "nome_fantasia", "situacao_cadastral", "data_situacao_cadastral",
    "motivo_situacao", "nome_cidade_exterior", "pais", "data_inicio_atividade",
    "cnae_principal", "cnae_secundaria", "tipo_logradouro", "logradouro",
    "numero", "complemento", "bairro", "cep", "uf", "municipio", "ddd_1",
    "telefone_1", "ddd_2", "telefone_2", "ddd_fax", "fax", "correio_eletronico",
    "situacao_especial", "data_situacao_especial",
]

# Tabelas do contrato e a ordem estável usada para comparar conteúdo.
# `socio.id` fica FORA de propósito: é IDENTITY e depende da ordem de inserção,
# que a spec permite mudar. Ver R1/7.2.
TABELAS_CONTRATO = {
    "empresa": "cnpj_basico",
    "estabelecimento": "cnpj, uf",
    "estabelecimento_cnae_secundario": "cnpj, cnae_cod",
    "simples": "cnpj_basico",
    "dim_cnae": "codigo",
    "dim_natureza_juridica": "codigo",
    "dim_qualificacao": "codigo",
    "dim_pais": "codigo",
    "dim_motivo_situacao": "codigo",
    "dim_municipio": "codigo",
}


def _lit(v):
    return "NULL" if v is None else "'" + str(v).replace("'", "''") + "'"


def sql_popular_staging():
    """SQL que preenche a staging com a fixture. Idempotente (limpa antes)."""
    linhas = ["TRUNCATE staging.empresas, staging.estabelecimentos, staging.socios, staging.simples,"
              " staging.cnaes, staging.naturezas, staging.qualificacoes, staging.paises,"
              " staging.motivos, staging.municipios;"]

    linhas += ["INSERT INTO staging.empresas VALUES " + ", ".join(
        "(" + ", ".join(_lit(c) for c in linha) + ")" for linha in EMPRESAS) + ";"]

    valores = []
    for reg in ESTABELECIMENTOS:
        valores.append("(" + ", ".join(_lit(reg.get(c, "")) for c in COLUNAS_ESTAB) + ")")
    linhas += ["INSERT INTO staging.estabelecimentos VALUES " + ", ".join(valores) + ";"]

    linhas += ["INSERT INTO staging.socios VALUES " + ", ".join(
        "(" + ", ".join(_lit(c) for c in linha) + ")" for linha in SOCIOS) + ";"]
    linhas += ["INSERT INTO staging.simples VALUES " + ", ".join(
        "(" + ", ".join(_lit(c) for c in linha) + ")" for linha in SIMPLES) + ";"]

    for tabela, registros in DIMENSOES.items():
        linhas += [f"INSERT INTO staging.{tabela} VALUES " + ", ".join(
            "(" + ", ".join(_lit(c) for c in r) + ")" for r in registros) + ";"]

    return "\n".join(linhas)


def preparar_banco(psql_db):
    """Schema + staging + fixture, prontos para o 03_transform.sql.

    `00_carga.sql` entra aqui porque o transform depende dele: é onde moram os
    casts totais (`carga.num_smallint` e companhia) e a auditoria do rejeito. O
    load.sh aplica os três na mesma ordem.
    """
    for arquivo in ("00_carga.sql", "01_schema.sql", "02_staging.sql"):
        r = psql_db.arquivo(SQL / arquivo)
        assert r.returncode == 0, f"{arquivo} falhou:\n{r.stderr[-2000:]}"
    r = psql_db.sql(sql_popular_staging())
    assert r.returncode == 0, f"fixture falhou:\n{r.stderr[-2000:]}"


def rodar_transform(psql_db):
    r = psql_db.arquivo(SQL / "03_transform.sql")
    assert r.returncode == 0, f"03_transform.sql falhou:\n{r.stderr[-2000:]}"
    return r


def hash_tabela(psql_db, tabela, ordem, schema="analytics", colunas="t"):
    """Hash do conteúdo lógico: dump ordenado pela chave natural.

    `colunas` permite excluir coluna volátil — é assim que `socio` é comparada
    sem o `id`, que é IDENTITY.
    """
    r = psql_db.sql(
        f"SELECT coalesce(md5(string_agg({colunas}::text, E'\\n' ORDER BY {colunas}::text)), 'VAZIA') "
        f"FROM {schema}.{tabela} t"
    )
    assert r.returncode == 0, r.stderr
    for linha in (l.strip() for l in r.stdout.splitlines()):
        if len(linha) == 32 or linha == "VAZIA":
            return linha
    raise AssertionError(f"hash não encontrado na saída:\n{r.stdout}")


def hashes_do_contrato(psql_db):
    """Hash de todas as tabelas do contrato, mais `socio` sem o `id`."""
    saida = {t: hash_tabela(psql_db, t, ordem) for t, ordem in TABELAS_CONTRATO.items()}
    saida["socio"] = hash_tabela(
        psql_db, "socio", "", colunas="(cnpj_basico, identificador_socio, nome_socio,"
        " cnpj_cpf_socio, qualificacao_socio_cod, data_entrada_sociedade, pais_cod,"
        " cpf_representante, nome_representante, qualificacao_repr_cod, faixa_etaria_cod)")
    return saida


def dump_schema(nome_banco, schema="analytics"):
    """Estrutura do schema, normalizada, como o T1 a compara.

    `pg_dump --schema-only` já sai em ordem estável; o que se remove aqui são os
    comentários de versão (`-- Dumped by ...`), que mudam quando a imagem do
    Postgres muda sem que a estrutura tenha mudado.

    E as linhas `\\restrict` / `\\unrestrict`: o pg_dump 18 abre e fecha o script
    com um token **aleatório por execução** (proteção do psql contra injeção em
    dump). Sem removê-lo, dois dumps do mesmo banco nunca batem — foi o que fez
    o T1 falhar na primeira vez que rodou.
    """
    r = subprocess.run(
        ["docker", "compose", "exec", "-T", PG_SERVICE,
         "pg_dump", "-U", "cnpj", "-d", nome_banco, "--schema-only", "--schema=" + schema,
         "--no-owner", "--no-privileges"],
        cwd=str(RAIZ), capture_output=True, text=True, encoding="utf-8", errors="replace",
    )
    assert r.returncode == 0, f"pg_dump falhou:\n{r.stderr[-2000:]}"
    linhas = [l for l in r.stdout.splitlines()
              if l.strip() and not l.startswith("--") and not l.startswith("SET ")
              and not l.startswith("SELECT pg_catalog.set_config")
              and not l.startswith("\\restrict") and not l.startswith("\\unrestrict")]
    return "\n".join(linhas)


def hash_schema(nome_banco, schema="analytics"):
    return hashlib.md5(dump_schema(nome_banco, schema).encode("utf-8")).hexdigest()
