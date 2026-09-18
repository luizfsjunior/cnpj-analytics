"""T3 — a DAG chama, não reescreve (spec-dag-carga.md, R2).

O retry de download existe porque o share da Receita derruba 22–35% das
conexões: 7 tentativas, backoff 0/2/5/15/30/30/60s, resume por `.part` com
header `Range`, 2ª passada. Retry de task inteira é grosso demais para 37 zips.
O `load.sh`, por sua vez, é o dono das seis fases e do `trap`.

Este teste é o que impede a erosão: um dia alguém acha mais prático "só fazer um
psql aqui" e o contrato da carga passa a ter dois donos.
"""

import re

import pytest


@pytest.fixture(scope="module")
def fonte(raiz):
    return (raiz / "airflow" / "dags" / "cnpj_carga.py").read_text(encoding="utf-8")


def test_t3_download_chama_a_funcao_do_watcher(fonte):
    assert re.search(r"\bdownload_month\s*\(", fonte), (
        "a task de download tem de chamar watcher.download_month — é ali que "
        "vivem os 7 retries, o resume por .part e a 2ª passada"
    )


def test_t3_download_nao_e_reimplementado(fonte):
    """Sinais de que alguém começou a refazer o download dentro da DAG."""
    for proibido in ("requests.get", "PROPFIND", "Range", ".part"):
        assert proibido not in fonte, (
            f"{proibido!r} aparece na DAG: o download é do watcher (R2)"
        )


def test_t3_a_carga_e_o_load_sh(fonte):
    assert "load.sh" in fonte, "a task de carga tem de invocar o load.sh"


def test_t3_a_dag_nao_fala_sql_de_carga(fonte):
    """A DAG lê `carga.resumo` (R5/R6) e nada mais. Qualquer outro SQL é sinal
    de que uma regra da carga vazou para cá."""
    for proibido in ("COPY ", "\\copy", "unzip", "CREATE INDEX", "staging."):
        assert proibido not in fonte, (
            f"{proibido!r} aparece na DAG: isso é trabalho do load.sh (R2)"
        )


def test_t3_o_sql_da_dag_so_toca_carga_resumo(fonte):
    """As únicas consultas que a DAG pode fazer são as de R5 e R6."""
    for consulta in re.findall(r"SELECT.+?FROM\s+([\w.]+)", fonte, re.I | re.S):
        assert consulta.lower() == "carga.resumo", (
            f"a DAG consulta {consulta}; só `carga.resumo` é permitido (R2)"
        )


def test_t3_nenhuma_regra_de_sanitizacao_na_dag(fonte):
    """As regras S1–S12 são do transform. Um regex de CNPJ aqui seria a segunda
    cópia de uma regra que a spec-carga.md define como tendo uma só."""
    assert r"\d{8}" not in fonte, "regra de sanitização (S4) copiada para a DAG"
    assert "parse_date" not in fonte

