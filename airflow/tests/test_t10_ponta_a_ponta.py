"""T10 — a DAG ponta a ponta, com dado real em amostra (spec-dag-carga.md, 6).

Os T1–T9 olham a forma da DAG: retries, pool, trigger_rule, as decisões puras.
Nenhum deles executa uma carga. E a lição mais cara deste projeto é que os
testes com stub não pegam o que só aparece com zip de verdade — três bugs
passaram por toda a suíte verde, entre eles um `psql` em background que consumia
o stdin do loop de blocos e fazia a carga terminar **com sucesso e quatro tabelas
vazias** (spec-carga.md, seção 6).

O T10 roda a DAG inteira num Airflow local, com `SAMPLE` em vez da carga
completa: detecta o mês, baixa (ou reaproveita o que já está em disco), carrega,
lê `carga.resumo` e confere o desfecho. Minutos, não horas.

Ele **pula sozinho** sem zips, sem Docker ou sem Airflow — mas não é opcional
por isso: é **obrigatório antes de a DAG ir para o servidor**, exatamente como o
T9 da carga.

Para rodar:

    # com o Airflow local de pé e airflow/dags deste repo montado nele
    CNPJ_DATA_DIR=/caminho/com/os/zips pytest airflow/tests/test_t10_ponta_a_ponta.py
"""

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

RAIZ = Path(__file__).resolve().parents[2]
DATA_DIR = os.environ.get("CNPJ_DATA_DIR") or str(RAIZ / "data")
AMOSTRA = os.environ.get("T10_SAMPLE", "20000")
BANCO = os.environ.get("T10_DB", "cnpj_t10")
DAG_ID = "cnpj_carga_mensal"


def _tem_zips() -> bool:
    d = Path(DATA_DIR)
    return d.is_dir() and any(d.glob("Estabelecimentos*.zip"))


def _tem_docker() -> bool:
    if not shutil.which("docker"):
        return False
    r = subprocess.run(["docker", "info"], capture_output=True)
    return r.returncode == 0


# Como chamar a CLI do Airflow. No servidor ela está no PATH; na máquina de
# desenvolvimento o Airflow local é um compose (`airflow-local`) e não há nada
# no PATH do host — daí o `docker exec`. O cwd dentro do container é o repo
# montado somente leitura: a DAG vem da pasta de dags, não do cwd.
CONTAINER_AIRFLOW = os.environ.get(
    "T10_AIRFLOW_CONTAINER", "airflow-local-airflow-scheduler-1"
)
REPO_NO_CONTAINER = os.environ.get("T10_REPO_NO_CONTAINER", "/opt/cnpj-analytics")


def _cli_airflow() -> list[str] | None:
    """O prefixo de comando que executa a CLI do Airflow, ou None se não há."""
    if shutil.which("airflow"):
        return ["airflow"]
    if not shutil.which("docker"):
        return None
    vivo = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Running}}", CONTAINER_AIRFLOW],
        capture_output=True, text=True,
    )
    if vivo.returncode == 0 and vivo.stdout.strip() == "true":
        return ["docker", "exec", "-w", REPO_NO_CONTAINER, CONTAINER_AIRFLOW, "airflow"]
    return None


def _tem_airflow() -> bool:
    return _cli_airflow() is not None


pytestmark = [
    pytest.mark.skipif(
        not _tem_zips(),
        reason=f"sem zips da Receita em {DATA_DIR} — defina CNPJ_DATA_DIR para rodar o T10",
    ),
    pytest.mark.skipif(not _tem_docker(), reason="Docker indisponível"),
    pytest.mark.skipif(
        not _tem_airflow(),
        reason=(
            "CLI do Airflow indisponível: nem no PATH, nem no container "
            f"{CONTAINER_AIRFLOW} (defina T10_AIRFLOW_CONTAINER)"
        ),
    ),
]


def _airflow(*args, **kwargs):
    return subprocess.run(
        [*_cli_airflow(), *args],
        cwd=str(RAIZ), capture_output=True, text=True,
        encoding="utf-8", errors="replace", **kwargs,
    )


def _data_dir_para_dag() -> str:
    """O `data_dir` como a DAG o espera: o caminho no HOST.

    Ele é a fonte do bind mount do DockerOperator, resolvido pelo daemon do
    Docker — não um caminho de dentro do container do Airflow. Quem monta a
    pasta é o host, e lá dentro ela é sempre `/data` (DATA_DIR_CONTAINER).
    """
    return DATA_DIR


def _psql(sql: str) -> str:
    r = subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres-cnpj-rfb", "psql",
         "-U", "cnpj", "-d", BANCO, "-tAc", sql],
        cwd=str(RAIZ), capture_output=True, text=True,
        encoding="utf-8", errors="replace",
    )
    assert r.returncode == 0, f"psql falhou: {r.stderr}"
    return r.stdout.strip()


@pytest.fixture(scope="module")
def run_da_dag():
    """Dispara a DAG uma vez em modo amostra e devolve o mês carregado."""
    # json.dumps, e não f-string: no Windows o DATA_DIR vem com barras
    # invertidas e o `--conf` montado à mão vira JSON inválido.
    cfg = json.dumps(
        {"sample": AMOSTRA, "db": BANCO, "data_dir": _data_dir_para_dag()}
    )
    r = _airflow("dags", "test", DAG_ID, "2026-09-18", "--conf", cfg)
    assert r.returncode == 0, f"a DAG falhou:\n{r.stdout}\n{r.stderr}"
    return r.stdout


def test_t10_a_dag_termina_sem_falha(run_da_dag):
    assert "failed" not in run_da_dag.lower()


def test_t10_as_seis_tasks_executaram(run_da_dag):
    for task in ("listar_meses", "detectar_mes", "baixar_zips", "carregar",
                 "conferir_desfecho", "recuperar_indices"):
        assert task in run_da_dag, f"a task {task} não apareceu na execução"


def test_t10_carga_resumo_registrou_a_competencia(run_da_dag):
    """R6: é daqui que a run seguinte descobre o que já foi carregado. Um
    resumo sem linha significa que a DAG não tem como não recarregar o mês."""
    desfecho = _psql(
        "SELECT desfecho FROM carga.resumo ORDER BY inicio DESC LIMIT 1"
    )
    assert desfecho in ("sucesso", "degradado"), f"desfecho inesperado: {desfecho!r}"


def test_t10_o_resumo_foi_fechado(run_da_dag):
    fim = _psql("SELECT fim IS NOT NULL FROM carga.resumo ORDER BY inicio DESC LIMIT 1")
    assert fim == "t", "carga.resumo ficou com fim NULL: a carga não fechou o resumo"


def test_t10_as_tabelas_nao_estao_vazias(run_da_dag):
    """O bug que passou por toda a suíte verde: sucesso com quatro tabelas
    vazias. Amostra pequena não é desculpa — SAMPLE gera amostra COERENTE, com
    joins ponta a ponta."""
    for tabela in ("empresa", "estabelecimento", "socio", "simples"):
        n = int(_psql(f"SELECT count(*) FROM analytics.{tabela}"))
        assert n > 0, f"analytics.{tabela} ficou vazia"


def test_t10_os_indices_voltaram(run_da_dag):
    """A Fase 4 recria a partir do que a Fase 2 salvou. Se a contagem final for
    menor que a salva, a base ficou sem índice e ninguém percebeu."""
    salvos = int(_psql("SELECT count(*) FROM carga.indice_salvo"))
    atuais = int(_psql(
        "SELECT count(*) FROM pg_indexes WHERE schemaname = 'analytics'"
    ))
    assert atuais >= salvos, f"índices: {atuais} atuais < {salvos} salvos pela Fase 2"


def test_t10_a_competencia_e_a_detectada(run_da_dag):
    """T4 sobre dado real: o rótulo em carga.resumo é o mês publicado pela
    Receita, não o mês em que a carga rodou."""
    competencia = _psql(
        "SELECT competencia FROM carga.resumo ORDER BY inicio DESC LIMIT 1"
    )
    assert "detectar_mes" in run_da_dag
    assert competencia in run_da_dag, (
        f"a competência gravada ({competencia}) não é a que a DAG detectou"
    )
