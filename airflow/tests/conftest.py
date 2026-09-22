"""Fixtures dos testes da DAG de carga (spec-dag-carga.md, seção 6).

Dois grupos de teste convivem aqui:

* os que só olham **arquivos do repo** (compose, deploy, o fonte da DAG) —
  rodam sem Airflow, sem rede e sem banco;
* os que importam o **módulo da DAG** (`cnpj_carga`) — exigem o pacote
  `apache-airflow` instalado e pulam com mensagem explícita se não estiver.
"""

import sys
from pathlib import Path

import pytest

RAIZ = Path(__file__).resolve().parent.parent.parent   # raiz do repo
DAGS_DIR = RAIZ / "airflow" / "dags"

# A raiz do repo SAI do sys.path, e só a pasta de DAGs entra.
#
# Gotcha real, não hipotético — ele derruba a suíte na primeira execução:
# existe um diretório `airflow/` neste repo, e `python -m pytest` coloca o
# diretório corrente (a raiz) em sys.path[0]. Aí `import airflow` acha essa
# pasta como *namespace package* e sombreia o Apache Airflow de verdade. O
# sintoma é um AttributeError sem relação nenhuma com o que está sendo testado.
#
# Rodar `pytest` (em vez de `python -m pytest`) evita o sys.path[0], mas não dá
# para contar com isso: IDE, CI e tox chamam de jeitos diferentes.
for _entrada in ("", ".", str(RAIZ)):
    while _entrada in sys.path:
        sys.path.remove(_entrada)

if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

# Se o `airflow` já entrou sombreado (outro conftest, plugin, import anterior),
# desfaz — senão a remoção acima chega tarde demais.
if getattr(sys.modules.get("airflow"), "__file__", "sentinela") is None:
    del sys.modules["airflow"]


@pytest.fixture(scope="session")
def raiz() -> Path:
    return RAIZ


@pytest.fixture(scope="session")
def modulo():
    """O módulo da DAG. As decisões puras (`proxima_competencia`,
    `avaliar_desfecho`, `env_carga`) moram nele, no topo, junto dos operators —
    um arquivo só, porque a suíte exige o Airflow de qualquer jeito (T1, T6, T9,
    T10) e a DAG ainda vai migrar para o repo do Airflow (D2)."""
    _exige_airflow()
    import cnpj_carga

    return cnpj_carga


def _exige_airflow():
    airflow = pytest.importorskip(
        "airflow", reason="apache-airflow não instalado: pip install -r airflow/requirements-dev.txt"
    )

    # Guarda contra o sombreamento descrito acima: se `import airflow` trouxe a
    # pasta do repo em vez do pacote, é melhor falhar dizendo isso do que
    # despejar um AttributeError obscuro.
    assert hasattr(airflow, "DAG"), (
        "o módulo `airflow` importado não é o Apache Airflow — provavelmente a "
        f"raiz do repo entrou no sys.path e a pasta {RAIZ / 'airflow'} o sombreou"
    )


@pytest.fixture(scope="session")
def dag(modulo):
    """O objeto DAG."""
    return modulo.dag


@pytest.fixture(scope="session")
def tarefas(dag):
    """{task_id: objeto da task}."""
    return {t.task_id: t for t in dag.tasks}


@pytest.fixture(scope="session")
def fonte_dag() -> str:
    """O texto do módulo da DAG.

    Há contrato que só se enxerga no fonte: um template Jinja (`{{ conn... }}`,
    `{{ var.value... }}`) é uma string até a task rodar, então nenhum atributo
    do objeto DAG o distingue de um endereço escrito à mão. Não exige Airflow
    instalado.
    """
    return (DAGS_DIR / "cnpj_carga.py").read_text(encoding="utf-8")
