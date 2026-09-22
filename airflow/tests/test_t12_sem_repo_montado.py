"""T12 — a DAG não depende do repo montado no Airflow (spec-dag-carga.md, 9.2).

Medido em 22/09/2026 no servidor: o worker do Airflow monta `logs/`, `config/`,
`plugins/`, `dags/`, o socket do Docker e uma pasta do comparador — e mais nada.
Não há repo do cnpj-analytics em lugar nenhum dele. O `detectar_mes` que
importava o watcher por `sys.path` morreria no import, e o Airflow local não
pegava isso porque lá o compose monta o repo justamente em `/opt/cnpj-analytics`,
o caminho do default. Um teste que só roda onde o bug não existe não é teste.

Daí a regra que este arquivo guarda: o código do watcher vem da **imagem da
carga**, a mesma das tasks de download e de carga, com a mesma tag. Nada de um
segundo lugar de onde ele possa vir — e divergir.

Tudo aqui é forma: não sobe container, não fala com o share.
"""

import pytest


# --------------------------------------------------------------------------
# Nada de repo montado
# --------------------------------------------------------------------------

@pytest.mark.parametrize("vestigio", ["CNPJ_REPO_DIR", "REPO_DIR", "sys.path"])
def test_t12_a_dag_nao_procura_o_repo(fonte_dag, vestigio):
    assert vestigio not in fonte_dag, (
        f"{vestigio!r} no fonte: a DAG voltou a depender de um repo montado no "
        "Airflow, que o servidor não tem"
    )


def test_t12_a_dag_nao_importa_o_watcher_em_processo(fonte_dag):
    """`from watcher.watcher import download_month` dentro do COMANDO de
    `baixar_zips` é esperado — é código que roda DENTRO do container. O que não
    pode existir é um import desses no nível do módulo da DAG, executado pelo
    worker. Por isso o teste olha só as linhas de import de topo (sem indentação
    e fora de uma string de comando), não qualquer ocorrência da substring."""
    linhas_de_import = [
        l for l in fonte_dag.splitlines()
        if l.startswith("import ") or l.startswith("from ")
    ]
    proibidas = [l for l in linhas_de_import if "watcher" in l]
    assert not proibidas, (
        f"import de watcher no nível do módulo: {proibidas} — o watcher só "
        "roda dentro da imagem da carga"
    )


def test_t12_requisitos_de_implantacao_sem_o_mount(fonte_dag):
    """O bloco de requisitos no fim do arquivo é o que alguém lê antes de
    implantar. Ele mentir custa uma janela de carga."""
    assert "somente leitura" not in fonte_dag


# --------------------------------------------------------------------------
# A listagem roda na imagem da carga
# --------------------------------------------------------------------------

def test_t12_listar_meses_existe(tarefas):
    assert "listar_meses" in tarefas


def test_t12_listar_meses_roda_em_container(tarefas, modulo):
    from airflow.providers.docker.operators.docker import DockerOperator

    task = tarefas["listar_meses"]
    assert isinstance(task, DockerOperator)
    assert task.image == modulo.IMAGEM_CARGA, (
        "a listagem tem de usar a MESMA imagem da carga: é o que garante que a "
        "detecção e o download rodam o mesmo watcher"
    )


def test_t12_listar_meses_publica_a_lista(tarefas):
    """Sem `do_xcom_push`, a lista não chega ao `detectar_mes` e a task vira
    um container que roda, imprime e não serve para nada."""
    assert tarefas["listar_meses"].do_xcom_push is True


def test_t12_listar_meses_chama_o_watcher(tarefas):
    """R2: listar os meses é `fetch_available_months`, não um PROPFIND novo."""
    comando = repr(tarefas["listar_meses"].command)
    assert "fetch_available_months" in comando
    for reimplementacao in ("PROPFIND", "requests.", "webdav"):
        assert reimplementacao not in comando


# --------------------------------------------------------------------------
# O short-circuit continua no worker
# --------------------------------------------------------------------------

def test_t12_detectar_mes_continua_curto_circuitando(tarefas):
    """Um `DockerOperator` não pula as tasks seguintes: quem faz isso é o
    `ShortCircuitOperator`. Por isso a decisão fica no worker, e só a ida ao
    share sai dele (T7)."""
    from airflow.providers.standard.operators.python import ShortCircuitOperator

    assert isinstance(tarefas["detectar_mes"], ShortCircuitOperator)


def test_t12_detectar_mes_le_a_lista_do_xcom(fonte_dag):
    assert 'xcom_pull(task_ids="listar_meses")' in fonte_dag


def test_t12_a_lista_do_xcom_e_desserializada(fonte_dag):
    """Bug real, pego rodando o T10 com dado de verdade em 22/09/2026: o XCom
    do DockerOperator é a última linha do stdout — uma STRING JSON, não a
    lista já pronta. Sem `json.loads`, `max()` sobre a string
    `'["2026-09"]'` devolve o CARACTERE de maior código (`]`), não o mês mais
    recente, e o download é pedido para o mês `]`."""
    assert "json.loads" in fonte_dag


def test_t12_a_ordem_das_tasks(tarefas):
    a_jusante = {t.task_id for t in tarefas["listar_meses"].downstream_list}
    assert a_jusante == {"detectar_mes"}
