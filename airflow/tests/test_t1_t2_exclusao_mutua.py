"""T1 e T2 — nunca duas cargas ao mesmo tempo (spec-dag-carga.md, R1).

É o invariante mais caro de violar: duas cargas concorrentes disputam o mesmo
schema `staging`, o mesmo `carga.indice_salvo` (a ÚNICA cópia da DDL dos 212
índices) e o mesmo orçamento de RAM num host sem swap.

R1 exige três camadas, e as três estão aqui: `max_active_runs=1`, um pool de UM
slot, e a ausência do watcher. As duas primeiras não valem nada enquanto a
terceira não for verdade — daí o T2 olhar o compose, e não a DAG.
"""

import json

import pytest

yaml = pytest.importorskip("yaml", reason="pip install -r airflow/requirements-dev.txt")

POOL = "cnpj_carga"


# --- T1: a DAG não se sobrepõe a si mesma ----------------------------------

def test_t1_max_active_runs_e_um(dag):
    assert dag.max_active_runs == 1, (
        "max_active_runs != 1: duas runs poderiam carregar ao mesmo tempo"
    )


def test_t1_catchup_desligado(dag):
    """Sem isto, ligar a DAG com start_date antigo enfileira uma carga de 4h
    por dia perdido — e cada uma delas é uma carga completa do mês corrente."""
    assert dag.catchup is False


def test_t1_carga_usa_o_pool_dedicado(tarefas):
    carregar = tarefas["carregar"]
    assert carregar.pool == POOL, (
        f"a task de carga tem de usar o pool {POOL!r}; sem ele, outra DAG pesada "
        "da equipe pode rodar junto e estourar a RAM de um host sem swap"
    )


def test_t1_o_pool_e_declarado_com_um_slot(raiz):
    """O pool é configuração do Airflow, não do código da DAG — mas se não vier
    versionado junto, ninguém lembra de criá-lo e a DAG roda sem proteção
    nenhuma, silenciosamente."""
    arquivo = raiz / "airflow" / "pools.json"
    assert arquivo.exists(), (
        "falta airflow/pools.json (importado com `airflow pools import`)"
    )
    pools = json.loads(arquivo.read_text(encoding="utf-8"))
    assert POOL in pools, f"{POOL} não declarado em pools.json"
    assert pools[POOL]["slots"] == 1, "o pool da carga tem de ter exatamente 1 slot"


# --- T2: o watcher saiu de cena (D1) ---------------------------------------

@pytest.fixture(scope="module")
def compose(raiz):
    return yaml.safe_load((raiz / "docker-compose.yml").read_text(encoding="utf-8"))


def test_t2_servico_do_watcher_nao_existe_mais(compose):
    servicos = compose.get("services", {})
    assert "watcher-cnpj-rfb" not in servicos, (
        "o serviço do watcher continua no compose. Enquanto ele existir, "
        "max_active_runs e pool não impedem carga dupla: são dois disparadores "
        "independentes"
    )


def test_t2_nenhum_servico_roda_o_watcher_em_loop(compose):
    """Renomear o serviço não conta como remover. O que o T2 guarda é que nada
    no compose executa o loop do watcher.

    O loop não aparece como `command:` no compose — vem do `CMD` da imagem
    (`watcher/Dockerfile`). Então olhar só o `command:` deixaria este teste
    passar de graça: qualquer serviço construído a partir daquele Dockerfile
    **sem** comando explícito sobe o daemon."""
    for nome, svc in (compose.get("services") or {}).items():
        comando = svc.get("command")
        assert "watcher.py" not in json.dumps([comando, svc.get("entrypoint")]), (
            f"o serviço {nome} ainda executa o watcher.py"
        )

        build = svc.get("build") or {}
        dockerfile = build.get("dockerfile") if isinstance(build, dict) else None
        if dockerfile and "watcher" in dockerfile.lower():
            assert comando, (
                f"o serviço {nome} constrói a imagem do watcher sem comando "
                "próprio: sobe o CMD do Dockerfile, que é o loop do daemon"
            )


def test_t2_volume_de_estado_nao_e_mais_declarado(compose):
    """R6: o `state.json` morre junto com o watcher. Manter o volume vivo é
    manter a ambiguidade sobre qual é a fonte da verdade do último mês."""
    volumes = compose.get("volumes") or {}
    assert "watcher_state" not in volumes


def test_t2_o_deploy_nao_sobe_mais_o_watcher(raiz):
    workflow = (raiz / ".github" / "workflows" / "deploy.yml").read_text(encoding="utf-8")
    assert "watcher-cnpj-rfb" not in workflow, (
        "o deploy ainda cita o watcher no `docker compose up`"
    )
    assert "--remove-orphans" in workflow, (
        "o --remove-orphans é o que mata o container do watcher que já está "
        "rodando no servidor quando o serviço sai do compose"
    )
