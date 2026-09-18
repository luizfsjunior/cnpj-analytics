"""T4 a T9 — as decisões da DAG (spec-dag-carga.md, R3 a R8).

T4, T5, T7 e T8 exercitam as decisões puras do módulo (`proxima_competencia`,
`avaliar_desfecho`, `env_carga`) — sem rede e sem banco. T6 e T9 olham a forma
da DAG. Todos precisam do Airflow instalado, porque a DAG é um módulo só.
"""

import pytest

# --------------------------------------------------------------------------
# T4 — a competência é a DETECTADA, nunca o mês corrente (R6 / seção 3)
#
# Uma carga que começa às 22h do dia 30 atravessa a meia-noite. O default do
# load.sh é "mês corrente": rotularia a carga em `carga.resumo` com o mês
# errado, e a run seguinte acharia que o mês novo ainda não foi carregado.
# --------------------------------------------------------------------------

def test_t4_competencia_e_obrigatoria(modulo):
    with pytest.raises(TypeError):
        modulo.env_carga()


def test_t4_competencia_vai_para_o_ambiente(modulo):
    assert modulo.env_carga("2026-09")["COMPETENCIA"] == "2026-09"


def test_t4_competencia_nao_vem_do_relogio(modulo, monkeypatch):
    """Mesmo virando o mês entre a detecção e a carga, o rótulo não muda."""
    env = modulo.env_carga("2026-09")
    assert env["COMPETENCIA"] == "2026-09"
    assert "now" not in repr(env).lower()


def test_t4_a_dag_passa_a_competencia_da_deteccao(raiz):
    """A carga tem de puxar a competência do XCom de `detectar_mes` — não de um
    macro de data do Airflow (`{{ ds }}`, `{{ logical_date }}`), que é a data da
    run e não o mês publicado pela Receita."""
    fonte = (raiz / "airflow" / "dags" / "cnpj_carga.py").read_text(encoding="utf-8")
    assert "detectar_mes" in fonte
    for macro in ("{{ ds ", "{{ ds }}", "logical_date", "execution_date"):
        assert macro not in fonte, (
            f"{macro!r} usado como competência: a competência é a detectada (T4)"
        )


# --------------------------------------------------------------------------
# T5 — `degradado` é sucesso (R5)
#
# Mês em que a Receita publica chave natural repetida: o índice sai não-único,
# as chaves vão para carga.duplicata, e a carga TERMINOU. Tratar isso como
# falha é agendar um retry de 4 horas todo mês em que a fonte vier suja.
# --------------------------------------------------------------------------

def test_t5_sucesso_passa(modulo):
    assert modulo.avaliar_desfecho("sucesso", "2026-09") == "sucesso"


def test_t5_degradado_passa(modulo):
    assert modulo.avaliar_desfecho("degradado", "2026-09") == "degradado"


def test_t5_falha_reprova(modulo):
    with pytest.raises(modulo.CargaFalhou):
        modulo.avaliar_desfecho("falha", "2026-09")


def test_t5_desfecho_ausente_reprova(modulo):
    """Sem linha em `carga.resumo` a carga morreu antes de fechar o resumo —
    ou nem chegou a abrir. Não é sucesso silencioso."""
    with pytest.raises(modulo.CargaFalhou):
        modulo.avaliar_desfecho(None, "2026-09")


def test_t5_desfecho_desconhecido_reprova(modulo):
    """O CHECK de carga.resumo só admite três valores; um quarto significa que
    alguém mexeu no schema sem passar por aqui."""
    with pytest.raises(modulo.CargaFalhou):
        modulo.avaliar_desfecho("parcial", "2026-09")


def test_t5_a_dag_nao_decide_pelo_exit_code(raiz):
    fonte = (raiz / "airflow" / "dags" / "cnpj_carga.py").read_text(encoding="utf-8")
    assert "avaliar_desfecho" in fonte, (
        "o resultado da run sai de carga.resumo, não do código de saída do "
        "container (R5)"
    )


# --------------------------------------------------------------------------
# T7 — sem mês novo, a run pula limpa (seção 3)
# --------------------------------------------------------------------------

@pytest.mark.parametrize(
    "disponiveis, ultima, esperado",
    [
        (["2026-07", "2026-08", "2026-09"], "2026-08", "2026-09"),
        (["2026-07", "2026-08", "2026-09"], "2026-09", None),   # nada novo
        (["2026-07", "2026-08"], "2026-09", None),              # base à frente do share
        (["2026-09"], None, "2026-09"),                         # base vazia
        ([], "2026-08", None),                                  # PROPFIND vazio
        ([], None, None),
    ],
)
def test_t7_proxima_competencia(modulo, disponiveis, ultima, esperado):
    assert modulo.proxima_competencia(disponiveis, ultima) == esperado


def test_t7_pega_o_mais_recente_mesmo_fora_de_ordem(modulo):
    """`fetch_available_months` devolve ordenado, mas a decisão não pode
    depender disso."""
    assert modulo.proxima_competencia(["2026-09", "2026-07", "2026-08"], "2026-07") == "2026-09"


def test_t7_sem_mes_novo_a_run_nao_falha(raiz):
    """Pular não é falhar: um alerta por dia em que a Receita não publicou nada
    treina a equipe a ignorar o alerta que importa (R8)."""
    fonte = (raiz / "airflow" / "dags" / "cnpj_carga.py").read_text(encoding="utf-8")
    assert "ShortCircuitOperator" in fonte or "short_circuit" in fonte, (
        "sem mês novo a run tem de curto-circuitar, não levantar exceção"
    )


def test_t7_ultima_competencia_sai_de_carga_resumo(modulo):
    """R6: a fonte da verdade é `carga.resumo`, não um state.json em volume."""
    sql = " ".join(modulo.SQL_ULTIMA_COMPETENCIA.lower().split())
    assert "from carga.resumo" in sql
    assert "sucesso" in sql and "degradado" in sql, (
        "um mês `degradado` está carregado: ignorá-lo faria a DAG recarregá-lo"
    )
    assert "falha" not in sql.replace("desfecho", "")


# --------------------------------------------------------------------------
# T8 — o orçamento vai explícito (R3)
#
# 8 vCPU e 16 GB divididos com Airflow, Kong e Traefik, SEM swap. Passar do
# teto ali não é lentidão: é OOM kill, e a vítima pode ser o Postgres de outra
# stack.
# --------------------------------------------------------------------------

def test_t8_orcamento_no_ambiente_da_carga(modulo):
    env = modulo.env_carga("2026-09")
    assert env["ORCAMENTO_RAM_MB"] == "3072"
    assert env["ORCAMENTO_VCPU"] == "4"


def test_t8_nada_de_tune_ram_gb(modulo):
    """Nome antigo do orçamento, em GB. Dois valores dizendo a mesma coisa é
    como o servidor acabou com `TUNE_RAM_GB=16` de uma instalação antiga."""
    assert "TUNE_RAM_GB" not in modulo.env_carga("2026-09")


def test_t8_destino_e_dados_sao_explicitos(modulo):
    env = modulo.env_carga("2026-09")
    assert env["DB"] == "cnpj_full"
    assert env["DATA_DIR"] == "/data"


def test_t8_orcamento_nao_e_herdado_do_ambiente(modulo, monkeypatch):
    """O `.env` do servidor não é visível para a DAG. Herdar dele significaria
    a carga rodar com o orçamento da máquina de desenvolvimento (16 GB)."""
    monkeypatch.setenv("ORCAMENTO_RAM_MB", "16384")
    assert modulo.env_carga("2026-09")["ORCAMENTO_RAM_MB"] == "3072"


# --------------------------------------------------------------------------
# T6 e T9 — a forma da DAG (R4, R7, R8)
# --------------------------------------------------------------------------

TASKS_ESPERADAS = {
    "detectar_mes",
    "baixar_zips",
    "carregar",
    "conferir_desfecho",
    "recuperar_indices",
}


def test_as_tasks_sao_as_da_spec(tarefas):
    assert set(tarefas) == TASKS_ESPERADAS


def test_t6_recuperar_indices_roda_sempre(tarefas):
    """Entre as Fases 2 e 4 a base fica SEM ÍNDICE. O `trap EXIT` do load.sh é
    garantia de processo: se o Airflow matar o container (timeout, zombie,
    restart), ele pode não completar. Esta task é a rede da rede."""
    t = tarefas["recuperar_indices"]
    assert t.trigger_rule == "all_done", (
        "recuperar_indices tem de rodar com a carga falhando, pulada ou morta"
    )


def test_t6_recuperar_indices_vem_depois_da_carga(tarefas):
    assert "carregar" in tarefas["recuperar_indices"].upstream_task_ids


def test_t6_recuperacao_chama_o_script_do_repo(raiz):
    fonte = (raiz / "airflow" / "dags" / "cnpj_carga.py").read_text(encoding="utf-8")
    assert "recuperar_indices.sh" in fonte


@pytest.mark.parametrize(
    "task_id, retries",
    [
        ("detectar_mes", 3),
        ("baixar_zips", 1),
        ("carregar", 0),       # R8: retentar 4h de carga às 2h pode ser pior
        ("recuperar_indices", 2),
    ],
)
def test_t9_retries_por_task(tarefas, task_id, retries):
    assert tarefas[task_id].retries == retries


def test_t9_a_carga_tem_timeout_de_20h(tarefas):
    """O LOAD_TIMEOUT_H que o watcher usava. A carga completa medida é 3h38;
    20h é o teto que existe para o caso patológico, não a expectativa."""
    t = tarefas["carregar"]
    assert t.execution_timeout is not None
    assert t.execution_timeout.total_seconds() == 20 * 3600


def test_t9_a_dag_avisa_quando_falha(dag, raiz):
    fonte = (raiz / "airflow" / "dags" / "cnpj_carga.py").read_text(encoding="utf-8")
    assert "on_failure_callback" in fonte or dag.default_args.get("on_failure_callback"), (
        "R8: falha tem de alertar onde a equipe lê — não no journalctl"
    )


def test_r7_a_carga_roda_em_container_proprio(raiz):
    """Dentro do worker, um restart do Airflow mata a carga — e matá-la entre as
    Fases 2 e 4 é o cenário caro."""
    fonte = (raiz / "airflow" / "dags" / "cnpj_carga.py").read_text(encoding="utf-8")
    assert "DockerOperator" in fonte or "docker compose run" in fonte
    assert "services-net" in fonte, (
        "o container precisa da rede services-net para achar postgres-cnpj-rfb"
    )


def test_o_schedule_substitui_o_loop_do_watcher(dag):
    """CHECK_INTERVAL_H=24 + LOAD_AFTER_HOUR=22 eram um cron artesanal."""
    assert str(getattr(dag, "schedule_interval", dag.schedule)) == "0 22 * * *"
