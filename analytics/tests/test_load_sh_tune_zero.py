"""Bug 3 — com TUNE=0 a carga termina em exit 1 mesmo dando tudo certo.

    reset_tuning() {
        [ "$TUNE" = "1" ] || return      # return PURO
        ...

`return` sem argumento devolve o status do último comando — aqui, o próprio
teste `[ "$TUNE" = "1" ]`, que é falso, isto é, 1. Sob `set -euo pipefail` esse
1 derruba o script. `apply_tuning` escapa por acidente: o `return` dele está
dentro de `{ echo ...; return; }` e herda o 0 do `echo`.

Por que importa: a carga roda inteira, grava tudo, e só então sai com 1. O
watcher decide sucesso apenas pelo returncode (`watcher.py`, run_load), então
uma carga mensal perfeita seria registrada como falha e retentada.

`TUNE=0` é opção pública — está na tabela do README e no .env.example — e é o
que se usa num Postgres compartilhado, onde ALTER SYSTEM não é bem-vindo.
"""


def test_tune_zero_termina_com_sucesso(rodar_load):
    r = rodar_load(sample="0", com_rg=1, extra_env={"TUNE": "0"})

    assert r.returncode == 0, (
        "com TUNE=0 a carga completa fez todo o trabalho e mesmo assim saiu com "
        f"{r.returncode} — o watcher leria isso como falha.\n"
        f"fim da saída:\n{r.stdout[-800:]}\n{r.stderr[-800:]}"
    )


def test_tune_zero_chega_ao_fim_do_script(rodar_load):
    """Não basta o código de saída: o script tem de executar até a última linha.

    Ancora na mensagem final, que só sai depois de reset_tuning — é ela que
    prova que o fluxo não foi interrompido no meio.
    """
    r = rodar_load(sample="0", com_rg=1, extra_env={"TUNE": "0"})

    assert "concluído" in r.stdout, (
        "a execução parou antes da mensagem final (reset_tuning abortou o "
        f"script).\nfim da saída:\n{r.stdout[-800:]}"
    )


def test_tune_um_continua_funcionando(rodar_load):
    """O caminho com tuning ligado (default) segue íntegro."""
    r = rodar_load(sample="0", com_rg=1, extra_env={"TUNE": "1"})

    assert r.returncode == 0, f"{r.stdout[-800:]}\n{r.stderr[-800:]}"
    assert "revertendo synchronous_commit" in r.stdout, (
        "com TUNE=1 o reset_tuning precisa de fato reverter os parâmetros"
    )
