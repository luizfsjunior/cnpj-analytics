"""Bug 1 — o modo SAMPLE do load.sh depende de `rg` e falha em SILÊNCIO sem ele.

`copy_zips_match` roda `( unzip -p ... | rg -f "$patterns" || true ) | psql \\copy`.
O `|| true` existe para tolerar o exit 1 legítimo do rg ("nenhum match neste
zip"), mas engole igualmente o **exit 127** de binário inexistente: cada zip vira
`COPY 0`, a amostra sai sem empresas/sócios/simples e o script termina com
sucesso — o oposto da "amostra COERENTE" que o cabeçalho promete.

Observado nesta máquina: `rg` não é o ripgrep, é uma função de shell, e funções
não passam para subprocessos.

Contrato exercitado aqui:
  1. rg ausente (127)  -> a carga PRECISA falhar, e dizer por quê;
  2. rg sem match (1)  -> a carga NÃO pode falhar (comportamento atual, protegido);
  3. dependência faltando -> aviso ANTES de horas de trabalho, não depois.
"""

import shutil

import pytest


def test_falha_quando_rg_nao_existe(rodar_load):
    """rg retornando 127 (binário ausente) tem de derrubar a carga."""
    r = rodar_load(com_rg=127)

    assert r.returncode != 0, (
        "load.sh terminou com sucesso mesmo sem rg — a amostra sai sem "
        "empresas/sócios/simples e ninguém fica sabendo.\n"
        f"saída:\n{r.stdout[-2000:]}"
    )
    combinada = (r.stdout + r.stderr).lower()
    assert "rg" in combinada or "ripgrep" in combinada, (
        "a falha precisa nomear a dependência que faltou; "
        f"saída:\n{r.stdout[-2000:]}\n{r.stderr[-2000:]}"
    )


def test_nao_falha_quando_rg_nao_encontra_match(rodar_load):
    """rg retornando 1 é normal (zip sem nenhum dos básicos) e deve ser tolerado.

    Protege o motivo pelo qual o `|| true` foi escrito: a correção do caso 127
    não pode transformar 'zip sem match' em erro.
    """
    r = rodar_load(com_rg=1)

    assert r.returncode == 0, (
        "exit 1 do rg significa apenas 'nenhum match' e não pode derrubar a "
        f"carga.\nsaída:\n{r.stdout[-2000:]}\n{r.stderr[-2000:]}"
    )


def test_preflight_reclama_de_dependencia_ausente(rodar_load, stub_bin):
    """Sem rg no PATH, o script deve parar logo — antes de qualquer COPY.

    O cabeçalho do load.sh lista `unzip` e `rg` como pré-requisitos, mas nada
    verifica. Numa carga real a descoberta vem depois de horas de COPY.
    """
    if shutil.which("rg") is not None:
        pytest.skip("existe um rg de verdade no PATH; este teste precisa da ausência")

    r = rodar_load(com_rg=None)  # nenhum stub de rg é instalado

    assert r.returncode != 0, (
        f"load.sh deveria abortar sem rg.\nsaída:\n{r.stdout[-2000:]}"
    )
    combinada = (r.stdout + r.stderr).lower()
    assert "rg" in combinada or "ripgrep" in combinada
    assert "COPY" not in r.stdout.split("staging.empresas")[0][-200:], (
        "o aborto deve vir antes de começar a copiar dados"
    )


def test_carga_completa_nao_precisa_de_rg(rodar_load):
    """A carga completa (SAMPLE=0) usa copy_zips, que não chama rg.

    Garante que a correção do modo amostra não vá exigir ripgrep de quem só roda
    a carga completa — inclusive o watcher em produção.
    """
    if shutil.which("rg") is not None:
        pytest.skip("existe um rg de verdade no PATH; este teste precisa da ausência")

    r = rodar_load(sample="0", com_rg=None)

    assert r.returncode == 0, (
        "a carga completa não depende de rg e não pode ser bloqueada por ele.\n"
        f"saída:\n{r.stdout[-2000:]}\n{r.stderr[-2000:]}"
    )
