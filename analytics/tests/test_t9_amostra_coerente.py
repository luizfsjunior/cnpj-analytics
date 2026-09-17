"""T9 — amostra coerente sobre dado real (7.2 nível 2).

A fixture do T2 pega o que alguém teve imaginação de inventar. O T9 pega o resto:
`SAMPLE` nos dois caminhos, mesmo zip, mesmo mês, dois bancos na mesma máquina,
mesma comparação por hash.

Ele **pula sozinho** quando não há zips da Receita na máquina — que é o caso da
maioria dos ambientes de desenvolvimento. Não é opcional por isso: é obrigatório
antes de qualquer coisa ir para o servidor (seção 7 da spec).

Para rodar:  CNPJ_DATA_DIR=/caminho/com/os/zips pytest analytics/tests/test_t9_amostra_coerente.py

Ele demora: são dois `load.sh` completos em modo amostra, cada um com COPY,
transform e reconstrução de índice. Conte alguns minutos, não segundos.
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))

from fixture_carga import RAIZ, hashes_do_contrato  # noqa: E402

DATA_DIR = os.environ.get("CNPJ_DATA_DIR") or str(Path(__file__).resolve().parents[2] / "data")
PG_SERVICE = "postgres-cnpj-rfb"
AMOSTRA = os.environ.get("T9_SAMPLE", "200000")


def _tem_zips():
    d = Path(DATA_DIR)
    return d.is_dir() and any(d.glob("Estabelecimentos*.zip"))


pytestmark = pytest.mark.skipif(
    not _tem_zips(),
    reason=f"sem zips da Receita em {DATA_DIR} — defina CNPJ_DATA_DIR para rodar o T9",
)


def _psql(banco, *args, entrada=None):
    return subprocess.run(
        ["docker", "compose", "exec", "-T", PG_SERVICE, "psql", "-U", "cnpj",
         "-d", banco, "-v", "ON_ERROR_STOP=1", *args],
        cwd=str(RAIZ), capture_output=True, text=True, encoding="utf-8",
        errors="replace", input=entrada,
    )


class _Runner:
    """O mínimo que `hashes_do_contrato` precisa, apontado para um banco."""

    def __init__(self, nome):
        self.nome_banco = nome

    def sql(self, texto):
        return _psql(self.nome_banco, "-c", texto)


def _dropar(banco):
    subprocess.run(
        ["docker", "compose", "exec", "-T", PG_SERVICE, "psql", "-U", "cnpj",
         "-d", "postgres", "-c", f'DROP DATABASE IF EXISTS "{banco}" WITH (FORCE)'],
        cwd=str(RAIZ), capture_output=True, text=True,
    )


def _rodar_carga(bash_exe, banco, transform):
    """Roda o load.sh em modo amostra, escolhendo o caminho do transform.

    `CARGA_TRANSFORM` é a chave do teste: o load.sh usa o caminho em blocos por
    padrão, e aceita `sequencial` para percorrer o 03_transform.sql — que é a
    referência de conteúdo. Os dois têm de chegar ao mesmo lugar.
    """
    env = dict(os.environ)
    env.update({
        "DB": banco,
        "SAMPLE": AMOSTRA,
        "DATA_DIR": DATA_DIR,
        "CARGA_TRANSFORM": transform,
        "TIMING": "0",
        # TUNE=0: o T9 compara CONTEÚDO. Mexer em ALTER SYSTEM no meio de uma
        # comparação só acrescenta variável sem mudar o que se mede.
        "TUNE": "0",
    })
    return subprocess.run(
        [bash_exe, "analytics/load.sh"], cwd=str(RAIZ), env=env,
        capture_output=True, text=True, encoding="utf-8", errors="replace",
        timeout=3600,
    )


def test_t9_amostra_produz_o_mesmo_conteudo_nos_dois_caminhos(bash_exe):
    """Carga em blocos × carga sequencial, sobre dado REAL, hash a hash.

    Duas ressalvas do modo amostra, que a 7.2 registra e que este teste NÃO
    resolve: ele não carrega `regime` (a staging fica vazia) e o recorte é
    `head -N` de um zip só, então casos raros — partição `??`, a duplicata de
    `empresa` — podem não aparecer. Por isso o T2 existe e não é substituível
    por este.
    """
    bancos = {"sequencial": "cnpj_t9_seq", "blocos": "cnpj_t9_blocos"}
    for banco in bancos.values():
        _dropar(banco)

    hashes = {}
    try:
        for caminho, banco in bancos.items():
            r = _rodar_carga(bash_exe, banco, caminho)
            assert r.returncode == 0, (
                f"a carga '{caminho}' falhou:\n{r.stdout[-3000:]}\n{r.stderr[-3000:]}")
            hashes[caminho] = hashes_do_contrato(_Runner(banco))

        divergentes = {
            t: {"sequencial": hashes["sequencial"][t], "blocos": hashes["blocos"].get(t)}
            for t in hashes["sequencial"]
            if hashes["sequencial"][t] != hashes["blocos"].get(t)
        }
        assert not divergentes, (
            "os dois caminhos produziram conteúdo diferente sobre dado real — é "
            "exatamente o que o T2 não conseguiria pegar sozinho.\n"
            f"{divergentes}"
        )
    finally:
        for banco in bancos.values():
            _dropar(banco)
