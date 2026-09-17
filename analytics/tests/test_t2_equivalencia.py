"""T2 — equivalência de conteúdo (7.2 nível 1 / spec-carga.md seção 6).

O contrato: para a mesma entrada, a carga nova produz o mesmo **conteúdo lógico**
que a atual em todas as tabelas de `analytics`. Conteúdo lógico é o dump ordenado
pela chave natural, sem colunas voláteis (`socio.id` fica fora — é IDENTITY).

Como a v2 ainda não existe, este arquivo faz o que dá para fazer hoje e é o que
importa: **congela o comportamento atual num golden**. Quando a v2 chegar, ela
passa a ser comparada contra este arquivo, não contra a memória de ninguém.

Se um teste daqui falhar depois de mexer no `03_transform.sql`, a pergunta certa
NÃO é "como faço o teste passar": é "essa mudança de conteúdo era intencional?".
Se era, ela vira exceção nomeada na spec antes de o golden ser regravado.
"""

import json
from pathlib import Path

import pytest

from fixture_carga import hashes_do_contrato, preparar_banco, rodar_transform

GOLDEN = Path(__file__).parent / "golden_carga_atual.json"


def _hashes(psql_db):
    preparar_banco(psql_db)
    rodar_transform(psql_db)
    return hashes_do_contrato(psql_db)


def test_transform_e_deterministico(psql_db):
    """Duas execuções sobre a mesma entrada produzem o mesmo conteúdo.

    Pré-condição de qualquer comparação: se a carga atual já não for
    determinística, o contrato não tem como existir. (É aqui que apareceria o
    problema que a 7.2 antecipou: com blocos paralelos, "a primeira linha do
    arquivo vence" deixa de ser determinístico.)
    """
    primeira = _hashes(psql_db)
    rodar_transform(psql_db)
    segunda = hashes_do_contrato(psql_db)

    divergentes = {t: (primeira[t], segunda[t]) for t in primeira if primeira[t] != segunda[t]}
    assert not divergentes, f"transform não é determinístico em: {divergentes}"


def test_conteudo_bate_com_o_golden(psql_db):
    """O conteúdo atual é o que o golden registra.

    Na primeira execução o golden é criado e o teste passa, deixando o arquivo
    para ser versionado junto. Daí em diante, ele compara.
    """
    atual = _hashes(psql_db)

    if not GOLDEN.exists():
        GOLDEN.write_text(json.dumps(atual, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        pytest.skip(f"golden criado em {GOLDEN.name} — versione o arquivo e rode de novo")

    esperado = json.loads(GOLDEN.read_text(encoding="utf-8"))
    divergentes = {t: {"golden": esperado.get(t), "atual": atual.get(t)}
                   for t in set(esperado) | set(atual) if esperado.get(t) != atual.get(t)}
    assert not divergentes, (
        "o conteúdo produzido mudou em relação ao golden.\n"
        "Se a mudança foi intencional, registre-a como exceção nomeada na spec "
        "ANTES de regravar o golden.\n"
        f"{json.dumps(divergentes, indent=2)}"
    )


def test_v2_produz_o_mesmo_conteudo_da_carga_atual(psql_db):
    """O teste que fecha o contrato: caminho atual × caminho v2, hash a hash."""
    atual = _hashes(psql_db)

    from fixture_carga import SQL
    v2 = SQL / "03_transform_v2.sql"
    assert v2.exists(), "03_transform_v2.sql não existe"
    preparar_banco(psql_db)
    r = psql_db.arquivo(v2)
    assert r.returncode == 0, r.stderr
    novo = hashes_do_contrato(psql_db)

    divergentes = {t: (atual[t], novo.get(t)) for t in atual if atual[t] != novo.get(t)}
    assert not divergentes, f"v2 produz conteúdo diferente em: {divergentes}"
