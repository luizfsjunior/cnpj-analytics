import sys
from pathlib import Path

import pytest

# watcher.py mora um nível acima de tests/ e não é um pacote instalável.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import watcher  # noqa: E402


@pytest.fixture
def sleeps(monkeypatch):
    """Substitui o time.sleep do watcher e devolve a lista de esperas pedidas.

    Sem isso cada teste de retry levaria os 52s reais de backoff. A lista também
    é a asserção sobre a curva de espera (0/2/5/15/30 antes da tentativa 1..5);
    esperas de 0s não chegam a virar chamada de sleep.
    """
    chamadas: list[float] = []
    monkeypatch.setattr(watcher.time, "sleep", lambda s: chamadas.append(s))
    return chamadas


@pytest.fixture
def dest(tmp_path):
    return tmp_path
