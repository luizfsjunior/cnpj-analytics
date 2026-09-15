"""Contrato do retry nos PROPFIND (mesma curva do download: RETRY_BACKOFF).

Mesma causa raiz do download: a conexao completa o TLS, o servidor aceita o
PROPFIND e nunca responde. Sem retry, uma trava de 30s numa listagem de 18 KB
derruba o ciclo inteiro e custa 24h ate a proxima verificacao.
"""
import requests
import responses

import watcher

RAIZ = watcher.SHARE_URL
MES = f"{watcher.SHARE_URL}2026-09/"

XML_MESES = """<?xml version="1.0"?>
<d:multistatus xmlns:d="DAV:">
  <d:response><d:href>/public.php/webdav/</d:href></d:response>
  <d:response><d:href>/public.php/webdav/2026-08/</d:href></d:response>
  <d:response><d:href>/public.php/webdav/2026-09/</d:href></d:response>
</d:multistatus>"""

XML_ARQUIVOS = """<?xml version="1.0"?>
<d:multistatus xmlns:d="DAV:">
  <d:response>
    <d:href>/public.php/webdav/2026-09/Cnaes.zip</d:href>
    <d:propstat><d:prop><d:getcontentlength>22078</d:getcontentlength></d:prop></d:propstat>
  </d:response>
  <d:response>
    <d:href>/public.php/webdav/2026-09/Empresas0.zip</d:href>
    <d:propstat><d:prop><d:getcontentlength>563070974</d:getcontentlength></d:prop></d:propstat>
  </d:response>
</d:multistatus>"""


def _timeout():
    return requests.exceptions.ReadTimeout("Read timed out. (read timeout=30)")


# --------------------------------------------------------------------------
# 1. trava na listagem de meses -> retenta e devolve os meses
# --------------------------------------------------------------------------
@responses.activate
def test_propfind_meses_recupera_no_retry(sleeps):
    responses.add("PROPFIND", RAIZ, body=_timeout())
    responses.add("PROPFIND", RAIZ, body=XML_MESES, status=207)

    assert watcher.fetch_available_months() == ["2026-08", "2026-09"]
    assert sleeps == [2]


# --------------------------------------------------------------------------
# 2. todas as tentativas travam -> lista vazia (contrato atual) e backoff cheio
# --------------------------------------------------------------------------
@responses.activate
def test_propfind_meses_esgota_tentativas(sleeps):
    for _ in range(len(watcher.RETRY_BACKOFF)):
        responses.add("PROPFIND", RAIZ, body=_timeout())

    assert watcher.fetch_available_months() == []
    assert len(responses.calls) == len(watcher.RETRY_BACKOFF)
    assert sleeps == [2, 5, 15, 30, 30, 60]


# --------------------------------------------------------------------------
# 3. trava na listagem do mes -> retenta e devolve (nome, tamanho)
# --------------------------------------------------------------------------
@responses.activate
def test_propfind_arquivos_do_mes_recupera_no_retry(sleeps):
    responses.add("PROPFIND", MES, body=_timeout())
    responses.add("PROPFIND", MES, body=XML_ARQUIVOS, status=207)

    assert watcher.fetch_month_files("2026-09") == [
        ("Cnaes.zip", 22078),
        ("Empresas0.zip", 563070974),
    ]
    assert sleeps == [2]


# --------------------------------------------------------------------------
# 4. 404 nao e transitorio: falha na hora, sem gastar backoff
# --------------------------------------------------------------------------
@responses.activate
def test_propfind_404_sem_retry(sleeps):
    responses.add("PROPFIND", MES, status=404)

    assert watcher.fetch_month_files("2026-09") == []
    assert len(responses.calls) == 1
    assert sleeps == []


# --------------------------------------------------------------------------
# 5. 5xx e transitorio: retenta
# --------------------------------------------------------------------------
@responses.activate
def test_propfind_500_e_retentado(sleeps):
    responses.add("PROPFIND", RAIZ, status=502)
    responses.add("PROPFIND", RAIZ, body=XML_MESES, status=207)

    assert watcher.fetch_available_months() == ["2026-08", "2026-09"]
    assert sleeps == [2]


# --------------------------------------------------------------------------
# 6. latest_month sobre o caminho com retry
# --------------------------------------------------------------------------
@responses.activate
def test_latest_month_recupera_no_retry(sleeps):
    responses.add("PROPFIND", RAIZ, body=_timeout())
    responses.add("PROPFIND", RAIZ, body=XML_MESES, status=207)

    assert watcher.latest_month() == "2026-09"
