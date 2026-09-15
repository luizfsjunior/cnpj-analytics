"""Contrato do retry de download (spec: 7 tentativas, backoff
0/2/5/15/30/30/60s, segunda passada em download_month).

A causa real medida contra o share da RFB: ~35% das conexoes completam o TLS,
aceitam o GET e nunca entregam um byte -> ReadTimeout. Retry imediato resolve a
maioria; com espera, quase todas.
"""
import requests
import responses

import watcher

URL = "https://arquivos.receitafederal.gov.br/public.php/webdav/2026-09/Empresas1.zip"
TOKEN = "tok"
DATA = b"x" * 1000


def _timeout():
    return requests.exceptions.ReadTimeout("Read timed out. (read timeout=60)")


# --------------------------------------------------------------------------
# 1. trava com 0 bytes na 1a tentativa, sucesso na 2a
# --------------------------------------------------------------------------
@responses.activate
def test_trava_sem_bytes_recupera_na_segunda_tentativa(dest, sleeps):
    responses.add(responses.GET, URL, body=_timeout())
    responses.add(responses.GET, URL, body=DATA, status=200)

    alvo = dest / "Empresas1.zip"
    assert watcher._download_one(URL, TOKEN, alvo, len(DATA)) is True
    assert alvo.read_bytes() == DATA
    assert not (dest / "Empresas1.zip.part").exists()   # .part virou o arquivo final
    assert sleeps == [2]                                # so a espera da tentativa 2


# --------------------------------------------------------------------------
# 2. trava NO MEIO do stream -> a tentativa seguinte retoma de onde parou
#    (o `have` precisa ser relido do .part a cada tentativa, senao os bytes
#     ja gravados sao duplicados e o zip sai corrompido)
# --------------------------------------------------------------------------
@responses.activate
def test_trava_no_meio_retoma_do_ponto_certo(dest, sleeps):
    part = dest / "Empresas1.zip.part"

    def grava_parcial_e_trava(request):
        part.write_bytes(DATA[:300])     # o que a conexao alcancou gravar
        raise _timeout()

    responses.add_callback(responses.GET, URL, callback=grava_parcial_e_trava)
    responses.add(responses.GET, URL, body=DATA[300:], status=206)

    alvo = dest / "Empresas1.zip"
    assert watcher._download_one(URL, TOKEN, alvo, len(DATA)) is True
    assert alvo.read_bytes() == DATA                    # sem bytes duplicados
    assert responses.calls[1].request.headers["Range"] == "bytes=300-"


# --------------------------------------------------------------------------
# 3. todas as tentativas falham -> False, .part preservado, backoff completo
# --------------------------------------------------------------------------
@responses.activate
def test_todas_as_falhas_preservam_part_e_percorrem_o_backoff(dest, sleeps):
    for _ in range(7):
        responses.add(responses.GET, URL, body=_timeout())
    part = dest / "Empresas1.zip.part"
    part.write_bytes(DATA[:100])

    alvo = dest / "Empresas1.zip"
    assert watcher._download_one(URL, TOKEN, alvo, len(DATA)) is False
    assert part.read_bytes() == DATA[:100]              # resume preservado
    assert not alvo.exists()
    assert len(responses.calls) == 7
    assert sleeps == [2, 5, 15, 30, 30, 60]


# --------------------------------------------------------------------------
# 4. 404 nao e transitorio: falha na hora, sem gastar backoff
# --------------------------------------------------------------------------
@responses.activate
def test_404_falha_sem_retry(dest, sleeps):
    responses.add(responses.GET, URL, status=404)

    assert watcher._download_one(URL, TOKEN, dest / "Empresas1.zip", None) is False
    assert len(responses.calls) == 1
    assert sleeps == []


# --------------------------------------------------------------------------
# 5. 5xx e transitorio: retenta
# --------------------------------------------------------------------------
@responses.activate
def test_500_e_retentado(dest, sleeps):
    responses.add(responses.GET, URL, status=503)
    responses.add(responses.GET, URL, body=DATA, status=200)

    alvo = dest / "Empresas1.zip"
    assert watcher._download_one(URL, TOKEN, alvo, len(DATA)) is True
    assert alvo.read_bytes() == DATA
    assert sleeps == [2]


# --------------------------------------------------------------------------
# 6. servidor ignora o Range e devolve 200 -> recomeca do zero (regressao)
# --------------------------------------------------------------------------
@responses.activate
def test_servidor_ignora_range_recomeca_do_zero(dest, sleeps):
    part = dest / "Empresas1.zip.part"
    part.write_bytes(b"lixo de tentativa anterior")
    responses.add(responses.GET, URL, body=DATA, status=200)   # 200, nao 206

    alvo = dest / "Empresas1.zip"
    assert watcher._download_one(URL, TOKEN, alvo, len(DATA)) is True
    assert alvo.read_bytes() == DATA


# --------------------------------------------------------------------------
# 7. 416 com .part ja completo -> renomeia e da sucesso
# --------------------------------------------------------------------------
@responses.activate
def test_416_com_part_completo_finaliza(dest, sleeps):
    part = dest / "Empresas1.zip.part"
    part.write_bytes(DATA)
    responses.add(responses.GET, URL, status=416)

    alvo = dest / "Empresas1.zip"
    assert watcher._download_one(URL, TOKEN, alvo, len(DATA)) is True
    assert alvo.read_bytes() == DATA
    assert not part.exists()
    assert sleeps == []


# --------------------------------------------------------------------------
# 8. download_month: falhou a 1a passada, passou na segunda -> True
# --------------------------------------------------------------------------
def test_download_month_recupera_na_segunda_passada(dest, sleeps, monkeypatch):
    monkeypatch.setattr(watcher, "DATA_DIR", str(dest))
    monkeypatch.setattr(watcher, "REGIME_FILES", [])
    monkeypatch.setattr(watcher, "fetch_month_files",
                        lambda mes: [("Cnaes.zip", len(DATA)), ("Empresas1.zip", len(DATA))])

    tentativas: dict[str, int] = {}

    def falso_download(url, token, target, size):
        n = tentativas.get(target.name, 0) + 1
        tentativas[target.name] = n
        # Empresas1 esgota as tentativas da 1a passada e so vem na segunda.
        if target.name == "Empresas1.zip" and n == 1:
            return False
        target.write_bytes(DATA)
        return True

    monkeypatch.setattr(watcher, "_download_one", falso_download)

    assert watcher.download_month("2026-09") is True
    assert tentativas["Empresas1.zip"] == 2      # 2a passada repetiu o arquivo
    assert tentativas["Cnaes.zip"] == 1          # quem foi bem nao repete


# --------------------------------------------------------------------------
# 9. regime tributario continua best-effort: falha nele nao derruba o mes
# --------------------------------------------------------------------------
def test_regime_falho_nao_bloqueia_o_mes(dest, sleeps, monkeypatch):
    monkeypatch.setattr(watcher, "DATA_DIR", str(dest))
    monkeypatch.setattr(watcher, "REGIME_FILES", ["entidades-lucro-real.zip"])
    monkeypatch.setattr(watcher, "fetch_month_files",
                        lambda mes: [("Cnaes.zip", len(DATA))])

    def falso_download(url, token, target, size):
        if target.name.startswith("entidades-"):
            return False
        target.write_bytes(DATA)
        return True

    monkeypatch.setattr(watcher, "_download_one", falso_download)
    assert watcher.download_month("2026-09") is True


# --------------------------------------------------------------------------
# 10. constantes da spec
# --------------------------------------------------------------------------
def test_constantes_da_spec():
    assert watcher.RETRY_BACKOFF == (0, 2, 5, 15, 30, 30, 60)
    assert watcher.DOWNLOAD_TIMEOUT == (30, 60)
