"""
watcher.py — verifica mensalmente o share Nextcloud da Receita Federal e dispara
o load.sh quando detecta dados novos.

Uso:
    python watcher.py           # verifica agora + agenda verificações mensais
    python watcher.py --check   # verifica uma vez e sai (útil p/ cron/debug)

Variáveis de ambiente (ou .env na raiz do projeto):
    CNPJ_DB           banco de destino do load.sh (default: cnpj_full)
    CNPJ_DATA_DIR     DATA_DIR do load.sh         (default: ../minha-receita/data)
    CNPJ_LOAD_SH      caminho do load.sh           (default: analytics/load.sh)
    CNPJ_STATE_FILE   onde persistir o estado      (default: watcher/state.json)
    CHECK_INTERVAL_H  intervalo em horas           (default: 720 = 30 dias)
"""

import json
import logging
import os
import subprocess
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

import re

import requests
import schedule
import time

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).parent.parent
SHARE_URL = "https://arquivos.receitafederal.gov.br/public.php/webdav/"
SHARE_TOKEN = "YggdBLfdninEJX9"

DB = os.getenv("CNPJ_DB", "cnpj_full")
DATA_DIR = os.getenv("CNPJ_DATA_DIR", "../minha-receita/data")
LOAD_SH = os.getenv("CNPJ_LOAD_SH", str(BASE_DIR / "analytics" / "load.sh"))


def to_wsl_path(p: str | Path) -> str:
    """Converte caminho Windows (C:/...) para WSL (/mnt/c/...) se necessário."""
    s = str(p).replace("\\", "/")
    m = re.match(r"^([A-Za-z]):/(.+)$", s)
    if m:
        return f"/mnt/{m.group(1).lower()}/{m.group(2)}"
    return s
STATE_FILE = Path(os.getenv("CNPJ_STATE_FILE", str(Path(__file__).parent / "state.json")))
CHECK_INTERVAL_H = int(os.getenv("CHECK_INTERVAL_H", "720"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(stream=open(sys.stdout.fileno(), "w", encoding="utf-8", closefd=False)),
        logging.FileHandler(Path(__file__).parent / "watcher.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Estado persistido
# ---------------------------------------------------------------------------

def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    return {"last_loaded_month": None, "last_check": None}


def save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")

# ---------------------------------------------------------------------------
# Verificação do share via WebDAV PROPFIND
# ---------------------------------------------------------------------------

def fetch_available_months() -> list[str]:
    """Retorna a lista de meses disponíveis no share (ex: ['2025-12', '2026-01'])."""
    try:
        resp = requests.request(
            "PROPFIND",
            SHARE_URL,
            auth=(SHARE_TOKEN, ""),
            headers={"Depth": "1"},
            timeout=30,
            # evita proxy corporativo em localhost (equivalente ao --noproxy '*')
            proxies={"http": "", "https": ""},
            verify=True,
        )
        resp.raise_for_status()
    except requests.RequestException as e:
        log.error("Erro ao consultar share Nextcloud: %s", e)
        return []

    # O PROPFIND devolve XML com <D:href> para cada entrada.
    # Entradas de mês têm formato /public.php/webdav/AAAA-MM/ ou AAAA-MM-DD/.
    root = ET.fromstring(resp.text)
    ns = {"D": "DAV:"}
    months = []
    for href in root.findall(".//D:href", ns):
        part = href.text.rstrip("/").split("/")[-1]
        # aceita AAAA-MM ou AAAA-MM-DD
        if len(part) >= 7 and part[:4].isdigit() and part[4] == "-" and part[5:7].isdigit():
            month = part[:7]  # AAAA-MM
            if month not in months:
                months.append(month)
    return sorted(months)


def latest_month() -> str | None:
    months = fetch_available_months()
    if not months:
        return None
    return months[-1]

# ---------------------------------------------------------------------------
# Disparo do load.sh
# ---------------------------------------------------------------------------

def run_load(month: str) -> bool:
    """Executa o load.sh completo. Retorna True se bem-sucedido."""
    log.info("Iniciando load.sh para o mês %s (banco=%s)...", month, DB)
    env = {
        **os.environ,
        "DB": DB,
        "DATA_DIR": DATA_DIR,
        "TUNE": "1",
    }
    try:
        result = subprocess.run(
            ["bash", to_wsl_path(LOAD_SH)],
            env=env,
            cwd=to_wsl_path(BASE_DIR),
            capture_output=False,   # herda stdout/stderr -> aparece no log do processo
            timeout=6 * 3600,       # 6 h: carga completa pode demorar
        )
        if result.returncode == 0:
            log.info("load.sh concluído com sucesso para %s.", month)
            return True
        else:
            log.error("load.sh falhou (código %d) para %s.", result.returncode, month)
            return False
    except subprocess.TimeoutExpired:
        log.error("load.sh excedeu o timeout de 6h para %s.", month)
        return False
    except Exception as e:
        log.error("Erro ao executar load.sh: %s", e)
        return False

# ---------------------------------------------------------------------------
# Ciclo principal de verificação
# ---------------------------------------------------------------------------

def check_and_load() -> None:
    now = datetime.now(timezone.utc).isoformat()
    state = load_state()
    log.info("Verificando share Nextcloud... (último mês carregado: %s)", state["last_loaded_month"])

    month = latest_month()
    state["last_check"] = now

    if month is None:
        log.warning("Não foi possível obter o mês disponível no share.")
        save_state(state)
        return

    log.info("Mês disponível no share: %s", month)

    if month == state["last_loaded_month"]:
        log.info("Dados já atualizados para %s. Nada a fazer.", month)
        save_state(state)
        return

    log.info("Novo mês detectado: %s (anterior: %s). Iniciando carga...", month, state["last_loaded_month"])
    success = run_load(month)

    if success:
        state["last_loaded_month"] = month
        log.info("Estado atualizado: last_loaded_month = %s", month)
    else:
        log.error("Carga falhou. Estado NÃO atualizado — será retentada na próxima verificação.")

    save_state(state)

# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main() -> None:
    one_shot = "--check" in sys.argv

    if one_shot:
        log.info("Modo --check: verificação única.")
        check_and_load()
        return

    log.info(
        "Watcher iniciado. Verificação a cada %d h (~%d dias). Verificando agora...",
        CHECK_INTERVAL_H,
        CHECK_INTERVAL_H // 24,
    )
    check_and_load()

    schedule.every(CHECK_INTERVAL_H).hours.do(check_and_load)
    log.info("Próxima verificação em %d h.", CHECK_INTERVAL_H)

    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    main()
