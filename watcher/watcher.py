"""
watcher.py — verifica diariamente o share Nextcloud da Receita Federal e dispara
o load.sh quando detecta dados novos, mas só após as LOAD_AFTER_HOUR (default 22h).

Fluxo:
  1. Verifica o share (PROPFIND leve) a cada CHECK_INTERVAL_H horas.
  2. Se detectar mês novo E horário >= LOAD_AFTER_HOUR -> roda o load imediatamente.
  3. Se detectar mês novo E horário < LOAD_AFTER_HOUR -> agenda o load para as
     LOAD_AFTER_HOUR do mesmo dia e aguarda.

Uso:
    python watcher.py           # loop contínuo
    python watcher.py --check   # verifica uma vez e sai (debug/cron externo)

Variáveis de ambiente:
    CNPJ_DB           banco de destino do load.sh (default: cnpj_full)
    CNPJ_DATA_DIR     DATA_DIR do load.sh         (default: ../minha-receita/data)
    CNPJ_LOAD_SH      caminho do load.sh           (default: analytics/load.sh)
    CNPJ_STATE_FILE   onde persistir o estado      (default: watcher/state.json)
    CHECK_INTERVAL_H  intervalo de verificação      (default: 24h)
    LOAD_AFTER_HOUR   hora mínima para o load       (default: 22, formato 0-23)
"""

import json
import logging
import os
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from pathlib import Path

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
STATE_FILE = Path(os.getenv("CNPJ_STATE_FILE", str(Path(__file__).parent / "state.json")))
CHECK_INTERVAL_H = int(os.getenv("CHECK_INTERVAL_H", "24"))
LOAD_AFTER_HOUR = int(os.getenv("LOAD_AFTER_HOUR", "22"))   # 0-23

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(
            stream=open(sys.stdout.fileno(), "w", encoding="utf-8", closefd=False)
        ),
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
# WebDAV PROPFIND
# ---------------------------------------------------------------------------

def fetch_available_months() -> list[str]:
    """Retorna lista de meses disponíveis no share (ex: ['2025-12', '2026-01'])."""
    try:
        resp = requests.request(
            "PROPFIND",
            SHARE_URL,
            auth=(SHARE_TOKEN, ""),
            headers={"Depth": "1"},
            timeout=30,
            proxies={"http": "", "https": ""},  # ignora proxy corporativo
            verify=True,
        )
        resp.raise_for_status()
    except requests.RequestException as e:
        log.error("Erro ao consultar share Nextcloud: %s", e)
        return []

    root = ET.fromstring(resp.text)
    ns = {"D": "DAV:"}
    months = []
    for href in root.findall(".//D:href", ns):
        part = href.text.rstrip("/").split("/")[-1]
        if len(part) >= 7 and part[:4].isdigit() and part[4] == "-" and part[5:7].isdigit():
            month = part[:7]  # AAAA-MM
            if month not in months:
                months.append(month)
    return sorted(months)


def latest_month() -> str | None:
    months = fetch_available_months()
    return months[-1] if months else None

# ---------------------------------------------------------------------------
# Janela de horário
# ---------------------------------------------------------------------------

def seconds_until_load_window() -> float:
    """Segundos até LOAD_AFTER_HOUR hoje (0 se já passou da hora)."""
    now = datetime.now()
    target = now.replace(hour=LOAD_AFTER_HOUR, minute=0, second=0, microsecond=0)
    if now >= target:
        return 0.0
    return (target - now).total_seconds()


def wait_for_load_window() -> None:
    """Bloqueia até LOAD_AFTER_HOUR se ainda não chegou. Loga a espera."""
    secs = seconds_until_load_window()
    if secs <= 0:
        return
    wake = datetime.now() + timedelta(seconds=secs)
    log.info(
        "Fora da janela de carga (antes das %dh). Aguardando até %s...",
        LOAD_AFTER_HOUR,
        wake.strftime("%H:%M"),
    )
    time.sleep(secs)
    log.info("Janela de carga atingida (>= %dh). Iniciando load.", LOAD_AFTER_HOUR)

# ---------------------------------------------------------------------------
# Disparo do load.sh
# ---------------------------------------------------------------------------

def to_wsl_path(p: str | Path) -> str:
    """Converte caminho Windows (C:/...) para WSL (/mnt/c/...).

    No-op em paths POSIX/Linux: só transforma quando casa o padrão `X:/...`,
    então um `/home/user/...` passa intacto. Por isso o watcher roda tanto no
    dev Windows+WSL quanto num servidor Linux nativo sem alteração.
    """
    s = str(p).replace("\\", "/")
    m = re.match(r"^([A-Za-z]):/(.+)$", s)
    if m:
        return f"/mnt/{m.group(1).lower()}/{m.group(2)}"
    return s


def run_load(month: str) -> bool:
    """Executa o load.sh. Retorna True se bem-sucedido."""
    log.info("Iniciando load.sh para o mês %s (banco=%s)...", month, DB)
    env = {**os.environ, "DB": DB, "DATA_DIR": DATA_DIR, "TUNE": "1"}
    try:
        result = subprocess.run(
            ["bash", to_wsl_path(LOAD_SH)],
            env=env,
            cwd=to_wsl_path(BASE_DIR),
            timeout=6 * 3600,  # 6h: carga completa pode demorar
        )
        if result.returncode == 0:
            log.info("load.sh concluído com sucesso para %s.", month)
            return True
        log.error("load.sh falhou (código %d) para %s.", result.returncode, month)
        return False
    except subprocess.TimeoutExpired:
        log.error("load.sh excedeu o timeout de 6h para %s.", month)
        return False
    except Exception as e:
        log.error("Erro ao executar load.sh: %s", e)
        return False

# ---------------------------------------------------------------------------
# Ciclo de verificação
# ---------------------------------------------------------------------------

def check_and_load() -> None:
    state = load_state()
    log.info(
        "Verificando share Nextcloud... (último mês carregado: %s)",
        state["last_loaded_month"],
    )

    month = latest_month()
    state["last_check"] = datetime.now(timezone.utc).isoformat()

    if month is None:
        log.warning("Não foi possível obter o mês disponível no share.")
        save_state(state)
        return

    log.info("Mês disponível no share: %s", month)

    if month == state["last_loaded_month"]:
        log.info("Dados já atualizados para %s. Nada a fazer.", month)
        save_state(state)
        return

    log.info(
        "Novo mês detectado: %s (anterior: %s).",
        month,
        state["last_loaded_month"],
    )

    # Aguarda a janela de horário (>= LOAD_AFTER_HOUR) antes de carregar.
    wait_for_load_window()

    success = run_load(month)
    if success:
        state["last_loaded_month"] = month
        log.info("Estado atualizado: last_loaded_month = %s", month)
    else:
        log.error("Carga falhou. Será retentada na próxima verificação.")

    save_state(state)

# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main() -> None:
    if "--check" in sys.argv:
        log.info("Modo --check: verificação única.")
        check_and_load()
        return

    log.info(
        "Watcher iniciado. Verificação a cada %dh; load só após as %dh. "
        "O PROPFIND é leve — o load pesado só roda quando detecta mês novo.",
        CHECK_INTERVAL_H,
        LOAD_AFTER_HOUR,
    )
    check_and_load()
    schedule.every(CHECK_INTERVAL_H).hours.do(check_and_load)

    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    main()
