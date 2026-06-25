# Imagem do watcher: monitora o share da Receita e dispara o load.sh.
# Build a partir da RAIZ do repo (precisa de analytics/ e watcher/):
#   docker compose build watcher        # context: . (definido no compose)
FROM python:3.12-slim

# Dependências do load.sh: psql (cliente), unzip e ripgrep (rg). bash p/ rodá-lo.
# O watcher fala com o postgres via TCP (PGHOST), então NÃO precisa do Docker CLI
# nem do socket do host — só o cliente psql.
RUN apt-get update && apt-get install -y --no-install-recommends \
        bash postgresql-client unzip ripgrep ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY watcher/requirements.txt watcher/requirements.txt
RUN pip install --no-cache-dir -r watcher/requirements.txt

# Código. Os zips (./data) e o state.json entram por volume, não na imagem.
COPY analytics/ analytics/
COPY watcher/ watcher/

# PGHOST/PGPASSWORD, CNPJ_* e TUNE_RAM_GB vêm do compose (environment).
CMD ["python", "watcher/watcher.py"]
