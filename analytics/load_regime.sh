#!/usr/bin/env bash
# ============================================================================
# load_regime.sh — carga INCREMENTAL do regime tributário (entidades-*.zip)
#
# Popula analytics.regime_tributario sem tocar nas demais tabelas. Útil porque
# os arquivos de regime são distribuídos num share Nextcloud SEPARADO da Receita
# (token MPPfFit7g7zdA8C) e podem chegar depois da carga principal.
#
# Uso:
#   DB=cnpj_full bash analytics/load_regime.sh
#
# Variáveis: DB (default cnpj), DATA_DIR (default ./data; cai p/ ../minha-receita/data)
#
# Fonte dos zips (baixar antes para o DATA_DIR):
#   BASE=https://arquivos.receitafederal.gov.br/public.php/webdav
#   for f in entidades-imunes-e-isentas entidades-lucro-arbitrado \
#            entidades-lucro-presumido entidades-lucro-real; do
#     curl -sk -u "MPPfFit7g7zdA8C:" -o "$f.zip" "$BASE/$f.zip"
#   done
# ============================================================================
set -euo pipefail

DB="${DB:-cnpj}"
DATA_DIR="${DATA_DIR:-./data}"

if [ ! -e "$DATA_DIR/entidades-lucro-real.zip" ] && [ -e "../minha-receita/data/entidades-lucro-real.zip" ]; then
    DATA_DIR="../minha-receita/data"
fi

PSQL=(docker compose exec -T postgres psql -U cnpj -d "$DB" -v ON_ERROR_STOP=1)

echo "== regime tributário -> banco '$DB' (data: $DATA_DIR) =="

# 1) staging text (só esta tabela; não mexe no resto do schema staging)
"${PSQL[@]}" <<'SQL'
CREATE SCHEMA IF NOT EXISTS staging;
DROP TABLE IF EXISTS staging.regime_tributario;
CREATE TABLE staging.regime_tributario (
    ano                          text,
    cnpj                         text,
    cnpj_da_scp                  text,
    forma_de_tributacao          text,
    quantidade_de_escrituracoes  text
);
SQL

# 2) COPY dos 4 zips. Diferente do load.sh principal:
#    - delimitador VÍRGULA (não ';')
#    - cada zip pode conter VÁRIOS CSVs (um por ano), cada um com cabeçalho ->
#      filtra todas as linhas de cabeçalho com grep -vi
#    - tr -d '\000\r' remove NUL e CR (alguns CSVs vêm com CRLF)
for f in entidades-imunes-e-isentas entidades-lucro-arbitrado entidades-lucro-presumido entidades-lucro-real; do
    z="$DATA_DIR/$f.zip"
    [ -e "$z" ] || { echo "!! faltando $z — pulando"; continue; }
    echo ">> COPY $f.zip -> staging.regime_tributario"
    unzip -p "$z" | tr -d '\000\r' | grep -vi '^ano,cnpj,cnpj_da_scp' \
        | "${PSQL[@]}" -c "\copy staging.regime_tributario FROM STDIN (FORMAT csv, DELIMITER ',', QUOTE '\"', ENCODING 'UTF8')"
done

# 3) transform tipado (recria a tabela final no formato correto p/ estes dados)
echo ">> transform staging -> analytics.regime_tributario"
"${PSQL[@]}" <<'SQL'
DROP TABLE IF EXISTS analytics.regime_tributario;
CREATE TABLE analytics.regime_tributario (
    id                   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    cnpj                 char(14) NOT NULL,        -- CNPJ completo (a ordem varia)
    cnpj_basico          char(8)  NOT NULL,        -- p/ join com empresa
    ano                  smallint NOT NULL,
    forma_de_tributacao  text     NOT NULL,
    qtd_escrituracoes    integer,
    cnpj_da_scp          char(14)                  -- '0' na origem -> NULL
) WITH (fillfactor = 100);

INSERT INTO analytics.regime_tributario
    (cnpj, cnpj_basico, ano, forma_de_tributacao, qtd_escrituracoes, cnpj_da_scp)
SELECT DISTINCT
    cnpj14::char(14),
    substr(cnpj14, 1, 8)::char(8),
    nullif(ano, '')::smallint,
    forma_de_tributacao,
    nullif(quantidade_de_escrituracoes, '')::integer,
    nullif(nullif(cnpj_da_scp, ''), '0')::char(14)
FROM (
    SELECT regexp_replace(cnpj, '\D', '', 'g') AS cnpj14, *
    FROM staging.regime_tributario
) s
WHERE length(cnpj14) = 14
  AND nullif(ano, '') IS NOT NULL
  AND nullif(forma_de_tributacao, '') IS NOT NULL;

CREATE INDEX ix_regime_cnpj_basico ON analytics.regime_tributario (cnpj_basico);
CREATE INDEX ix_regime_cnpj        ON analytics.regime_tributario (cnpj);
CREATE INDEX ix_regime_ano_forma   ON analytics.regime_tributario (ano, forma_de_tributacao);

DROP TABLE IF EXISTS staging.regime_tributario;
ANALYZE analytics.regime_tributario;
SQL

echo "== concluído =="
"${PSQL[@]}" -c "SELECT count(*) AS linhas,
       count(DISTINCT cnpj_basico) AS empresas,
       min(ano) AS ano_min, max(ano) AS ano_max
FROM analytics.regime_tributario;"
