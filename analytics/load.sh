#!/usr/bin/env bash
# ============================================================================
# load.sh — popula o schema analytics a partir dos zips em ./data
#
# Estratégia: streaming `unzip -p <zip> | psql \copy ... FROM STDIN` direto para
# dentro do container postgres. Não extrai CSV em disco nem precisa montar ./data
# no container (que só monta ./data/postgres).
#
# Uso (a partir da raiz do repo):
#   bash analytics/load.sh                  # carga COMPLETA no banco `cnpj`
#   SAMPLE=20000 bash analytics/load.sh     # amostra COERENTE de ~20k estabelecimentos
#   DB=cnpj_full bash analytics/load.sh     # carga completa em outro banco
#
# Variáveis:
#   DB            nome do banco de destino           (default: cnpj)
#   SAMPLE        se >0, gera amostra coerente        (default: 0 = base completa)
#   DATA_DIR      pasta com os zips da Receita        (default: ./data; ver nota)
#   TUNE          aplica tuning de carga (reload)     (default: 1; 0 desliga)
#   KEEP_STAGING  preserva o schema staging no fim    (default: 0 = dropa, ~27GB)
#   MAINT_WORK_MEM/MAX_PARALLEL_MAINT/MAX_WAL_SIZE/WORK_MEM  knobs do tuning
#                 (defaults: 2GB / 4 / 8GB / 256MB — ver analytics/tuning-carga.md)
#   NB: shared_buffers exige RESTART -> defina ANTES da carga (não é feito aqui).
#
# Amostra COERENTE: ancora em N estabelecimentos (head do 1º zip) e carrega apenas
# as empresas/sócios/simples cujo CNPJ básico aparece nesses estabelecimentos —
# garantindo que joins empresa↔filial↔sócio↔simples funcionem de ponta a ponta.
#
# Pré-requisitos: docker compose up -d postgres ; unzip e rg (ripgrep) no PATH.
# ============================================================================
set -euo pipefail

DB="${DB:-cnpj}"
SAMPLE="${SAMPLE:-0}"
DATA_DIR="${DATA_DIR:-./data}"
HERE="$(cd "$(dirname "$0")" && pwd)"

# --- Tuning de carga (reload-only; NÃO derruba a sessão nem exige restart) ------
# Defaults do Postgres são minúsculos (maintenance_work_mem=64MB, work_mem=4MB...).
# Estes valores aceleram MUITO os CREATE INDEX e o COPY. Sobrescrevíveis por env;
# desligue tudo com TUNE=0. Detalhes e racional em analytics/tuning-carga.md.
# OBS: `shared_buffers` exige RESTART -> fica de fora; defina-o ANTES (ver doc).
TUNE="${TUNE:-1}"
MAINT_WORK_MEM="${MAINT_WORK_MEM:-2GB}"        # pico = (workers+1) x este valor!
MAX_PARALLEL_MAINT="${MAX_PARALLEL_MAINT:-4}"
MAX_WAL_SIZE="${MAX_WAL_SIZE:-8GB}"
WORK_MEM="${WORK_MEM:-256MB}"
# Mantém o schema staging após a carga (debug). Default: dropar e liberar ~27GB.
KEEP_STAGING="${KEEP_STAGING:-0}"

# Os zips ficam no repo minha-receita; permita sobrescrever via DATA_DIR.
if [ ! -e "$DATA_DIR/Empresas0.zip" ] && [ -e "../minha-receita/data/Empresas0.zip" ]; then
    DATA_DIR="../minha-receita/data"
fi

# psql dentro do container; -T = sem TTY (essencial para pipe via STDIN)
PSQL=(docker compose exec -T postgres psql -U cnpj -d "$DB" -v ON_ERROR_STOP=1)
COPY_OPTS="(FORMAT csv, DELIMITER ';', QUOTE '\"', ENCODING 'LATIN9')"

run_sql_file() { echo ">> aplicando $1"; "${PSQL[@]}" < "$1"; }

# Aplica o tuning de carga via ALTER SYSTEM + pg_reload_conf (SIGHUP). Cada ALTER
# vai num -c separado porque ALTER SYSTEM não roda dentro de transação.
# Requer superuser (no compose o user 'cnpj' é superuser; num servidor, conferir).
apply_tuning() {
    [ "$TUNE" = "1" ] || { echo ">> tuning de carga DESATIVADO (TUNE=0)"; return; }
    echo ">> aplicando tuning de carga (reload, sem restart)"
    "${PSQL[@]}" \
        -c "ALTER SYSTEM SET maintenance_work_mem = '$MAINT_WORK_MEM';" \
        -c "ALTER SYSTEM SET max_parallel_maintenance_workers = $MAX_PARALLEL_MAINT;" \
        -c "ALTER SYSTEM SET max_wal_size = '$MAX_WAL_SIZE';" \
        -c "ALTER SYSTEM SET work_mem = '$WORK_MEM';" \
        -c "ALTER SYSTEM SET synchronous_commit = 'off';" \
        -c "SELECT pg_reload_conf();"
}

# Reverte só os parâmetros voláteis/arriscados pós-carga. maintenance_work_mem,
# max_wal_size e max_parallel ficam (ajudam queries e REFRESH do dia a dia).
reset_tuning() {
    [ "$TUNE" = "1" ] || return
    echo ">> revertendo synchronous_commit e work_mem ao default"
    "${PSQL[@]}" \
        -c "ALTER SYSTEM RESET synchronous_commit;" \
        -c "ALTER SYSTEM RESET work_mem;" \
        -c "SELECT pg_reload_conf();"
}

# \copy de todos os zips que casam com o glob para a tabela informada (COMPLETO).
copy_zips() {
    local table="$1"; shift
    local glob="$1"; shift
    shopt -s nullglob
    local files=( $DATA_DIR/$glob )
    shopt -u nullglob
    if [ ${#files[@]} -eq 0 ]; then
        echo "!! nenhum arquivo para $glob — pulando $table"; return
    fi
    for z in "${files[@]}"; do
        echo ">> COPY $(basename "$z") -> staging.$table"
        # tr -d '\000': remove bytes NUL que aparecem em alguns campos da Receita
        # (ex.: complemento) e quebram o COPY com "unterminated CSV quoted field".
        unzip -p "$z" | tr -d '\000' | "${PSQL[@]}" -c "\copy staging.$table FROM STDIN $COPY_OPTS"
    done
}

# \copy apenas das linhas cujo 1º campo (cnpj_basico) casa com o arquivo de
# padrões $1 — varre os zips inteiros via rg. Usado no modo amostra.
copy_zips_match() {
    local patterns="$1"; shift
    local table="$1"; shift
    local glob="$1"; shift
    shopt -s nullglob
    local files=( $DATA_DIR/$glob )
    shopt -u nullglob
    if [ ${#files[@]} -eq 0 ]; then
        echo "!! nenhum arquivo para $glob — pulando $table"; return
    fi
    for z in "${files[@]}"; do
        echo ">> MATCH $(basename "$z") -> staging.$table"
        # rg sai !=0 quando não há match no zip — tolerar com || true
        ( unzip -p "$z" | tr -d '\000' | rg -f "$patterns" || true ) \
            | "${PSQL[@]}" -c "\copy staging.$table FROM STDIN $COPY_OPTS"
    done
}

# \copy dos arquivos de regime tributário (entidades-*.zip). Diferente do COPY
# principal: delimitador VÍRGULA, e cada zip pode ter VÁRIOS CSVs (um por ano),
# cada um com cabeçalho -> filtra todas as linhas de header com grep -vi.
copy_regime() {
    shopt -s nullglob
    local files=( $DATA_DIR/entidades-*.zip )
    shopt -u nullglob
    if [ ${#files[@]} -eq 0 ]; then
        echo "!! nenhum entidades-*.zip — pulando regime tributário (fonte separada, ver load_regime.sh)"; return
    fi
    for z in "${files[@]}"; do
        echo ">> COPY $(basename "$z") -> staging.regime_tributario"
        unzip -p "$z" | tr -d '\000\r' | grep -vi '^ano,cnpj,cnpj_da_scp' \
            | "${PSQL[@]}" -c "\copy staging.regime_tributario FROM STDIN (FORMAT csv, DELIMITER ',', QUOTE '\"', ENCODING 'UTF8')"
    done
}

echo "== destino: banco '$DB' | modo: $( [ "$SAMPLE" -gt 0 ] && echo "AMOSTRA ($SAMPLE estab.)" || echo COMPLETO ) =="

apply_tuning

echo "== [1/5] schema =="
run_sql_file "$HERE/01_schema.sql"

echo "== [2/5] staging =="
run_sql_file "$HERE/02_staging.sql"

echo "== [3/5] COPY bruto dos CSVs =="
# lookups: sempre completos (são pequenos)
copy_zips cnaes        'Cnaes.zip'
copy_zips naturezas    'Naturezas.zip'
copy_zips qualificacoes 'Qualificacoes.zip'
copy_zips paises       'Paises.zip'
copy_zips motivos      'Motivos.zip'
copy_zips municipios   'Municipios.zip'

if [ "$SAMPLE" -gt 0 ]; then
    # --- âncora: head -N de UM zip de estabelecimentos ---
    estab_zip="$(ls "$DATA_DIR"/Estabelecimentos*.zip 2>/dev/null | head -1)"
    [ -n "$estab_zip" ] || { echo "!! sem Estabelecimentos*.zip"; exit 1; }
    echo ">> SAMPLE head -$SAMPLE $(basename "$estab_zip") -> staging.estabelecimentos"
    set +o pipefail
    unzip -p "$estab_zip" | tr -d '\000' | head -n "$SAMPLE" \
        | "${PSQL[@]}" -c "\copy staging.estabelecimentos FROM STDIN $COPY_OPTS"
    set -o pipefail

    # --- padrões ^"<basico>"; a partir dos básicos amostrados ---
    patterns="$(mktemp)"
    "${PSQL[@]}" -At -c \
        "SELECT DISTINCT cnpj_basico FROM staging.estabelecimentos;" \
        | sed 's/.*/^"&";/' > "$patterns"
    echo ">> $(wc -l < "$patterns") básicos distintos — casando empresas/sócios/simples"

    copy_zips_match "$patterns" empresas 'Empresas*.zip'
    copy_zips_match "$patterns" socios   'Socios*.zip'
    copy_zips_match "$patterns" simples  'Simples.zip'
    rm -f "$patterns"
else
    copy_zips empresas         'Empresas*.zip'
    copy_zips estabelecimentos 'Estabelecimentos*.zip'
    copy_zips socios           'Socios*.zip'
    copy_zips simples          'Simples.zip'
    copy_regime    # regime tributário (entidades-*.zip; fonte Nextcloud separada)
fi

echo "== [4/5] transform (staging -> analytics) =="
run_sql_file "$HERE/03_transform.sql"

echo "== [5/5] índices + materialized views =="
run_sql_file "$HERE/04_indexes.sql"
run_sql_file "$HERE/05_materialized_views.sql"

# staging já cumpriu o papel (foi consumido em 03_transform) -> liberar o espaço.
if [ "$KEEP_STAGING" = "1" ]; then
    echo ">> mantendo schema staging (KEEP_STAGING=1)"
else
    echo ">> removendo schema staging (libera ~27GB na carga completa)"
    "${PSQL[@]}" -c "DROP SCHEMA IF EXISTS staging CASCADE;"
fi

reset_tuning

echo "== concluído (banco '$DB') =="
"${PSQL[@]}" -c "SELECT 'empresa' t, count(*) FROM analytics.empresa
UNION ALL SELECT 'estabelecimento', count(*) FROM analytics.estabelecimento
UNION ALL SELECT 'socio', count(*) FROM analytics.socio
UNION ALL SELECT 'simples', count(*) FROM analytics.simples
UNION ALL SELECT 'regime_tributario', count(*) FROM analytics.regime_tributario;"
