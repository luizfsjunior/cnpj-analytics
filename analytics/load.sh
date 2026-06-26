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
#   TUNE_RAM_GB   orçamento de RAM para o tuning       (default: 6 GB)
#                 Todos os knobs são calculados proporcionalmente a este valor.
#                 Pico de RAM durante índices ≈ TUNE_RAM_GB * 60%.
#   KEEP_STAGING  preserva o schema staging no fim    (default: 0 = dropa, ~27GB)
#   MAX_PARALLEL_MAINT  workers paralelos p/ índices  (default: 4)
#   MAINT_WORK_MEM/WORK_MEM/MAX_WAL_SIZE  sobrescrevem o cálculo automático.
#   NB: shared_buffers exige RESTART -> defina ANTES da carga (não é feito aqui).
#
# Amostra COERENTE: ancora em N estabelecimentos (head do 1º zip) e carrega apenas
# as empresas/sócios/simples cujo CNPJ básico aparece nesses estabelecimentos —
# garantindo que joins empresa↔filial↔sócio↔simples funcionem de ponta a ponta.
#
# Pré-requisitos: docker compose up -d postgres ; unzip e rg (ripgrep) no PATH.
# ============================================================================
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
# Carrega o .env da raiz do repo (se existir) para que as variáveis abaixo —
# inclusive as de tuning (TUNE, TUNE_RAM_GB, ...) — possam ser definidas lá.
# Só preenche o que ainda NÃO veio do ambiente, então valores passados na linha
# de comando (ex.: `TUNE_RAM_GB=64 bash load.sh`) têm prioridade sobre o .env.
if [ -f "$HERE/../.env" ]; then
    while IFS='=' read -r _k _v; do
        _k="${_k%$'\r'}"; _v="${_v%$'\r'}"              # tolera CRLF (.env salvo no Windows)
        case "$_k" in ''|\#*) continue;; esac          # ignora vazias/comentários
        [ -n "${!_k+x}" ] && continue                   # já definida no shell: mantém
        export "$_k=$_v"
    done < "$HERE/../.env"
    unset _k _v
fi

DB="${DB:-cnpj}"
SAMPLE="${SAMPLE:-0}"
DATA_DIR="${DATA_DIR:-./data}"

# --- Tuning de carga (reload-only; NÃO derruba a sessão nem exige restart) ------
# TUNE_RAM_GB = orçamento de RAM que o load pode usar (default: 6 GB).
# Os knobs são calculados a partir desse único valor:
#   maintenance_work_mem = TUNE_RAM_GB * 0.6 / (workers+1)  → pico = 60% da cota
#   work_mem             = TUNE_RAM_GB * 0.015               → ~1.5% da cota
#   max_wal_size         = TUNE_RAM_GB * 0.5                 → 50% da cota
# Sobrescreva qualquer knob individualmente via env pra forçar um valor fixo.
# Desligue tudo com TUNE=0. Detalhes: analytics/tuning-carga.md.
# OBS: shared_buffers exige RESTART -> defina ANTES da carga (não é feito aqui).
TUNE="${TUNE:-1}"
TUNE_RAM_GB="${TUNE_RAM_GB:-6}"
MAX_PARALLEL_MAINT="${MAX_PARALLEL_MAINT:-4}"
# Calculados a partir de TUNE_RAM_GB; sobrescrevíveis individualmente por env.
_mwm=$(awk "BEGIN{printf \"%d\", ${TUNE_RAM_GB}*0.6/(${MAX_PARALLEL_MAINT}+1)*1024}")
_wm=$(awk  "BEGIN{printf \"%d\", ${TUNE_RAM_GB}*0.015*1024}")
_wal=$(awk "BEGIN{printf \"%d\", ${TUNE_RAM_GB}*0.5}")
MAINT_WORK_MEM="${MAINT_WORK_MEM:-${_mwm}MB}"
WORK_MEM="${WORK_MEM:-${_wm}MB}"
MAX_WAL_SIZE="${MAX_WAL_SIZE:-${_wal}GB}"
unset _mwm _wm _wal
# Mantém o schema staging após a carga (debug). Default: dropar e liberar ~27GB.
KEEP_STAGING="${KEEP_STAGING:-0}"
# Carga INCREMENTAL só do regime tributário (entidades-*.zip), sem tocar no resto.
REGIME_ONLY="${REGIME_ONLY:-0}"

# Os zips ficam no repo minha-receita; permita sobrescrever via DATA_DIR.
if [ ! -e "$DATA_DIR/Empresas0.zip" ] && [ -e "../minha-receita/data/Empresas0.zip" ]; then
    DATA_DIR="../minha-receita/data"
fi
# No modo REGIME_ONLY a âncora de detecção é o zip de regime, não Empresas0.
if [ ! -e "$DATA_DIR/entidades-lucro-real.zip" ] && [ -e "../minha-receita/data/entidades-lucro-real.zip" ]; then
    DATA_DIR="../minha-receita/data"
fi

# Conexão com o postgres — dois modos:
#  - PGHOST setado (ex.: dentro do compose, serviço `watcher`): psql direto via
#    TCP. Requer PGPASSWORD. NÃO precisa do socket do Docker dentro do container.
#  - senão (dev no host): via `docker compose exec` no container `postgres`.
# -T = sem TTY (essencial para o pipe via STDIN e p/ não injetar \r na saída).
if [ -n "${PGHOST:-}" ]; then
    PSQL=(psql -h "$PGHOST" -p "${PGPORT:-5432}" -U "${PGUSER:-cnpj}" -d "$DB" -v ON_ERROR_STOP=1)
    PSQL_ADMIN=(psql -h "$PGHOST" -p "${PGPORT:-5432}" -U "${PGUSER:-cnpj}" -d postgres -v ON_ERROR_STOP=1)
else
    PSQL=(docker compose exec -T postgres psql -U cnpj -d "$DB" -v ON_ERROR_STOP=1)
    PSQL_ADMIN=(docker compose exec -T postgres psql -U cnpj -d postgres -v ON_ERROR_STOP=1)
fi
COPY_OPTS="(FORMAT csv, DELIMITER ';', QUOTE '\"', ENCODING 'LATIN9')"

run_sql_file() { echo ">> aplicando $1"; "${PSQL[@]}" < "$1"; }

# Cria o banco destino se ainda não existir (idempotente). O compose só cria o
# banco `cnpj`; cargas em `cnpj_full` (default do watcher) exigem o banco antes.
# Conecta no db de manutenção `postgres`.
ensure_db() {
    if ! "${PSQL_ADMIN[@]}" -tAc "SELECT 1 FROM pg_database WHERE datname = '$DB'" | grep -q 1; then
        echo ">> criando banco '$DB'"
        "${PSQL_ADMIN[@]}" -c "CREATE DATABASE \"$DB\""
    fi
}

# Aplica o tuning de carga via ALTER SYSTEM + pg_reload_conf (SIGHUP). Cada ALTER
# vai num -c separado porque ALTER SYSTEM não roda dentro de transação.
# Requer superuser (no compose o user 'cnpj' é superuser; num servidor, conferir).
apply_tuning() {
    [ "$TUNE" = "1" ] || { echo ">> tuning de carga DESATIVADO (TUNE=0)"; return; }
    local pico; pico=$(awk "BEGIN{printf \"%.1f\", ${TUNE_RAM_GB}*0.6}")
    echo ">> tuning de carga (cota=${TUNE_RAM_GB}GB | maint_work_mem=${MAINT_WORK_MEM} | workers=${MAX_PARALLEL_MAINT} | pico índices≈${pico}GB | work_mem=${WORK_MEM} | wal=${MAX_WAL_SIZE})"
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
        if [ ! -f "$z" ]; then
            echo "!! $(basename "$z") não encontrado em $DATA_DIR — pulando $table"; continue
        fi
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

# Cria a staging do regime (reusada pela carga completa e pela incremental). Fica
# aqui (e não no 02_staging.sql) para o modo REGIME_ONLY não recriar o resto.
create_regime_staging() {
    "${PSQL[@]}" <<'SQL'
CREATE SCHEMA IF NOT EXISTS staging;
DROP TABLE IF EXISTS staging.regime_tributario;
CREATE TABLE staging.regime_tributario (  -- entidades-*.csv (5 colunas, VÍRGULA, c/ header)
    ano                          text,
    cnpj                         text,     -- completo e formatado: 00.000.000/0001-91
    cnpj_da_scp                  text,     -- '0' = sem SCP
    forma_de_tributacao          text,
    quantidade_de_escrituracoes  text
);
SQL
}

# \copy dos arquivos de regime tributário (entidades-*.zip). Diferente do COPY
# principal: delimitador VÍRGULA, e cada zip pode ter VÁRIOS CSVs (um por ano),
# cada um com cabeçalho -> filtra todas as linhas de header com grep -vi.
copy_regime() {
    shopt -s nullglob
    local files=( $DATA_DIR/entidades-*.zip )
    shopt -u nullglob
    if [ ${#files[@]} -eq 0 ]; then
        echo "!! nenhum entidades-*.zip — pulando regime tributário (fonte Nextcloud separada, ver fontes-dados.md)"; return
    fi
    for z in "${files[@]}"; do
        echo ">> COPY $(basename "$z") -> staging.regime_tributario"
        unzip -p "$z" | tr -d '\000\r' | grep -vi '^ano,cnpj,cnpj_da_scp' \
            | "${PSQL[@]}" -c "\copy staging.regime_tributario FROM STDIN (FORMAT csv, DELIMITER ',', QUOTE '\"', ENCODING 'UTF8')"
    done
}

# --- Carga INCREMENTAL só do regime tributário (substitui o antigo load_regime.sh).
# Não aplica tuning, não recria o schema, não dropa o staging das outras tabelas.
if [ "$REGIME_ONLY" = "1" ]; then
    echo "== regime tributário (INCREMENTAL) -> banco '$DB' | data: $DATA_DIR =="
    ensure_db
    create_regime_staging
    copy_regime
    echo ">> transform staging -> analytics.regime_tributario"
    run_sql_file "$HERE/regime_transform.sql"
    "${PSQL[@]}" -c "SELECT count(*) AS linhas, count(DISTINCT cnpj_basico) AS empresas,
                            min(ano) AS ano_min, max(ano) AS ano_max
                     FROM analytics.regime_tributario;"
    echo "== concluído (regime, banco '$DB') =="
    exit 0
fi

echo "== destino: banco '$DB' | modo: $( [ "$SAMPLE" -gt 0 ] && echo "AMOSTRA ($SAMPLE estab.)" || echo COMPLETO ) =="

ensure_db
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

# staging do regime sempre criada (na amostra fica vazia -> tabela final vazia,
# mas existente, p/ a API não quebrar). COPY só no modo completo.
create_regime_staging

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
    copy_regime                            # COPY dos entidades-*.zip (fonte separada)
fi

echo "== [4/5] transform (staging -> analytics) =="
run_sql_file "$HERE/03_transform.sql"

echo "== [5/5] índices + materialized views =="
run_sql_file "$HERE/04_indexes.sql"
run_sql_file "$HERE/05_materialized_views.sql"

# regime tributário: transform isolado (sempre; na amostra a staging está vazia,
# então só cria a tabela final vazia — mantém o schema consistente p/ a API).
echo "== regime tributário (transform) =="
run_sql_file "$HERE/regime_transform.sql"

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
