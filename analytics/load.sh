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
#   DB        nome do banco de destino           (default: cnpj)
#   SAMPLE    se >0, gera amostra coerente        (default: 0 = base completa)
#   DATA_DIR  pasta com os zips da Receita        (default: ./data; ver nota abaixo)
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

# Os zips ficam no repo minha-receita; permita sobrescrever via DATA_DIR.
if [ ! -e "$DATA_DIR/Empresas0.zip" ] && [ -e "../minha-receita/data/Empresas0.zip" ]; then
    DATA_DIR="../minha-receita/data"
fi

# psql dentro do container; -T = sem TTY (essencial para pipe via STDIN)
PSQL=(docker compose exec -T postgres psql -U cnpj -d "$DB" -v ON_ERROR_STOP=1)
COPY_OPTS="(FORMAT csv, DELIMITER ';', QUOTE '\"', ENCODING 'LATIN9')"

run_sql_file() { echo ">> aplicando $1"; "${PSQL[@]}" < "$1"; }

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
        unzip -p "$z" | "${PSQL[@]}" -c "\copy staging.$table FROM STDIN $COPY_OPTS"
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
        ( unzip -p "$z" | rg -f "$patterns" || true ) \
            | "${PSQL[@]}" -c "\copy staging.$table FROM STDIN $COPY_OPTS"
    done
}

echo "== destino: banco '$DB' | modo: $( [ "$SAMPLE" -gt 0 ] && echo "AMOSTRA ($SAMPLE estab.)" || echo COMPLETO ) =="

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
    unzip -p "$estab_zip" | head -n "$SAMPLE" \
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
fi

echo "== [4/5] transform (staging -> analytics) =="
run_sql_file "$HERE/03_transform.sql"

echo "== [5/5] índices + materialized views =="
run_sql_file "$HERE/04_indexes.sql"
run_sql_file "$HERE/05_materialized_views.sql"

echo "== concluído (banco '$DB') =="
"${PSQL[@]}" -c "SELECT 'empresa' t, count(*) FROM analytics.empresa
UNION ALL SELECT 'estabelecimento', count(*) FROM analytics.estabelecimento
UNION ALL SELECT 'socio', count(*) FROM analytics.socio
UNION ALL SELECT 'simples', count(*) FROM analytics.simples;"
