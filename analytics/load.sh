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
#   IBGE_ONLY=1 bash analytics/load.sh      # só (re)preenche o de-para IBGE
#   SAMPLE=20000 bash analytics/load.sh     # amostra COERENTE de ~20k estabelecimentos
#   DB=cnpj_full bash analytics/load.sh     # carga completa em outro banco
#
# Variáveis:
#   DB            nome do banco de destino           (default: cnpj)
#   SAMPLE        se >0, gera amostra coerente        (default: 0 = base completa)
#   DATA_DIR      pasta com os zips da Receita        (default: ./data; ver nota)
#   TUNE          aplica tuning de carga (reload)     (default: 1; 0 desliga)
#   KEEP_STAGING  preserva o schema staging no fim    (default: 0 = dropa, ~27GB)
#   IBGE_ONLY     só o de-para IBGE (incremental)      (default: 0)
#   IBGE_REFRESH  rebaixa tabmun/IBGE ignorando cache  (default: 0)
#   IBGE_CACHE_MAX_DAYS  idade máx. do cache em dias    (default: 25)
#   IBGE_CACHE_DIR onde cachear as fontes do IBGE      (default: $DATA_DIR)
#
# --- Orçamento de recursos (Fase 0; spec-carga.md R3, seção 7.3) -------------
# Nada aqui é constante escolhida no olho: tudo sai do que o host tem no momento
# da carga. O servidor é COMPARTILHADO (8 vCPU e 16 GB com Airflow, Kong e
# Traefik) e NÃO TEM SWAP — estourar não é lentidão, é OOM kill, e a vítima pode
# ser o Postgres de outro serviço.
#
#   LOAD_JOBS     blocos simultâneos na Fase 3; derivado de nproc, teto 3
#                 (a 2.8 mediu 2,5× com 4 jobs, e o 4º core é da descompressão)
#   IDX_JOBS      índices construídos ao mesmo tempo na Fase 4 (default: LOAD_JOBS)
#   WORK_MEM      work_mem por OPERAÇÃO — não por conexão. Um INSERT complexo usa
#                 2 a 3 de uma vez, então o pico é LOAD_JOBS × ~3 × este valor.
#                 Derivado do orçamento; sobrescrevível por env.
#   ORCAMENTO_VCPU   vCPU que a carga pode ocupar   (default: min(4, nproc))
#   ORCAMENTO_RAM_MB teto de RAM da carga, em MB    (default: 3072)
#   REJEITO_LIMIAR_PCT  acima disto a carga AVISA (nunca aborta)  (default: 0.1)
#   CONTAR_DUP_SOCIO  conta duplicatas idênticas em socio (default: 0 — é caro)
#   COMPETENCIA   rótulo do mês em carga.resumo   (default: mês corrente)
#   MAX_PARALLEL_MAINT  workers paralelos p/ índices (derivado; ver Fase 0)
#   MAINT_WORK_MEM/MAX_WAL_SIZE  sobrescrevem o cálculo automático.
#   TUNE_RAM_GB   orçamento legado em GB; se definido, vira ORCAMENTO_RAM_MB.
#   TIMING        imprime tempo por fase + resumo  (default: 1; 0 desliga)
#   SQL_TIMING    	iming do psql (tempo por statement) (default: 1)
#   CARGA_TRANSFORM  `blocos` (default) ou `sequencial` — ver Fase 3
#   SIMULAR_LAYOUT_INVALIDO  força a reprovação de layout (só para teste)
#   NB: shared_buffers exige RESTART -> defina ANTES da carga (não é feito aqui).
#
# Amostra COERENTE: ancora em N estabelecimentos (head do 1º zip) e carrega apenas
# as empresas/sócios/simples cujo CNPJ básico aparece nesses estabelecimentos —
# garantindo que joins empresa↔filial↔sócio↔simples funcionem de ponta a ponta.
#
# Pré-requisitos: docker compose up -d postgres-cnpj-rfb ; unzip e rg (ripgrep) no PATH.
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
        _v="${_v%%[[:space:]]#*}"                       # corta comentário inline (` # ...`)
        _k="${_k#"${_k%%[![:space:]]*}"}"; _k="${_k%"${_k##*[![:space:]]}"}"   # trim
        _v="${_v#"${_v%%[![:space:]]*}"}"; _v="${_v%"${_v##*[![:space:]]}"}"   # trim
        case "$_k" in ''|*[!A-Za-z0-9_]*) continue;; esac  # nome de var inválido
        [ -n "${!_k+x}" ] && continue                   # já definida no shell: mantém
        export "$_k=$_v"
    done < "$HERE/../.env"
    unset _k _v
fi

DB="${DB:-cnpj}"
SAMPLE="${SAMPLE:-0}"
DATA_DIR="${DATA_DIR:-./data}"

# --- Fase 0: o orçamento de recursos (spec-carga.md R3, seção 7.3) -----------
#
# Antes, os knobs saíam de um único TUNE_RAM_GB chutado no arquivo. Agora saem
# do que o host REALMENTE tem no momento da carga — é o que o T7 cobra, e o
# motivo é que o servidor é compartilhado e sem swap: um número fixo que estava
# certo em março pode ser um OOM kill em setembro, quando outro serviço crescer.
#
# As Fases 3 e 4 NÃO se sobrepõem, então cada uma pode usar o teto inteiro. O
# que não pode é confundir "teto por fase" com "teto somado":
#
#                        | Fase 3 (transform)      | Fase 4 (índices)
#   teto total           | ORCAMENTO_RAM_MB        | ORCAMENTO_RAM_MB
#   jobs simultâneos     | LOAD_JOBS               | IDX_JOBS
#   parâmetro por job    | work_mem ≤ 256 MB       | maintenance_work_mem ≤ 512 MB
#   multiplicador oculto | ~3 operações por INSERT | 1 fatia por worker paralelo
#
# Os dois multiplicadores ocultos são o motivo de os números parecerem
# conservadores num host de 16 GB: 3 jobs × 3 operações × 256 MB já são 2,3 GB
# de pico teórico. Enquanto o T12 não medir o pico real, o número fica
# conservador de propósito.
TUNE="${TUNE:-1}"

# Teto de RAM. TUNE_RAM_GB é o nome antigo e continua valendo (está no README e
# no .env.example de instalações existentes); se vier, ele manda.
if [ -n "${TUNE_RAM_GB:-}" ]; then
    ORCAMENTO_RAM_MB="${ORCAMENTO_RAM_MB:-$((TUNE_RAM_GB * 1024))}"
fi
ORCAMENTO_RAM_MB="${ORCAMENTO_RAM_MB:-3072}"
TUNE_RAM_GB="${TUNE_RAM_GB:-$(awk "BEGIN{printf \"%.1f\", ${ORCAMENTO_RAM_MB}/1024}")}"

# vCPU. `nproc` pode não existir (container mínimo); o fallback é 2, pessimista
# de propósito — errar para menos custa tempo, errar para mais custa o host.
_nproc="$(nproc 2>/dev/null || echo 2)"
ORCAMENTO_VCPU="${ORCAMENTO_VCPU:-$(( _nproc < 4 ? _nproc : 4 ))}"

# LOAD_JOBS: teto de 3 porque a 2.8 mediu 2,5× com 4 jobs e o quarto core é da
# descompressão do zip, que roda no cliente. Um core sempre fica de fora
# (`- 1`): ele é do `unzip | tr`, do psql e de quem mais dividir o host.
_teto_jobs=$(( ORCAMENTO_VCPU - 1 ))
[ "$_teto_jobs" -lt 1 ] && _teto_jobs=1
[ "$_teto_jobs" -gt 3 ] && _teto_jobs=3
LOAD_JOBS="${LOAD_JOBS:-$_teto_jobs}"
IDX_JOBS="${IDX_JOBS:-$LOAD_JOBS}"

# O shared_buffers sai do MESMO teto, e é preciso descontá-lo antes de derivar
# qualquer coisa — foi o T12 que pegou isto. Com 3 GB de teto e 1 GB fixo de
# shared_buffers (decisão 3, fixado no docker-compose.yml), 3 jobs × 3 operações
# × 256 MB davam 2,3 GB, que somados ao próprio shared_buffers passam dos 3 GB.
# A tabela da seção 7.3 é explícita: a sobra POR FASE é 2 GB, não 3.
SHARED_BUFFERS_MB="${SHARED_BUFFERS_MB:-1024}"
_sobra=$(( ORCAMENTO_RAM_MB - SHARED_BUFFERS_MB ))
[ "$_sobra" -lt 256 ] && _sobra=256

# work_mem é por OPERAÇÃO. O divisor 3 é o número típico de operações de memória
# num INSERT ... SELECT com unnest e DISTINCT; o teto de 256 MB é da seção 7.3.
_wm=$(( _sobra / (LOAD_JOBS * 3 + 1) ))
[ "$_wm" -gt 256 ] && _wm=256
[ "$_wm" -lt 16 ] && _wm=16
WORK_MEM="${WORK_MEM:-${_wm}MB}"

# Cada worker paralelo de CREATE INDEX pega a SUA fatia de
# maintenance_work_mem. Com IDX_JOBS builds simultâneos e W workers cada, são
# IDX_JOBS × (W + 1) alocações, não IDX_JOBS. Por isso W = 1 (seção 7.3).
MAX_PARALLEL_MAINT="${MAX_PARALLEL_MAINT:-1}"
_mwm=$(( _sobra / (IDX_JOBS * (MAX_PARALLEL_MAINT + 1)) ))
[ "$_mwm" -gt 512 ] && _mwm=512
[ "$_mwm" -lt 64 ] && _mwm=64
MAINT_WORK_MEM="${MAINT_WORK_MEM:-${_mwm}MB}"
MAX_WAL_SIZE="${MAX_WAL_SIZE:-$(awk "BEGIN{printf \"%dGB\", ${ORCAMENTO_RAM_MB}/2048}")}"
unset _mwm _wm _teto_jobs _sobra

# Limiar de rejeito: acima dele a carga AVISA. Nunca aborta — é a regra 7.1, e
# ela existe porque não há ninguém de madrugada para decidir se o aviso importa.
REJEITO_LIMIAR_PCT="${REJEITO_LIMIAR_PCT:-0.1}"
# Contagem de duplicatas idênticas em `socio` (decisão 5.2). DESLIGADA por
# padrão: é um GROUP BY de 11 colunas sobre 27,8 milhões de linhas, e foi o
# pedaço mais caro dos 51 minutos que a Fase 4 gastava em varredura preventiva.
# Ela não alimenta nenhuma decisão da carga — só o contador que confirma, ao
# longo de alguns meses, se as 22 duplicatas são um conjunto congelado.
# Ligue quando quiser conferir: CONTAR_DUP_SOCIO=1 bash analytics/load.sh
CONTAR_DUP_SOCIO="${CONTAR_DUP_SOCIO:-0}"
COMPETENCIA="${COMPETENCIA:-$(date +%Y-%m)}"
# Mantém o schema staging após a carga (debug). Default: dropar e liberar ~27GB.
KEEP_STAGING="${KEEP_STAGING:-0}"
# Carga INCREMENTAL só do regime tributário (entidades-*.zip), sem tocar no resto.
REGIME_ONLY="${REGIME_ONLY:-0}"
# Carga INCREMENTAL só do de-para IBGE (dim_municipio.codigo_ibge/uf), sem tocar no resto.
IBGE_ONLY="${IBGE_ONLY:-0}"
# Cache das fontes do IBGE (tabmun.csv + ibge_municipios.json). Ficam no DATA_DIR
# para que a carga do watcher não dependa de rede: baixa uma vez, reusa depois.
IBGE_CACHE_DIR="${IBGE_CACHE_DIR:-$DATA_DIR}"
# 1 = rebaixa as fontes mesmo com cache válido (usar quando um município novo sair).
IBGE_REFRESH="${IBGE_REFRESH:-0}"
# Idade máxima do cache em dias; passou disso, rebaixa (cai no cache se a rede falhar).
IBGE_CACHE_MAX_DAYS="${IBGE_CACHE_MAX_DAYS:-25}"

# Os zips ficam no repo minha-receita; permita sobrescrever via DATA_DIR.
if [ ! -e "$DATA_DIR/Empresas0.zip" ] && [ -e "../minha-receita/data/Empresas0.zip" ]; then
    DATA_DIR="../minha-receita/data"
fi
# No modo REGIME_ONLY a âncora de detecção é o zip de regime, não Empresas0.
if [ ! -e "$DATA_DIR/entidades-lucro-real.zip" ] && [ -e "../minha-receita/data/entidades-lucro-real.zip" ]; then
    DATA_DIR="../minha-receita/data"
fi

# Nome do serviço do banco no compose (só usado no modo `docker compose exec`).
PG_SERVICE="${PG_SERVICE:-postgres-cnpj-rfb}"
# Conexão com o postgres — dois modos:
#  - PGHOST setado (ex.: dentro do compose, serviço `watcher-cnpj-rfb`): psql
#    direto via TCP. Requer PGPASSWORD. NÃO precisa do socket do Docker.
#  - senão (dev no host): via `docker compose exec` no serviço $PG_SERVICE.
# -T = sem TTY (essencial para o pipe via STDIN e p/ não injetar \r na saída).
if [ -n "${PGHOST:-}" ]; then
    PSQL=(psql -h "$PGHOST" -p "${PGPORT:-5432}" -U "${PGUSER:-cnpj}" -d "$DB" -v ON_ERROR_STOP=1)
    PSQL_ADMIN=(psql -h "$PGHOST" -p "${PGPORT:-5432}" -U "${PGUSER:-cnpj}" -d postgres -v ON_ERROR_STOP=1)
else
    PSQL=(docker compose exec -T "$PG_SERVICE" psql -U cnpj -d "$DB" -v ON_ERROR_STOP=1)
    PSQL_ADMIN=(docker compose exec -T "$PG_SERVICE" psql -U cnpj -d postgres -v ON_ERROR_STOP=1)
fi
COPY_OPTS="(FORMAT csv, DELIMITER ';', QUOTE '\"', ENCODING 'LATIN9')"

# --- Instrumentação: tempo por fase -----------------------------------------
# Puramente observacional: nada aqui altera o que é carregado. `phase <nome>`
# fecha a fase anterior e abre a nova; o resumo sai no fim (via trap EXIT, então
# uma carga que aborta no meio também mostra onde o tempo foi). Serve pra
# responder "o gargalo é COPY, transform ou índice?" antes de otimizar no chute.
#   TIMING=0     desliga a instrumentação inteira (saída original).
#   SQL_TIMING=0 desliga só o \timing do psql (duração por statement SQL).
TIMING="${TIMING:-1}"
SQL_TIMING="${SQL_TIMING:-1}"
LOAD_T0=$(date +%s)
_PHASE_NAME=""
_PHASE_T0=0
_PHASE_LOG=""

# '256MB' -> 256, '1GB' -> 1024, '512' -> 512. Existe porque os knobs de memória
# são sobrescrevíveis por env e podem chegar em qualquer uma dessas formas.
mb_de() {
    local v="${1:-0}" n
    n="${v//[^0-9]/}"
    [ -n "$n" ] || n=0
    case "$v" in *[gG][bB]*) n=$(( n * 1024 ));; esac
    printf '%s' "$n"
}

# segundos -> "1h02m03s"
fmt_dur() { printf '%dh%02dm%02ds' $(($1/3600)) $((($1%3600)/60)) $(($1%60)); }

# Preenche $1 até $2 colunas. Não dá pra usar `printf %-32s`: printf conta BYTES
# e nomes de fase têm acento (em UTF-8 "í" ocupa 2), o que desalinha a coluna.
# ${#s} conta CARACTERES, então o padding é calculado aqui.
_pad() {
    local s="$1" n=$(( $2 - ${#1} ))
    while [ "$n" -gt 0 ]; do s="$s "; n=$((n-1)); done
    printf '%s' "$s"
}

phase() {
    [ "$TIMING" = "1" ] || return 0
    phase_end
    _PHASE_NAME="$1"
    _PHASE_T0=$(date +%s)
    echo ">> [fase] $(date +%H:%M:%S) início: $_PHASE_NAME"
}

phase_end() {
    [ "$TIMING" = "1" ] || return 0
    [ -n "$_PHASE_NAME" ] || return 0
    local dur=$(( $(date +%s) - _PHASE_T0 ))
    echo ">> [fase] $(date +%H:%M:%S) fim: $_PHASE_NAME ($(fmt_dur $dur))"
    _PHASE_LOG+="$(_pad "$_PHASE_NAME" 32) $(printf '%10s' "$(fmt_dur $dur)")"$'\n'
    _PHASE_NAME=""
}

# Chamado pelo trap EXIT -> sai tanto no fim normal quanto num abort.
phase_summary() {
    [ "$TIMING" = "1" ] || return 0
    phase_end
    [ -n "$_PHASE_LOG" ] || return 0
    local total=$(( $(date +%s) - LOAD_T0 ))
    echo ""
    echo "================= tempo por fase ================="
    printf '%s' "$_PHASE_LOG"
    printf '%s %10s\n' "$(_pad TOTAL 32)" "$(fmt_dur $total)"
    echo "=================================================="
}
# Estado da janela perigosa: entre a Fase 2 e a Fase 4 a base fica SEM ÍNDICE.
FASE2_FEITA=0
FASE4_FEITA=0

# Chamado pelo trap EXIT -> roda tanto no fim normal quanto num abort.
#
# A recuperação é a razão de esta função existir, e ela NÃO é opcional: sem ela,
# uma falha entre as Fases 2 e 4 deixa a API respondendo com seq scan em 73
# milhões de linhas até alguém agir de manhã — e o watcher só retenta em 24 h
# (7.1). O T5 trava isso nos dois níveis: que os índices voltam, e que este trap
# continua chamando a recuperação.
recuperar_e_resumir() {
    local st=$?
    if [ "$FASE2_FEITA" = "1" ] && [ "$FASE4_FEITA" != "1" ]; then
        echo "!! saída anormal com os índices dropados — recriando antes de sair" >&2
        DB="$DB" bash "$HERE/recuperar_indices.sh" \
            || echo "!! a recuperação de índices FALHOU — rode na mão: DB=$DB bash analytics/recuperar_indices.sh" >&2
    fi
    phase_summary
    return "$st"
}
trap recuperar_e_resumir EXIT

# Mede uma etapa avulsa (um zip, p.ex.) sem abrir fase: só imprime a duração.
# Uso: _t0=$(date +%s); <comando>; step_done "rótulo" "$_t0"
step_done() {
    [ "$TIMING" = "1" ] || return 0
    echo ">>    $1 em $(fmt_dur $(( $(date +%s) - $2 )))"
}

# SQL_TIMING=1 prefixa `\timing on` no arquivo, então o psql imprime a duração de
# CADA statement — é assim que se descobre qual índice do 04_indexes.sql domina
# (suspeito nº 1: o gin_trgm em razao_social) sem quebrar o SQL em pedaços.
# Confere as ferramentas externas ANTES de qualquer COPY. Descobrir que
# falta uma depois de horas de carga é caro — e o modo amostra chegava a
# terminar "com sucesso" e base vazia quando o rg não existia.
# $1 = valor de SAMPLE (rg só é usado quando > 0).
check_deps() {
    local faltando=""
    command -v unzip >/dev/null 2>&1 || faltando="$faltando unzip"
    if [ "${1:-0}" -gt 0 ]; then
        command -v rg >/dev/null 2>&1 || faltando="$faltando rg(ripgrep)"
    fi
    if [ -n "$faltando" ]; then
        echo "!! dependência ausente no PATH:$faltando" >&2
        echo "!! instale e rode de novo (pré-requisitos no cabeçalho deste arquivo)." >&2
        echo "!! atenção: um 'rg' que seja função/alias do shell NÃO vale —" >&2
        echo "!! funções não passam para subprocessos; é preciso o binário." >&2
        exit 1
    fi
    return 0
}

# --- Fase 0: pré-voo ---------------------------------------------------------
#
# Observa o host e DEGRADA o paralelismo se ele já estiver ocupado. Nunca
# aborta: a regra 7.1 vale aqui inteira — uma carga lenta às 3 da manhã é um
# problema de manhã; uma carga que não rodou é um problema de mês.
preflight_orcamento() {
    local livre_mb carga_1m disco_gb
    # O `|| echo 0` NÃO basta: num host sem `/proc/loadavg` (Git Bash no
    # Windows) ou sem a linha `MemAvailable`, o awk termina com sucesso e saída
    # VAZIA — o `||` não dispara e a variável fica "", que faz o `[ -gt ]` abaixo
    # morrer com "integer expected". Daí a normalização para dígitos.
    # `MemAvailable` é a medida certa (conta o cache recuperável), mas nem todo
    # /proc/meminfo a tem — o do MSYS, por exemplo, só expõe MemTotal e MemFree.
    # Sem o fallback, `livre_mb` ficava 0 e a degradação por memória nunca
    # acontecia: um silêncio que passaria despercebido justamente no host onde o
    # OOM é o risco.
    livre_mb="$(awk '/^MemAvailable:/{printf "%d", $2/1024; found=1}
                     END{if (!found) print ""}' /proc/meminfo 2>/dev/null || echo 0)"
    [ -n "${livre_mb//[^0-9]/}" ] || \
        livre_mb="$(awk '/^MemFree:/{printf "%d", $2/1024}' /proc/meminfo 2>/dev/null || echo 0)"
    carga_1m="$(awk '{printf "%d", $1}' /proc/loadavg 2>/dev/null || echo 0)"
    disco_gb="$(df -BG . 2>/dev/null | awk 'NR==2{gsub(/G/,"",$4); print $4}' || echo 0)"
    livre_mb="${livre_mb//[^0-9]/}"; [ -n "$livre_mb" ] || livre_mb=0
    carga_1m="${carga_1m//[^0-9]/}"; [ -n "$carga_1m" ] || carga_1m=0
    disco_gb="${disco_gb//[^0-9]/}"; [ -n "$disco_gb" ] || disco_gb=0

    echo ">> [fase 0] host: ${_nproc} vCPU | ${livre_mb} MB livres | load ${carga_1m} | ${disco_gb} GB de disco"

    # vCPU realmente livres = o que o HOST tem menos o que já está rodando nele,
    # e só então limitado pela nossa cota. A conta na ordem inversa — cota menos
    # load do host — mistura duas grandezas diferentes e subestima sempre: num
    # servidor de 8 vCPU com load 2 e cota 4, ela daria 2 jobs onde cabem 3.
    #
    # O load average de 2,0–2,4 medido na 2.11 foi tirado COM a carga rodando; a
    # linha de base real é menor, então o que sobra aqui é conservador.
    local livres=$(( _nproc - carga_1m ))
    [ "$livres" -gt "$ORCAMENTO_VCPU" ] && livres="$ORCAMENTO_VCPU"
    [ "$livres" -lt 1 ] && livres=1
    local teto=$(( livres - 1 ))          # um core fica para unzip/tr/psql
    [ "$teto" -gt 3 ] && teto=3
    [ "$teto" -lt 1 ] && teto=1

    if [ "$LOAD_JOBS" -gt "$teto" ]; then
        echo ">> [fase 0] host acima do orçamento — degradando LOAD_JOBS de $LOAD_JOBS para $teto"
        LOAD_JOBS="$teto"
        IDX_JOBS="$teto"
    fi

    # Memória: se o host não tem o que prometemos, encolhe em vez de invadir.
    # O host NÃO TEM SWAP — aqui, estourar não é lentidão, é OOM kill.
    if [ "$livre_mb" -gt 0 ] && [ "$livre_mb" -lt "$ORCAMENTO_RAM_MB" ]; then
        echo ">> [fase 0] memória disponível (${livre_mb} MB) abaixo do orçamento (${ORCAMENTO_RAM_MB} MB) — degradando"
        ORCAMENTO_RAM_MB=$(( livre_mb * 8 / 10 ))
        local wm=$(( ORCAMENTO_RAM_MB / (LOAD_JOBS * 3 + 1) ))
        [ "$wm" -gt 256 ] && wm=256
        [ "$wm" -lt 16 ] && wm=16
        WORK_MEM="${wm}MB"
    fi

    echo ">> [fase 0] orçamento: LOAD_JOBS=$LOAD_JOBS | IDX_JOBS=$IDX_JOBS | work_mem=$WORK_MEM | maintenance_work_mem=$MAINT_WORK_MEM | teto=${ORCAMENTO_RAM_MB}MB"
    PREFLIGHT_RECURSOS="{\"vcpu\": $_nproc, \"mem_livre_mb\": ${livre_mb:-0}, \"load_1m\": ${carga_1m:-0}, \"disco_gb\": ${disco_gb:-0}}"
    PREFLIGHT_PARAMS="{\"load_jobs\": $LOAD_JOBS, \"idx_jobs\": $IDX_JOBS, \"work_mem\": \"$WORK_MEM\", \"maintenance_work_mem\": \"$MAINT_WORK_MEM\", \"orcamento_ram_mb\": $ORCAMENTO_RAM_MB}"
}

# Grava o baseline em carga.resumo. Silencioso se o schema ainda não existe
# (primeira carga num banco novo) — o transform reabre o resumo depois.
registrar_baseline() {
    "${PSQL[@]}" -q -c "SET carga.competencia = '$COMPETENCIA';
        SELECT carga.resumo_atual();
        UPDATE carga.resumo SET versao_codigo = '$(git -C "$HERE/.." rev-parse --short HEAD 2>/dev/null || echo desconhecida)',
                                parametros = '$PREFLIGHT_PARAMS'::jsonb,
                                recursos   = '$PREFLIGHT_RECURSOS'::jsonb
         WHERE competencia = '$COMPETENCIA' AND fim IS NULL;" >/dev/null 2>&1 || true
}

# --- R2.2: conferir o layout ANTES do COPY grande ----------------------------
#
# Mudança no número de colunas de um arquivo NÃO é sanitizável: não há regra que
# salve a linha, e o COPY vai falhar de qualquer jeito. O que a v2 controla é
# QUANDO se descobre. Falhar em 2 minutos é recuperável; falhar na hora 16 queima
# a janela inteira e deixa a base pela metade.
#
# A contagem é por `";"`, e não por `;`, porque o layout da Receita é todo
# quoted: um ponto e vírgula dentro de um campo (endereço, razão social) não é
# separador e não pode contar.
colunas_do_zip() {
    # `head -1` fecha o pipe assim que lê a primeira linha, e o `unzip` morre de
    # SIGPIPE — comportamento normal e desejado, já que ler 6,5 GB para contar
    # colunas seria absurdo. Só que sob `set -o pipefail` esse SIGPIPE faz o
    # pipeline inteiro reportar falha mesmo tendo lido a linha com sucesso.
    #
    # Isto não é hipotético: sem o `+o pipefail` abaixo, o `|| echo 0` do
    # chamador emendava um "0" depois do número certo e a variável virava "7\n0",
    # que o `[ -ne ]` rejeita com "integer expected". Com os zips stubados dos
    # testes o problema não aparece — o stub termina sozinho antes do SIGPIPE.
    set +o pipefail
    unzip -p "$1" 2>/dev/null | head -n 1 | awk 'NR==1{print gsub(/";"/, "") + 1}'
    set -o pipefail
}

checar_layout() {
    # Sem unzip não há o que conferir; quem reclama disso é o check_deps, com
    # mensagem melhor. Não roubar o erro dele.
    command -v unzip >/dev/null 2>&1 || return 0

    if [ "${SIMULAR_LAYOUT_INVALIDO:-0}" = "1" ]; then
        echo "!! layout reprovado: número de colunas diferente do esperado (simulação)" >&2
        return 1
    fi

    local falhou=0
    local par nome esperado glob achado z
    for par in "empresas:7:Empresas*.zip" \
               "estabelecimentos:30:Estabelecimentos*.zip" \
               "socios:11:Socios*.zip" \
               "simples:7:Simples.zip"; do
        IFS=':' read -r nome esperado glob <<< "$par"
        shopt -s nullglob
        local arquivos=( $DATA_DIR/$glob )
        shopt -u nullglob
        [ ${#arquivos[@]} -gt 0 ] || continue
        z="${arquivos[0]}"
        # Só dígitos: qualquer ruído que escape do pipe acima viraria um
        # "integer expected" no teste abaixo, e a conferência de layout não pode
        # ser a causa de uma falha de layout.
        achado="$(colunas_do_zip "$z")"
        achado="${achado//[^0-9]/}"
        [ -n "$achado" ] || achado=0
        if [ "$achado" -ne "$esperado" ]; then
            echo "!! layout de $(basename "$z"): $achado colunas, esperado $esperado (staging.$nome)" >&2
            falhou=1
        fi
    done

    if [ "$falhou" = "1" ]; then
        echo "!! o layout da Receita mudou — a carga para AQUI, antes do COPY." >&2
        echo "!! atualize analytics/02_staging.sql e a conferência acima antes de rodar de novo." >&2
        return 1
    fi
    echo ">> layout dos CSVs conferido: todos com o número de colunas esperado"
    return 0
}

# --- Execução em paralelo ----------------------------------------------------
#
# Lê comandos SQL de stdin, um por linha, e executa até $1 ao mesmo tempo. Os
# lotes são de tamanho fixo (espera o lote inteiro antes de abrir o próximo):
# é menos eficiente que reabastecer de um em um, e é deliberado — o tempo de um
# bloco é dominado pelo I/O da faixa, então as durações são parecidas, e a
# versão simples não tem como deixar processo órfão num abort.
executar_paralelo() {
    local n="$1"; shift
    local -a pids=()
    local cmd p falhas=0
    while IFS= read -r cmd; do
        [ -n "$cmd" ] || continue
        # `< /dev/null` é obrigatório, não higiene: sem ele o psql herda o MESMO
        # pipe que alimenta este `while read` e consome as linhas seguintes. O
        # sintoma é traiçoeiro — o primeiro comando roda, os demais somem, e a
        # carga termina com sucesso e tabelas vazias.
        "${PSQL[@]}" -q -c "$cmd" >/dev/null < /dev/null &
        pids+=("$!")
        if [ "${#pids[@]}" -ge "$n" ]; then
            for p in "${pids[@]}"; do wait "$p" || falhas=$((falhas + 1)); done
            pids=()
        fi
    done
    for p in "${pids[@]}"; do wait "$p" || falhas=$((falhas + 1)); done
    [ "$falhas" -eq 0 ]
}

run_sql_file() {
    echo ">> aplicando $1"
    local _t0; _t0=$(date +%s)
    if [ "$SQL_TIMING" = "1" ] && [ "$TIMING" = "1" ]; then
        { echo '\timing on'; cat "$1"; } | "${PSQL[@]}"
    else
        "${PSQL[@]}" < "$1"
    fi
    step_done "$(basename "$1")" "$_t0"
}

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
    # O pico da Fase 4 é IDX_JOBS builds × (workers + 1) fatias de
    # maintenance_work_mem — a multiplicação que a seção 7.3 chama de
    # "multiplicador escondido" e que já levou host sem swap ao OOM.
    # `mb_de` e não `${VAR%MB}`: MAINT_WORK_MEM/WORK_MEM podem vir do ambiente
    # como '1GB', e aí a expansão crua viraria um erro de sintaxe aritmética que,
    # sob `set -e`, derrubaria a carga inteira no meio do tuning.
    local pico_idx=$(( IDX_JOBS * (MAX_PARALLEL_MAINT + 1) * $(mb_de "$MAINT_WORK_MEM") ))
    echo ">> tuning de carga (teto=${ORCAMENTO_RAM_MB}MB | maint_work_mem=${MAINT_WORK_MEM} × ${IDX_JOBS} jobs × $((MAX_PARALLEL_MAINT + 1)) fatias ≈ ${pico_idx}MB | work_mem=${WORK_MEM} | wal=${MAX_WAL_SIZE})"
    # max_parallel_workers_per_gather = 0 na carga (seção 7.3): paralelismo de
    # consulta dentro de um bloco só brigaria com os outros blocos por CPU.
    "${PSQL[@]}" \
        -c "ALTER SYSTEM SET maintenance_work_mem = '$MAINT_WORK_MEM';" \
        -c "ALTER SYSTEM SET max_parallel_maintenance_workers = $MAX_PARALLEL_MAINT;" \
        -c "ALTER SYSTEM SET max_parallel_workers_per_gather = 0;" \
        -c "ALTER SYSTEM SET max_wal_size = '$MAX_WAL_SIZE';" \
        -c "ALTER SYSTEM SET work_mem = '$WORK_MEM';" \
        -c "ALTER SYSTEM SET synchronous_commit = 'off';" \
        -c "SELECT pg_reload_conf();"
}

# Reverte só os parâmetros voláteis/arriscados pós-carga. maintenance_work_mem,
# max_wal_size e max_parallel ficam (ajudam queries e REFRESH do dia a dia).
reset_tuning() {
    # `return 0` explícito: um `return` pelado herdaria o status 1 do teste
    # acima e, com `set -e`, derrubaria a carga inteira aqui no fim.
    [ "$TUNE" = "1" ] || return 0
    echo ">> revertendo synchronous_commit e work_mem ao default"
    "${PSQL[@]}" \
        -c "ALTER SYSTEM RESET synchronous_commit;" \
        -c "ALTER SYSTEM RESET work_mem;" \
        -c "ALTER SYSTEM RESET max_parallel_workers_per_gather;" \
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
        local _t0; _t0=$(date +%s)
        # tr -d '\000': remove bytes NUL que aparecem em alguns campos da Receita
        # (ex.: complemento) e quebram o COPY com "unterminated CSV quoted field".
        unzip -p "$z" | tr -d '\000' | "${PSQL[@]}" -c "\copy staging.$table FROM STDIN $COPY_OPTS"
        step_done "$(basename "$z")" "$_t0"
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
        local _t0; _t0=$(date +%s)
        # Só o exit 1 do rg ("nenhum match neste zip") é tolerável. O `|| true`
        # que havia aqui engolia também o 127 de binário ausente e o 2 de erro
        # real: cada zip virava COPY 0 e a amostra saía sem empresas/sócios,
        # com o script reportando sucesso. PIPESTATUS separa os casos.
        set +o pipefail
        unzip -p "$z" | tr -d '\000' | rg -f "$patterns" \
            | "${PSQL[@]}" -c "\copy staging.$table FROM STDIN $COPY_OPTS"
        local _st=("${PIPESTATUS[@]}")
        set -o pipefail
        if [ "${_st[2]}" -gt 1 ]; then
            echo "!! rg falhou (exit ${_st[2]}) em $(basename "$z") — abortando" >&2
            exit 1
        fi
        if [ "${_st[3]}" -ne 0 ]; then
            echo "!! COPY falhou (exit ${_st[3]}) em $(basename "$z") — abortando" >&2
            exit 1
        fi
        step_done "$(basename "$z")" "$_t0"
    done
}

# Cria a staging do regime (reusada pela carga completa e pela incremental). Fica
# aqui (e não no 02_staging.sql) para o modo REGIME_ONLY não recriar o resto.
create_regime_staging() {
    "${PSQL[@]}" <<'SQL'
CREATE SCHEMA IF NOT EXISTS staging;
DROP TABLE IF EXISTS staging.regime_tributario;
CREATE UNLOGGED TABLE staging.regime_tributario (  -- entidades-*.csv (5 colunas, VÍRGULA, c/ header)
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
        local _t0; _t0=$(date +%s)
        unzip -p "$z" | tr -d '\000\r' | grep -vi '^ano,cnpj,cnpj_da_scp' \
            | "${PSQL[@]}" -c "\copy staging.regime_tributario FROM STDIN (FORMAT csv, DELIMITER ',', QUOTE '\"', ENCODING 'UTF8')"
        step_done "$(basename "$z")" "$_t0"
    done
}

# --- de-para IBGE ------------------------------------------------------------
# O Municipios.csv da Receita só traz (codigo SIAFI, nome). O código IBGE vem de
# duas fontes externas, cacheadas em $IBGE_CACHE_DIR para que a carga disparada
# pelo watcher não fique refém da rede:
#   tabmun.csv            TABMUN do Tesouro (CKAN) — de-para SIAFI -> IBGE + UF.
#   ibge_municipios.json  API de localidades do IBGE — fallback por nome, cobre
#                         municípios novos que o TABMUN ainda não publicou.
TABMUN_CKAN_URL="${TABMUN_CKAN_URL:-https://www.tesourotransparente.gov.br/ckan/api/3/action/package_show?id=abb968cb-3710-4f85-89cf-875c91b9c7f6}"
IBGE_API_URL="${IBGE_API_URL:-https://servicodados.ibge.gov.br/api/v1/localidades/municipios}"

# Baixa $2 para $1; mantém o cache anterior se o download falhar ou vier vazio.
# O cache vale por IBGE_CACHE_MAX_DAYS dias — assim a carga mensal do watcher
# reatualiza o de-para (municípios novos) sem rebaixar a cada execução avulsa.
# Retorna != 0 só quando não há download NEM cache.
fetch_cached() {
    local dest="$1" url="$2" label="$3"
    local vencido=""
    [ -n "$(find "$dest" -mtime "+$IBGE_CACHE_MAX_DAYS" 2>/dev/null)" ] && vencido=1
    if [ -s "$dest" ] && [ "$IBGE_REFRESH" != "1" ] && [ -z "$vencido" ]; then
        echo ">> $label: usando cache $dest"; return 0
    fi
    mkdir -p "$(dirname "$dest")"
    local tmp="$dest.tmp"
    echo ">> $label: baixando $url"
    if curl -fsSL --retry 3 --retry-delay 2 --max-time 120 "$url" -o "$tmp" && [ -s "$tmp" ]; then
        mv -f "$tmp" "$dest"; return 0
    fi
    rm -f "$tmp"
    if [ -s "$dest" ]; then
        echo "!! $label: download falhou — seguindo com o cache $dest"; return 0
    fi
    echo "!! $label: download falhou e não há cache em $dest"; return 1
}

# A URL do CSV do tabmun muda quando o Tesouro republica o recurso, então ela é
# descoberta pelo package_show do CKAN. Se o CKAN não responder, cai na última
# URL conhecida (suficiente enquanto o recurso não for substituído).
tabmun_url() {
    local meta
    meta="$(curl -fsSL --retry 2 --max-time 60 "$TABMUN_CKAN_URL" 2>/dev/null || true)"
    local url
    url="$(printf '%s' "$meta" | grep -o 'https://[^"]*tabmun[^"]*[.]csv' | head -1)"
    if [ -n "$url" ]; then printf '%s' "$url"; return 0; fi
    printf '%s' "https://www.tesourotransparente.gov.br/ckan/dataset/abb968cb-3710-4f85-89cf-875c91b9c7f6/resource/eebb3bc6-9eea-4496-8bcf-304f33155282/download/tabmun.csv"
}

# Baixa (ou reusa o cache), cria as stagings e faz o COPY das duas fontes.
copy_ibge_sources() {
    local tabmun="$IBGE_CACHE_DIR/tabmun.csv"
    local ibgejson="$IBGE_CACHE_DIR/ibge_municipios.json"
    fetch_cached "$tabmun"   "$(tabmun_url)"  "tabmun"   || return 1
    fetch_cached "$ibgejson" "$IBGE_API_URL"  "ibge-api" || return 1

    "${PSQL[@]}" <<'SQL'
CREATE SCHEMA IF NOT EXISTS staging;
DROP TABLE IF EXISTS staging.tabmun;
CREATE TABLE staging.tabmun (   -- tabmun.csv: ';', SEM header, campos com padding
    codigo_siafi text,
    cnpj         text,
    nome         text,
    uf           text,
    codigo_ibge  text
);
DROP TABLE IF EXISTS staging.ibge_raw;
CREATE TABLE staging.ibge_raw (doc text);   -- JSON da API do IBGE, linha única
DROP TABLE IF EXISTS staging.ibge_municipios;
CREATE TABLE staging.ibge_municipios (codigo_ibge integer, nome text);
SQL

    echo ">> COPY tabmun.csv -> staging.tabmun"
    # QUOTE em \b: o arquivo não é quoted, e o " default quebraria num nome com aspas.
    tr -d '\000\r' < "$tabmun" \
        | "${PSQL[@]}" -c "\copy staging.tabmun FROM STDIN (FORMAT csv, DELIMITER ';', QUOTE E'\b', ENCODING 'LATIN9')"

    echo ">> COPY ibge_municipios.json -> staging.ibge_raw"
    # O JSON vem numa linha só; delimitador/quote em bytes de controle que não
    # ocorrem no conteúdo fazem o COPY tratá-lo como um único campo text.
    tr -d '\000\r' < "$ibgejson" \
        | "${PSQL[@]}" -c "\copy staging.ibge_raw FROM STDIN (FORMAT csv, DELIMITER E'\x01', QUOTE E'\x02', ENCODING 'UTF8')"

    "${PSQL[@]}" -c "INSERT INTO staging.ibge_municipios (codigo_ibge, nome)
                     SELECT (e->>'id')::integer, e->>'nome'
                     FROM staging.ibge_raw, jsonb_array_elements(doc::jsonb) e;"
}

# Preenche dim_municipio.codigo_ibge/uf. Não recria nem trunca nada mais.
load_ibge() {
    copy_ibge_sources || return 1
    run_sql_file "$HERE/ibge_transform.sql"
}

# --- Carga INCREMENTAL só do de-para IBGE (dim_municipio.codigo_ibge/uf).
# Serve para preencher o IBGE numa base JÁ carregada, sem esperar o próximo mês
# nem redisparar o load completo (que recarregaria os zips do mês corrente).
if [ "$IBGE_ONLY" = "1" ]; then
    echo "== de-para IBGE (INCREMENTAL) -> banco '$DB' | cache: $IBGE_CACHE_DIR =="
    ensure_db
    phase "IBGE (incremental)"
    load_ibge
    "${PSQL[@]}" -c "SELECT count(*) AS municipios,
                            count(codigo_ibge) AS com_ibge,
                            count(*) FILTER (WHERE uf IS NULL) AS sem_uf
                     FROM analytics.dim_municipio;"
    echo "== concluído (IBGE, banco '$DB') =="
    exit 0
fi

# --- Carga INCREMENTAL só do regime tributário (substitui o antigo load_regime.sh).
# Não aplica tuning, não recria o schema, não dropa o staging das outras tabelas.
if [ "$REGIME_ONLY" = "1" ]; then
    echo "== regime tributário (INCREMENTAL) -> banco '$DB' | data: $DATA_DIR =="
    check_deps 0          # só unzip; este modo não usa rg
    ensure_db
    phase "COPY regime (incremental)"
    create_regime_staging
    copy_regime
    phase "transform regime"
    echo ">> transform staging -> analytics.regime_tributario"
    run_sql_file "$HERE/regime_transform.sql"
    "${PSQL[@]}" -c "SELECT count(*) AS linhas, count(DISTINCT cnpj_basico) AS empresas,
                            min(ano) AS ano_min, max(ano) AS ano_max
                     FROM analytics.regime_tributario;"
    echo "== concluído (regime, banco '$DB') =="
    exit 0
fi

echo "== destino: banco '$DB' | modo: $( [ "$SAMPLE" -gt 0 ] && echo "AMOSTRA ($SAMPLE estab.)" || echo COMPLETO ) =="

# R2.2: a conferência de layout vem antes de TUDO — antes até do check_deps —
# porque é a checagem mais barata que existe e a que mais economiza quando
# falha. Ela se cala sozinha se faltar o unzip, para não roubar a mensagem do
# check_deps, que é mais específica.
phase "0 pré-voo"
checar_layout || exit 1

check_deps "$SAMPLE"      # rg exigido apenas no modo amostra
preflight_orcamento
ensure_db
apply_tuning

echo "== [1/6] schema (analytics + carga) =="
phase "1/6 schema"
run_sql_file "$HERE/00_carga.sql"
run_sql_file "$HERE/01_schema.sql"
registrar_baseline

echo "== [2/6] staging =="
phase "2/6 staging (DDL)"
run_sql_file "$HERE/02_staging.sql"

echo "== [3/6] COPY bruto dos CSVs =="
phase "3/6 COPY lookups"
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
    phase "COPY amostra estabelecimentos"
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

    phase "MATCH empresas"; copy_zips_match "$patterns" empresas 'Empresas*.zip'
    phase "MATCH socios";   copy_zips_match "$patterns" socios   'Socios*.zip'
    phase "MATCH simples";  copy_zips_match "$patterns" simples  'Simples.zip'
    rm -f "$patterns"
else
    phase "COPY empresas";         copy_zips empresas         'Empresas*.zip'
    phase "COPY estabelecimentos"; copy_zips estabelecimentos 'Estabelecimentos*.zip'
    phase "COPY socios";           copy_zips socios           'Socios*.zip'
    phase "COPY simples";          copy_zips simples          'Simples.zip'
    phase "COPY regime";           copy_regime   # entidades-*.zip (fonte separada)
fi

# --- Fase 2: dropar os índices ----------------------------------------------
#
# É ESTA fase que ataca as 16 horas da 2.11: 212 índices e 14 GB mantidos vivos
# durante o INSERT, através de um shared_buffers de 128 MB, com o INSERT parado
# em DataFileRead. A partir daqui a base fica sem índice, e o trap passa a ser
# a única rede — ver recuperar_e_resumir().
echo "== [4/6] dropando índices antes do transform =="
phase "4/6 drop de índices"
run_sql_file "$HERE/fase2_drop_indices.sql"
FASE2_FEITA=1

# --- Fase 3: transform -------------------------------------------------------
#
# Dois caminhos, mesmo resultado — e é o T10 que cobra isso, três vezes seguidas:
#
#   blocos      (default) faixas de ctid da staging, LOAD_JOBS em paralelo
#   sequencial  o 03_transform.sql, uma tabela por vez
#
# O caminho sequencial não é legado: é a REFERÊNCIA de conteúdo (é dele que sai
# o golden do T2) e é a válvula de escape. Se um mês der errado com os blocos, a
# spec é clara em que o ganho grande vem da Fase 2, não deles — `CARGA_TRANSFORM=sequencial`
# devolve a carga ao caminho conhecido sem desfazer nada.
CARGA_TRANSFORM="${CARGA_TRANSFORM:-blocos}"

if [ "$CARGA_TRANSFORM" = "sequencial" ]; then
    echo "== [5/6] transform (staging -> analytics), caminho SEQUENCIAL =="
    phase "5/6 transform (sequencial)"
    "${PSQL[@]}" -q -c "SET carga.competencia = '$COMPETENCIA';" >/dev/null 2>&1 || true
    run_sql_file "$HERE/03_transform.sql"
else

echo "== [5/6] transform (staging -> analytics), $LOAD_JOBS bloco(s) em paralelo =="
phase "5/6 transform"
echo ">> definindo as funções de bloco (03_transform_v2.sql)"
"${PSQL[@]}" -v driver=0 -q < "$HERE/03_transform_v2.sql"
"${PSQL[@]}" -q -c "SET carga.competencia = '$COMPETENCIA'; SELECT carga.preparar();" >/dev/null

# Dimensões e a tabela de CNPJ básico vão juntas num job só: a segunda é
# sequencial por contrato (PK viva, ver 03_transform_v2.sql) e as primeiras são
# pequenas demais para valer um bloco.
# As faixas são geradas ANTES e guardadas, para que um erro aqui apareça em vez
# de virar silêncio: uma consulta de faixas que falha produziria zero blocos, e a
# carga terminaria "com sucesso" e a tabela vazia.
faixas_de() {
    "${PSQL[@]}" -tAq -c "$1" < /dev/null || {
        echo "!! não foi possível calcular as faixas de blocos — abortando a Fase 3" >&2
        return 1
    }
}

{
    echo "SET carga.competencia = '$COMPETENCIA'; SELECT carga.carregar_dimensoes(); SELECT carga.carregar_empresa();"
    # Os dois consumidores da MESMA staging saem adjacentes de propósito: lidos
    # ao mesmo tempo, o segundo acha em cache o que o primeiro trouxe — 22% mais
    # barato, medido na 2.9 (320 s contra 409 s).
    faixas_de "SELECT format('SET carga.competencia = %L; SELECT carga.carregar_estabelecimento(%L::tid, %L::tid);', '$COMPETENCIA', de, ate)
                    || E'\n'
                    || format('SET carga.competencia = %L; SELECT carga.carregar_cnae_secundario(%L::tid, %L::tid);', '$COMPETENCIA', de, ate)
                 FROM carga.faixas('staging.estabelecimentos', $LOAD_JOBS) ORDER BY bloco"
    faixas_de "SELECT format('SET carga.competencia = %L; SELECT carga.carregar_socio(%L::tid, %L::tid);', '$COMPETENCIA', de, ate)
                 FROM carga.faixas('staging.socios', $LOAD_JOBS) ORDER BY bloco"
    faixas_de "SELECT format('SET carga.competencia = %L; SELECT carga.carregar_simples(%L::tid, %L::tid);', '$COMPETENCIA', de, ate)
                 FROM carga.faixas('staging.simples', $LOAD_JOBS) ORDER BY bloco"
} | executar_paralelo "$LOAD_JOBS"

"${PSQL[@]}" -q -c "SET carga.competencia = '$COMPETENCIA'; SELECT carga.finalizar();" >/dev/null

fi   # fim da escolha entre os dois caminhos do transform

# --- O aviso de rejeito ------------------------------------------------------
#
# Um mês que rejeite acima do limiar AVISA e segue. Nunca aborta: não há ninguém
# de madrugada para decidir se o aviso importa, e abortar transformaria um
# problema de qualidade de dado num problema de disponibilidade (7.1).
resumo_rejeito() {
    local linha
    linha="$("${PSQL[@]}" -tAq -c "
        SELECT coalesce(string_agg(format('%s/%s=%s', tabela, regra, quantidade), ' '), 'nenhum')
          FROM carga.resumo_regras
         WHERE competencia = '$COMPETENCIA' AND regra ~ '^S[0-9]'" 2>/dev/null || echo "")"
    echo ">> rejeito da competência $COMPETENCIA (limiar de aviso: ${REJEITO_LIMIAR_PCT}%): ${linha:-nenhum}"
    local acima
    acima="$("${PSQL[@]}" -tAq -c "
        SELECT coalesce(string_agg(tabela || ' ' || round(pct, 3) || '%', ', '), '')
          FROM (SELECT r.tabela,
                       100.0 * count(*) / nullif(max(c.quantidade), 0) AS pct
                  FROM carga.rejeito r
                  JOIN carga.contador c ON c.tabela = r.tabela
                                       AND c.competencia = r.competencia
                                       AND c.regra = 'linhas_lidas'
                 WHERE r.competencia = '$COMPETENCIA'
                 GROUP BY r.tabela) t
         WHERE pct > $REJEITO_LIMIAR_PCT" 2>/dev/null || echo "")"
    if [ -n "${acima// /}" ]; then
        echo "!! AVISO: rejeito acima do limiar de ${REJEITO_LIMIAR_PCT}% em: $acima" >&2
        echo "!! a carga NÃO foi interrompida (7.1). Investigue com:" >&2
        echo "!!   SELECT * FROM carga.rejeito WHERE competencia = '$COMPETENCIA' LIMIT 50;" >&2
    fi
}
resumo_rejeito

# de-para IBGE em dim_municipio (fontes externas + cache). Nem falha de rede nem
# validação reprovada podem derrubar uma carga de horas: avisa e segue — o passo
# é reexecutável isoladamente depois.
echo "== de-para IBGE (dim_municipio) =="
phase "de-para IBGE"
load_ibge || echo "!! de-para IBGE NÃO aplicado (rede ou validação) — rode depois: IBGE_ONLY=1 DB=$DB bash analytics/load.sh"

# --- Fase 4: reconstruir os índices -----------------------------------------
#
# Depois da Fase 2, esta é a fase mais cara da carga — 14 GB de índice para
# construir. E é também o paralelismo mais barato: 212 índices independentes,
# nenhum ON CONFLICT, nenhuma ordem a preservar, nada de determinismo em jogo.
#
# Três passos, e a ordem importa: a detecção de duplicata vem ANTES, porque é
# ela que decide se cada índice sai único ou não-único (7.1).
echo "== [6/6] índices ($IDX_JOBS em paralelo) + materialized views =="
phase "6/6 pré-índices"
"${PSQL[@]}" -v fazer_indices=0 -v contar_dup_socio="$CONTAR_DUP_SOCIO" -q     < "$HERE/fase4_indices.sql" >/dev/null

phase "6/6 índices"
"${PSQL[@]}" -tAq -c "SELECT format('SELECT carga.recriar_indice(%L);', indice)
                        FROM carga.indice_salvo
                       WHERE schema_nome = 'analytics' AND dropado
                       ORDER BY tabela, e_constraint DESC, indice" 2>/dev/null \
    | executar_paralelo "$IDX_JOBS"

# A segunda passada fecha o que sobrou (índice que falhou num job, a conversão
# da PK num mês sujo) e roda o ANALYZE. É idempotente: o que já existe é pulado.
"${PSQL[@]}" -v fazer_duplicatas=0 -v contar_dup_socio=0 -q < "$HERE/fase4_indices.sql"

# Os índices voltaram: a janela perigosa fechou e o trap não precisa mais agir.
FASE4_FEITA=1

# O 04_indexes.sql continua sendo a FONTE das definições — ele cria o que ainda
# não existe (primeira carga num banco novo, ou índice acrescentado desde a
# última). Depois da Fase 4 ele não tem o que fazer, e é isso que garante que
# não existam duas definições concorrentes do mesmo índice (R1).
phase "6/6 índices (fonte)"
run_sql_file "$HERE/04_indexes.sql"
phase "6/6 materialized views"
run_sql_file "$HERE/05_materialized_views.sql"

# regime tributário: transform isolado (sempre; na amostra a staging está vazia,
# então só cria a tabela final vazia — mantém o schema consistente p/ a API).
echo "== regime tributário (transform) =="
phase "transform regime"
run_sql_file "$HERE/regime_transform.sql"

# staging já cumpriu o papel (foi consumido em 03_transform) -> liberar o espaço.
if [ "$KEEP_STAGING" = "1" ]; then
    echo ">> mantendo schema staging (KEEP_STAGING=1)"
else
    echo ">> removendo schema staging (libera ~27GB na carga completa)"
    phase "drop staging"
    "${PSQL[@]}" -c "DROP SCHEMA IF EXISTS staging CASCADE;"
fi

reset_tuning

# --- Fechamento do resumo ----------------------------------------------------
#
# `desfecho` tem TRÊS estados, e o terceiro é a razão de existir: um mês com
# duplicata termina 'degradado', não 'falha'. A diferença é operacional — o
# watcher decide por returncode, e não pode reagendar uma recarga de 6 horas por
# causa de uma linha repetida (7.1).
#
# Sobre `pico_rss_mb`: o que se mede aqui é o pico do PROCESSO DE CARGA e seus
# filhos (psql, unzip, tr), lido de /proc. O consumo que mais importa para o teto
# de 3 GB é o do servidor Postgres — work_mem × operações × blocos —, e ele NÃO é
# visível daqui quando o banco está noutro container (que é o caso no servidor).
# Por isso o orçamento derivado na Fase 0 também é gravado em `parametros`: é
# contra ele que o T12 confere que o pico TEÓRICO cabe no teto. Enquanto não
# houver medição do lado do servidor, o número fica conservador de propósito.
fechar_resumo() {
    local pico_kb pico_mb
    pico_kb="$(cat /proc/$$/status 2>/dev/null | awk '/VmHWM/{print $2}' || echo "")"
    pico_mb=$(( ${pico_kb:-0} / 1024 ))
    [ "$pico_mb" -lt 1 ] && pico_mb=1
    local teorico=$(( LOAD_JOBS * 3 * $(mb_de "$WORK_MEM") ))
    local desfecho
    desfecho="$("${PSQL[@]}" -tAq -c "
        SELECT CASE WHEN EXISTS (SELECT 1 FROM carga.duplicata WHERE competencia = '$COMPETENCIA')
                    THEN 'degradado' ELSE 'sucesso' END" 2>/dev/null | tr -d '[:space:]' || echo sucesso)"
    [ -n "$desfecho" ] || desfecho=sucesso
    "${PSQL[@]}" -q -c "
        UPDATE carga.resumo
           SET fim = now(), desfecho = '$desfecho', pico_rss_mb = $pico_mb,
               parametros = coalesce(parametros, '{}'::jsonb)
                            || jsonb_build_object('pico_teorico_mb', $teorico)
         WHERE competencia = '$COMPETENCIA' AND fim IS NULL;" >/dev/null 2>&1 || true
    echo ">> carga registrada em carga.resumo: desfecho=$desfecho | pico do processo=${pico_mb}MB | pico teórico no servidor=${teorico}MB"
    if [ "$desfecho" = "degradado" ]; then
        echo "!! SUCESSO DEGRADADO: houve duplicata de chave natural neste mês." >&2
        echo "!! O índice afetado saiu NÃO-ÚNICO e as chaves estão em carga.duplicata." >&2
        echo "!! Não é motivo para recarregar: a carga seguinte volta ao normal se o mês vier limpo." >&2
    fi
}
fechar_resumo

echo "== concluído (banco '$DB') =="
phase "contagens finais"
"${PSQL[@]}" -c "SELECT 'empresa' t, count(*) FROM analytics.empresa
UNION ALL SELECT 'estabelecimento', count(*) FROM analytics.estabelecimento
UNION ALL SELECT 'socio', count(*) FROM analytics.socio
UNION ALL SELECT 'simples', count(*) FROM analytics.simples
UNION ALL SELECT 'regime_tributario', count(*) FROM analytics.regime_tributario;"
