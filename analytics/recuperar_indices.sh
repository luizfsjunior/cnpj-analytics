#!/usr/bin/env bash
# ============================================================================
# recuperar_indices.sh — recria os índices de `analytics` a partir do que a
# Fase 2 salvou em `carga.indice_salvo`.
#
# Este script existe por causa do único ponto em que o desenho da v2 piora o
# estado atual: entre a Fase 2 e a Fase 4 a base fica SEM ÍNDICE. Uma falha
# nessa janela deixaria a API respondendo com seq scan em 73 milhões de linhas
# até alguém agir — e, pela 7.1, não há ninguém de madrugada: o watcher só
# retenta em 24 h.
#
# Por isso ele é chamado pelo `trap EXIT` do load.sh em QUALQUER saída anormal
# depois da Fase 2, antes de o erro se propagar. O teste T5 verifica as duas
# coisas: que os índices voltam, e que o trap continua chamando isto.
#
# Uso:
#   bash analytics/recuperar_indices.sh          # banco $DB (default: cnpj)
#   DB=cnpj_full bash analytics/recuperar_indices.sh
#
# É seguro rodar na mão, a qualquer momento e quantas vezes quiser: a Fase 4 é
# idempotente — índice que já existe é pulado.
# ============================================================================
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
DB="${DB:-cnpj}"
PG_SERVICE="${PG_SERVICE:-postgres-cnpj-rfb}"

if [ -n "${PGHOST:-}" ]; then
    PSQL=(psql -h "$PGHOST" -p "${PGPORT:-5432}" -U "${PGUSER:-cnpj}" -d "$DB" -v ON_ERROR_STOP=1)
else
    PSQL=(docker compose exec -T "$PG_SERVICE" psql -U cnpj -d "$DB" -v ON_ERROR_STOP=1)
fi

# Nada salvo = a Fase 2 nunca rodou neste banco = não há o que recuperar.
# Sair com 0 aqui é deliberado: chamado pelo trap, este script não pode virar
# a causa de um segundo erro em cima do erro que já estava acontecendo.
pendentes="$("${PSQL[@]}" -tAc \
    "SELECT count(*) FROM carga.indice_salvo WHERE schema_nome = 'analytics' AND dropado" \
    2>/dev/null || echo 0)"
pendentes="${pendentes//[^0-9]/}"

if [ -z "$pendentes" ] || [ "$pendentes" = "0" ]; then
    echo ">> recuperação de índices: nada pendente em '$DB'"
    exit 0
fi

echo ">> recuperando $pendentes índice(s) de analytics em '$DB' (Fase 4)"
"${PSQL[@]}" < "$HERE/fase4_indices.sql"
echo ">> recuperação de índices concluída em '$DB'"
