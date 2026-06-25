// Package api expõe os endpoints analíticos HTTP sobre o schema analytics.
package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"cnpj-analytics/internal/db"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// App agrega as dependências dos handlers.
type App struct {
	pool *pgxpool.Pool
}

// NewApp cria a aplicação a partir da conexão.
func NewApp(d *db.DB) *App { return &App{pool: d.Pool} }

// Handler monta o roteador com todas as rotas (padrões method+path do Go 1.22).
func (a *App) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", a.health)
	mux.HandleFunc("GET /stats/capital-por-natureza", a.capitalPorNatureza)
	mux.HandleFunc("GET /stats/empresas", a.statsEmpresas)
	mux.HandleFunc("GET /stats/regime", a.statsRegime)
	mux.HandleFunc("GET /empresas/{cnpj}", a.empresaDetalhe)
	mux.HandleFunc("GET /filial/{cnpj}", a.filialDetalhe)
	mux.HandleFunc("GET /socios", a.socios)
	return logging(mux)
}

// logging registra método, rota e duração de cada requisição.
func logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		slog.Info("request", "method", r.Method, "path", r.URL.Path, "dur", time.Since(start))
	})
}

// writeJSON serializa v como JSON com o status informado.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// queryRows executa a query e devolve as linhas como []map[string]any,
// genérico o suficiente para qualquer SELECT analítico.
func (a *App) queryRows(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
	rows, err := a.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return pgx.CollectRows(rows, pgx.RowToMap)
}
