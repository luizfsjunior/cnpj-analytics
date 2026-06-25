package api

import (
	"net/http"
	"strconv"
)

// health confirma a conexão com o banco.
func (a *App) health(w http.ResponseWriter, r *http.Request) {
	if err := a.pool.Ping(r.Context()); err != nil {
		writeError(w, http.StatusServiceUnavailable, "database unreachable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// capitalPorNatureza: ranking de capital social por natureza jurídica.
// GET /stats/capital-por-natureza?limit=10
func (a *App) capitalPorNatureza(w http.ResponseWriter, r *http.Request) {
	limit := parseLimit(r, 10, 200)
	const q = `
		SELECT n.descricao AS natureza_juridica,
		       mv.empresas,
		       mv.capital_total
		FROM analytics.mv_capital_por_natureza mv
		LEFT JOIN analytics.dim_natureza_juridica n ON n.codigo = mv.natureza_juridica_cod
		ORDER BY mv.capital_total DESC NULLS LAST
		LIMIT $1`
	rows, err := a.queryRows(r.Context(), q, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": rows})
}

// statsEmpresas: contagem de estabelecimentos com filtros opcionais.
// GET /stats/empresas?uf=SP&cnae=6201501&situacao=2
func (a *App) statsEmpresas(w http.ResponseWriter, r *http.Request) {
	q := `SELECT count(*) AS total FROM analytics.estabelecimento WHERE 1=1`
	var args []any
	if uf := r.URL.Query().Get("uf"); uf != "" {
		args = append(args, uf)
		q += " AND uf = $" + strconv.Itoa(len(args))
	}
	if cnae := r.URL.Query().Get("cnae"); cnae != "" {
		if v, err := strconv.Atoi(cnae); err == nil {
			args = append(args, v)
			q += " AND cnae_fiscal_principal = $" + strconv.Itoa(len(args))
		}
	}
	if sit := r.URL.Query().Get("situacao"); sit != "" {
		if v, err := strconv.Atoi(sit); err == nil {
			args = append(args, v)
			q += " AND situacao_cadastral = $" + strconv.Itoa(len(args))
		}
	}
	rows, err := a.queryRows(r.Context(), q, args...)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, rows[0])
}

// statsRegime: distribuição de empresas por forma de tributação.
// GET /stats/regime?ano=2024  (ano é opcional)
func (a *App) statsRegime(w http.ResponseWriter, r *http.Request) {
	q := `SELECT forma_de_tributacao,
	             count(*) AS registros,
	             count(DISTINCT cnpj_basico) AS empresas_distintas
	      FROM analytics.regime_tributario WHERE 1=1`
	var args []any
	if ano := r.URL.Query().Get("ano"); ano != "" {
		if v, err := strconv.Atoi(ano); err == nil {
			args = append(args, v)
			q += " AND ano = $" + strconv.Itoa(len(args))
		}
	}
	q += " GROUP BY forma_de_tributacao ORDER BY registros DESC"
	rows, err := a.queryRows(r.Context(), q, args...)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": rows})
}

// empresaDetalhe atende GET /empresas/{cnpj} e mostra SEMPRE a visão completa
// (empresa + estabelecimentos + QSA + Simples), aceite-se o CNPJ básico (8 díg.)
// ou o CNPJ completo (14 díg.). Com 14 dígitos, deriva o básico e marca a filial
// consultada (`consultado: true` + `cnpj_consultado` no topo).
func (a *App) empresaDetalhe(w http.ResponseWriter, r *http.Request) {
	cnpj := r.PathValue("cnpj")
	var basico, highlight string
	switch len(cnpj) {
	case 8:
		basico = cnpj
	case 14:
		basico, highlight = cnpj[:8], cnpj
	default:
		writeError(w, http.StatusBadRequest, "cnpj deve ter 8 (básico) ou 14 dígitos")
		return
	}
	a.empresaPorBasico(w, r, basico, highlight)
}

// empresaPorBasico monta a visão completa da empresa. `highlight` (CNPJ de 14
// díg. ou vazio) marca, quando informado, qual estabelecimento foi consultado.
func (a *App) empresaPorBasico(w http.ResponseWriter, r *http.Request, basico, highlight string) {
	ctx := r.Context()

	empresa, err := a.queryRows(ctx, `
		SELECT e.cnpj_basico, e.razao_social, n.descricao AS natureza_juridica,
		       e.capital_social, p.descricao AS porte
		FROM analytics.empresa e
		LEFT JOIN analytics.dim_natureza_juridica n ON n.codigo = e.natureza_juridica_cod
		LEFT JOIN analytics.dim_porte p ON p.codigo = e.porte_cod
		WHERE e.cnpj_basico = $1`, basico)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if len(empresa) == 0 {
		writeError(w, http.StatusNotFound, "empresa não encontrada")
		return
	}

	// Estabelecimentos com endereço completo; `consultado` marca o CNPJ pedido.
	estabs, err := a.queryRows(ctx, `
		SELECT est.cnpj, (est.cnpj = $2) AS consultado,
		       mf.descricao AS matriz_filial, est.nome_fantasia,
		       sit.descricao AS situacao, est.data_situacao_cadastral,
		       c.descricao AS cnae_principal, est.data_inicio_atividade,
		       est.tipo_logradouro, est.logradouro, est.numero, est.complemento,
		       est.bairro, est.cep, m.nome AS municipio, est.uf,
		       est.ddd_telefone_1, est.email
		FROM analytics.estabelecimento est
		LEFT JOIN analytics.dim_municipio m ON m.codigo = est.municipio_cod
		LEFT JOIN analytics.dim_situacao_cadastral sit ON sit.codigo = est.situacao_cadastral
		LEFT JOIN analytics.dim_matriz_filial mf ON mf.codigo = est.matriz_filial
		LEFT JOIN analytics.dim_cnae c ON c.codigo = est.cnae_fiscal_principal
		WHERE est.cnpj_basico = $1
		ORDER BY est.matriz_filial, est.cnpj`, basico, highlight)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	socios, err := a.queryRows(ctx, `
		SELECT s.nome_socio, s.cnpj_cpf_socio, q.descricao AS qualificacao,
		       s.data_entrada_sociedade
		FROM analytics.socio s
		LEFT JOIN analytics.dim_qualificacao q ON q.codigo = s.qualificacao_socio_cod
		WHERE s.cnpj_basico = $1`, basico)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	simples, err := a.queryRows(ctx, `
		SELECT opcao_simples, data_opcao_simples, data_exclusao_simples,
		       opcao_mei, data_opcao_mei, data_exclusao_mei
		FROM analytics.simples
		WHERE cnpj_basico = $1`, basico)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Regime tributário: 1:N por (cnpj, ano). Grão = CNPJ completo (a ordem
	// varia), por isso busca-se por cnpj_basico e devolve-se a lista por filial/ano.
	regime, err := a.queryRows(ctx, `
		SELECT cnpj, ano, forma_de_tributacao, qtd_escrituracoes, cnpj_da_scp
		FROM analytics.regime_tributario
		WHERE cnpj_basico = $1
		ORDER BY ano DESC, cnpj`, basico)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	resp := empresa[0]
	resp["estabelecimentos"] = estabs
	resp["qsa"] = socios
	resp["regime_tributario"] = regime
	if len(simples) > 0 {
		resp["simples"] = simples[0]
	} else {
		resp["simples"] = nil
	}
	if highlight != "" {
		resp["cnpj_consultado"] = highlight
	}
	writeJSON(w, http.StatusOK, resp)
}

// filialDetalhe: dados SÓ daquela filial pelo CNPJ completo (14 dígitos).
// GET /filial/{cnpj}?uf=SP — passar a UF habilita partition pruning (mais rápido).
func (a *App) filialDetalhe(w http.ResponseWriter, r *http.Request) {
	cnpj := r.PathValue("cnpj")
	if len(cnpj) != 14 {
		writeError(w, http.StatusBadRequest, "cnpj deve ter 14 dígitos")
		return
	}
	q := `
		SELECT est.cnpj, est.cnpj_basico, e.razao_social, est.nome_fantasia,
		       mf.descricao AS matriz_filial, sit.descricao AS situacao,
		       est.data_situacao_cadastral, est.data_inicio_atividade,
		       c.descricao AS cnae_principal,
		       est.tipo_logradouro, est.logradouro, est.numero, est.complemento,
		       est.bairro, est.cep, m.nome AS municipio, est.uf,
		       est.ddd_telefone_1, est.email,
		       e.capital_social, n.descricao AS natureza_juridica
		FROM analytics.estabelecimento est
		LEFT JOIN analytics.empresa e ON e.cnpj_basico = est.cnpj_basico
		LEFT JOIN analytics.dim_municipio m ON m.codigo = est.municipio_cod
		LEFT JOIN analytics.dim_situacao_cadastral sit ON sit.codigo = est.situacao_cadastral
		LEFT JOIN analytics.dim_matriz_filial mf ON mf.codigo = est.matriz_filial
		LEFT JOIN analytics.dim_cnae c ON c.codigo = est.cnae_fiscal_principal
		LEFT JOIN analytics.dim_natureza_juridica n ON n.codigo = e.natureza_juridica_cod
		WHERE est.cnpj = $1`
	args := []any{cnpj}
	if uf := r.URL.Query().Get("uf"); uf != "" {
		args = append(args, uf)
		q += " AND est.uf = $2" // partition pruning: varre só a partição da UF
	}
	rows, err := a.queryRows(r.Context(), q, args...)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if len(rows) == 0 {
		writeError(w, http.StatusNotFound, "filial não encontrada")
		return
	}
	writeJSON(w, http.StatusOK, rows[0])
}

// socios: rede societária — empresas vinculadas a um documento de sócio.
// GET /socios?doc=***846761**
func (a *App) socios(w http.ResponseWriter, r *http.Request) {
	doc := r.URL.Query().Get("doc")
	if doc == "" {
		writeError(w, http.StatusBadRequest, "parâmetro 'doc' obrigatório")
		return
	}
	limit := parseLimit(r, 50, 500)
	rows, err := a.queryRows(r.Context(), `
		SELECT s.cnpj_basico, e.razao_social, s.nome_socio,
		       q.descricao AS qualificacao
		FROM analytics.socio s
		LEFT JOIN analytics.empresa e ON e.cnpj_basico = s.cnpj_basico
		LEFT JOIN analytics.dim_qualificacao q ON q.codigo = s.qualificacao_socio_cod
		WHERE s.cnpj_cpf_socio = $1
		LIMIT $2`, doc, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": rows})
}

// parseLimit lê ?limit= com default e teto.
func parseLimit(r *http.Request, def, max int) int {
	v, err := strconv.Atoi(r.URL.Query().Get("limit"))
	if err != nil || v <= 0 {
		return def
	}
	if v > max {
		return max
	}
	return v
}
