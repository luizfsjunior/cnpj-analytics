# Testes manuais da API (cnpj-analytics)

Bateria de `curl` cobrindo todos os endpoints, com **valores reais** da base
completa carregada no banco `cnpj_full` (porta 5435). A API roda em
`http://localhost:8503`.

> **Proxy corporativo:** todos os comandos usam `--noproxy '*'` para evitar o `407`
> em `localhost`.
> **PowerShell:** `curl` é alias de `Invoke-WebRequest`; use **`curl.exe`** com a
> mesma sintaxe.
> **JSON formatado:** acrescente `| python -m json.tool` (ou `| jq`) ao final.

## Valores reais usados nos exemplos

| Item | Valor | Observação |
|---|---|---|
| Empresa com 4 filiais | `52809343` | HBC MOBILITY LTDA |
| Empresa com QSA | `07594978` | sócios preenchidos |
| Empresa MEI | `61979497` | `simples.opcao_mei = true` |
| Empresa Simples | `01610972` | `simples.opcao_simples = true` |
| CNPJ completo (14 díg.) | `52809343002572` | filial da HBC em Itapeví/SP |
| Documento de sócio | `***509360**` | já mascarado (PII) |
| Filtro setor + UF | `cnae=8219999`, `uf=SP` | CNAE ativo mais comum |

---

## 1. Health check

```bash
curl --noproxy '*' http://localhost:8503/healthz
```
Esperado: `{"status":"ok"}` (200). Se o banco cair → 503.

## 2. Empresa por CNPJ básico (8 dígitos)

Retorna empresa + **estabelecimentos** + **qsa** + **simples**.

```bash
# empresa com várias filiais
curl --noproxy '*' http://localhost:8503/empresas/52809343

# empresa com quadro societário (qsa preenchido)
curl --noproxy '*' http://localhost:8503/empresas/07594978

# empresa MEI (veja o bloco "simples": opcao_mei=true)
curl --noproxy '*' http://localhost:8503/empresas/61979497

# empresa optante do Simples (opcao_simples=true)
curl --noproxy '*' http://localhost:8503/empresas/01610972
```

## 3. CNPJ completo (14 dígitos)

Mesma rota, **mesma visão completa** — útil pra quem cola o CNPJ inteiro. A filial
correspondente vem com `consultado: true` e o topo traz `cnpj_consultado`.

```bash
curl --noproxy '*' http://localhost:8503/empresas/52809343002572
```

## 3b. Filial isolada (`/filial/{14díg}`)

Retorna **só aquela filial** + empresa-mãe. Passar `uf` faz partition pruning (mais rápido).

```bash
curl --noproxy '*' http://localhost:8503/filial/52809343002572
curl --noproxy '*' 'http://localhost:8503/filial/52809343002572?uf=SP'   # recomendado
```

## 4. Validação de tamanho (erros esperados)

```bash
# 10 dígitos -> 400
curl --noproxy '*' http://localhost:8503/empresas/1234567890

# básico inexistente (8 díg.) -> 404
curl --noproxy '*' http://localhost:8503/empresas/00000000
```

## 5. Estatística: contagem de estabelecimentos (filtros opcionais)

```bash
# sem filtro (total da base)
curl --noproxy '*' 'http://localhost:8503/stats/empresas'

# por UF
curl --noproxy '*' 'http://localhost:8503/stats/empresas?uf=SP'

# só ativas (situacao=2)
curl --noproxy '*' 'http://localhost:8503/stats/empresas?uf=SP&situacao=2'

# por CNAE
curl --noproxy '*' 'http://localhost:8503/stats/empresas?cnae=8219999'

# combinado: setor + UF + ativas
curl --noproxy '*' 'http://localhost:8503/stats/empresas?cnae=8219999&uf=SP&situacao=2'

# por município — o código é o do IBGE (7 díg.), não o da Receita
curl --noproxy '*' 'http://localhost:8503/stats/empresas?municipio_ibge=3550308'          # São Paulo/SP
curl --noproxy '*' 'http://localhost:8503/stats/empresas?municipio_ibge=3550308&uf=SP'    # + partition pruning
```

> O `municipio_ibge` é traduzido para o código da Receita via `dim_municipio`. Se o
> de-para ainda não tiver sido aplicado (ver `IBGE_ONLY=1` no README), a subquery não
> acha o município e a contagem volta **0** — sintoma clássico de `codigo_ibge` nulo.
> `/empresas/{cnpj}` e `/filial/{cnpj}` trazem o mesmo código no campo
> `codigo_municipio_ibge` de cada estabelecimento.

## 6. Estatística: capital social por natureza jurídica

```bash
curl --noproxy '*' 'http://localhost:8503/stats/capital-por-natureza?limit=5'
curl --noproxy '*' 'http://localhost:8503/stats/capital-por-natureza?limit=20'
```

## 6b. Regime tributário (distribuição por forma)

```bash
# distribuição geral (todos os anos)
curl --noproxy '*' 'http://localhost:8503/stats/regime'

# só o ano-base 2024
curl --noproxy '*' 'http://localhost:8503/stats/regime?ano=2024'
```

> O regime de cada empresa também aparece embutido no `/empresas/{cnpj}`, no
> campo `regime_tributario` (lista por filial/ano). Note que a fonte cobre
> sobretudo empresas de lucro real/presumido/arbitrado e imunes/isentas — MEI e
> Simples puros geralmente não constam, então o array pode vir vazio.

## 7. Rede societária por documento de sócio

```bash
# empresas vinculadas a um documento (CPF mascarado)
curl --noproxy '*' 'http://localhost:8503/socios?doc=***509360**'
curl --noproxy '*' 'http://localhost:8503/socios?doc=***509360**&limit=10'

# sem o parâmetro doc -> 400
curl --noproxy '*' 'http://localhost:8503/socios'
```

---

## Smoke test rápido (roda tudo de uma vez)

```bash
BASE='http://localhost:8503'
for path in \
  '/healthz' \
  '/empresas/52809343' \
  '/empresas/07594978' \
  '/empresas/61979497' \
  '/empresas/52809343002572' \
  '/stats/empresas?uf=SP&situacao=2' \
  '/stats/capital-por-natureza?limit=5' \
  '/stats/regime?ano=2024' \
  '/socios?doc=***509360**'
do
  printf '\n### GET %s\n' "$path"
  curl -s --noproxy '*' "$BASE$path" | head -c 400
  echo
done
```

---

## Inspeção direta no banco (DBeaver/DataGrip ou psql)

Conexão: `jdbc:postgresql://localhost:5435/cnpj_full` (user/pass `cnpj`/`cnpj`),
schema **`analytics`**. O banco `cnpj` (default do compose) é só a amostra/vazio;
a base completa fica em **`cnpj_full`**, que é o banco servido pela API.

```bash
# contagens
docker compose exec postgres-cnpj-rfb psql -U cnpj -d cnpj_full -c "
SELECT 'empresa' t, count(*) FROM analytics.empresa
UNION ALL SELECT 'estabelecimento', count(*) FROM analytics.estabelecimento
UNION ALL SELECT 'socio', count(*) FROM analytics.socio
UNION ALL SELECT 'simples', count(*) FROM analytics.simples;"

# partition pruning: só deve varrer estabelecimento_sp
docker compose exec postgres-cnpj-rfb psql -U cnpj -d cnpj_full -c "
EXPLAIN SELECT count(*) FROM analytics.estabelecimento WHERE uf='SP';"

# de-para IBGE: esperado 5572 municípios, 5571 com código IBGE, 0 sem UF
# (o único sem código é o 'EXTERIOR', SIAFI 9707)
docker compose exec postgres-cnpj-rfb psql -U cnpj -d cnpj_full -c "
SELECT count(*) AS municipios,
       count(codigo_ibge) AS com_ibge,
       count(*) FILTER (WHERE uf IS NULL) AS sem_uf
FROM analytics.dim_municipio;"

# âncoras do de-para
docker compose exec postgres-cnpj-rfb psql -U cnpj -d cnpj_full -c "
SELECT codigo, nome, codigo_ibge, uf FROM analytics.dim_municipio
WHERE codigo IN (7107, 6001, 1182, 9707) ORDER BY codigo;"
```

Esperado nas âncoras:

| codigo (SIAFI) | nome | codigo_ibge | uf |
|---|---|---|---|
| 1182 | BOA ESPERANCA DO NORTE | 5101837 | MT |
| 6001 | RIO DE JANEIRO | 3304557 | RJ |
| 7107 | SAO PAULO | 3550308 | SP |
| 9707 | EXTERIOR | *(null)* | EX |
