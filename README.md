# cnpj-analytics

API HTTP de **analytics** sobre os dados abertos de CNPJ da Receita Federal, construída
sobre o schema relacional/dimensional `analytics` (derivado do projeto
[minha-receita](../minha-receita)). Enquanto o `minha-receita` serve lookup por CNPJ a
partir de um documento `jsonb`, este projeto é otimizado para **agregações, filtros livres
e rede societária** — coisas inviáveis no modelo jsonb.

## Stack

- Go 1.23 + `net/http` (roteamento method+path do Go 1.22) + `pgx/v5`/`pgxpool`
- PostgreSQL 18 (schema `analytics`)

## Estrutura

```
cmd/api/          entrypoint do servidor
internal/db/      pool de conexão pgx
internal/api/     servidor, rotas e handlers
analytics/        DDL + ETL do schema (SQL puro + load.sh)
```

## Como rodar

```bash
cp .env.example .env
docker compose up -d postgres

# 1) cria o schema e carrega os dados (zips da Receita em ./data)
bash analytics/load.sh                   # carga COMPLETA (leva horas)
SAMPLE=20000 bash analytics/load.sh      # amostra COERENTE de ~20k estab. (rápido)

# 1b) regime tributário — fonte SEPARADA; carga incremental sem tocar no resto
DB=cnpj_full bash analytics/load_regime.sh

# 2) sobe a API
go run ./cmd/api          # ou: docker compose up --build api
```

> Os zips da Receita (`Empresas*.zip`, `Estabelecimentos*.zip`, …) devem estar em `./data`
> (ou em `../minha-receita/data` — o `load.sh` detecta). A carga completa leva horas.
>
> Os arquivos de **regime tributário** (`entidades-*.zip`) ficam num share Nextcloud
> **separado** da Receita (token `MPPfFit7g7zdA8C`) e geram a tabela
> `analytics.regime_tributario` (~10,6 mi linhas, anos 2016–2024). Como baixar, formato
> e detalhes: [`analytics/fontes-dados.md`](analytics/fontes-dados.md). O `load.sh`
> completo já os inclui se estiverem no `DATA_DIR`; senão, use `load_regime.sh` depois.

### Variáveis do `load.sh`

| Var | Default | Efeito |
|---|---|---|
| `SAMPLE` | `0` (completo) | Se `>0`, gera **amostra coerente** ancorada em N estabelecimentos: carrega só as empresas/sócios/simples cujo básico aparece neles, garantindo joins ponta-a-ponta. |
| `DB` | `cnpj` | Banco de destino. Ex.: `DB=cnpj_full bash analytics/load.sh` carrega num banco separado sem tocar na amostra. |
| `DATA_DIR` | `./data` | Pasta dos zips (cai para `../minha-receita/data` se necessário). |

Veja [`TESTING.md`](TESTING.md) para uma bateria de `curl` cobrindo todos os endpoints.

## Endpoints

| Método | Rota | Descrição |
|---|---|---|
| GET | `/healthz` | Liveness + ping no banco |
| GET | `/stats/capital-por-natureza?limit=10` | Ranking de capital social por natureza jurídica (via materialized view) |
| GET | `/stats/empresas?uf=SP&cnae=6201501&situacao=2` | Contagem de estabelecimentos com filtros opcionais |
| GET | `/stats/regime?ano=2024` | Distribuição de empresas por forma de tributação (lucro real/presumido/arbitrado/imunes-isentas). `ano` opcional |
| GET | `/empresas/{cnpj}` | Visão completa: empresa + estabelecimentos (com endereço) + QSA + Simples/MEI + **regime tributário** (lista por filial/ano). Aceita **8 ou 14 dígitos** — com 14, marca a filial consultada (`consultado: true` + `cnpj_consultado`) |
| GET | `/filial/{cnpj}?uf=SP` | Dados **só daquela filial** (14 díg.) + empresa-mãe. `uf` é opcional mas recomendado: habilita *partition pruning* (varre 1 partição em vez de 27) |
| GET | `/socios?doc=***509360**&limit=50` | Rede societária: empresas vinculadas a um documento de sócio |

> A rota `/empresas/{cnpj}` retorna **sempre a visão completa da empresa** (todas as
> filiais, QSA e Simples), tanto faz colar o CNPJ básico (8 díg.) ou o completo
> (14 díg.). Com 14 dígitos, a filial correspondente vem com `consultado: true` e o
> CNPJ pedido aparece em `cnpj_consultado`. Qualquer outro tamanho retorna `400`.

### Parâmetros (query string)

| Rota | Parâmetro | Tipo | Obrigatório | Default | Observação |
|---|---|---|---|---|---|
| `/stats/empresas` | `uf` | texto (2 letras) | não | — | filtra por UF, ex. `SP` |
| `/stats/empresas` | `cnae` | inteiro | não | — | CNAE fiscal principal, ex. `6201501` |
| `/stats/empresas` | `situacao` | inteiro | não | — | situação cadastral: `2`=ativa, `8`=baixada, `3`=suspensa, `4`=inapta, `1`=nula |
| `/stats/capital-por-natureza` | `limit` | inteiro | não | `10` | teto `200` |
| `/stats/regime` | `ano` | inteiro | não | — | filtra o ano-base, ex. `2024` (dados 2016–2024) |
| `/filial/{cnpj}` | `uf` | texto (2 letras) | não | — | UF da filial; habilita *partition pruning* (consulta mais rápida) |
| `/socios` | `doc` | texto | **sim** | — | documento do sócio (mascarado), ex. `***509360**` |
| `/socios` | `limit` | inteiro | não | `50` | teto `500` |

Os filtros de `/stats/empresas` são **combináveis** (AND). Valores não numéricos em
`cnae`/`situacao` são ignorados; `limit` inválido cai no default.

### Exemplos

```bash
curl 'http://localhost:8001/stats/empresas?uf=DF&situacao=2'
curl 'http://localhost:8001/stats/capital-por-natureza?limit=5'
curl 'http://localhost:8001/empresas/52809343'         # 8 díg.: empresa + filiais
curl 'http://localhost:8001/empresas/52809343002572'   # 14 díg.: uma filial
curl 'http://localhost:8001/socios?doc=***509360**'
```
