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

# 2) sobe a API
go run ./cmd/api          # ou: docker compose up --build api
```

> Os zips da Receita (`Empresas*.zip`, `Estabelecimentos*.zip`, …) devem estar em `./data`
> (ou em `../minha-receita/data` — o `load.sh` detecta). A carga completa leva horas.

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
| GET | `/empresas/{cnpj}` | **8 dígitos** → empresa + estabelecimentos + QSA + Simples/MEI; **14 dígitos** → a filial específica (endereço, contato, empresa-mãe) |
| GET | `/socios?doc=***509360**&limit=50` | Rede societária: empresas vinculadas a um documento de sócio |

> A rota `/empresas/{cnpj}` ramifica pelo tamanho do CNPJ: 8 dígitos consulta o grão
> **empresa** (básico, com todas as filiais), 14 dígitos consulta o grão
> **estabelecimento** (uma filial). Qualquer outro tamanho retorna `400`.

### Exemplos

```bash
curl 'http://localhost:8001/stats/empresas?uf=DF&situacao=2'
curl 'http://localhost:8001/stats/capital-por-natureza?limit=5'
curl 'http://localhost:8001/empresas/52809343'         # 8 díg.: empresa + filiais
curl 'http://localhost:8001/empresas/52809343002572'   # 14 díg.: uma filial
curl 'http://localhost:8001/socios?doc=***509360**'
```
