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
REGIME_ONLY=1 DB=cnpj_full bash analytics/load.sh

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
> completo já os inclui se estiverem no `DATA_DIR`; senão, use `REGIME_ONLY=1 bash
> analytics/load.sh` depois (carga incremental só do regime).

### Variáveis do `load.sh`

| Var | Default | Efeito |
|---|---|---|
| `SAMPLE` | `0` (completo) | Se `>0`, gera **amostra coerente** ancorada em N estabelecimentos: carrega só as empresas/sócios/simples cujo básico aparece neles, garantindo joins ponta-a-ponta. |
| `DB` | `cnpj` | Banco de destino. Ex.: `DB=cnpj_full bash analytics/load.sh` carrega num banco separado sem tocar na amostra. |
| `DATA_DIR` | `./data` | Pasta dos zips (cai para `../minha-receita/data` se necessário). |
| `TUNE` | `1` | Aplica tuning de carga (reload-only). `TUNE=0` desliga. **`shared_buffers` exige restart** — defina-o antes (ver [`tuning-carga.md`](analytics/tuning-carga.md)). |
| `TUNE_RAM_GB` | `6` | Orçamento de RAM para o tuning. Todos os knobs (`maintenance_work_mem`, `work_mem`, `max_wal_size`) são calculados proporcionalmente. Pico durante índices ≈ 60% deste valor (~3.6 GB). |
| `KEEP_STAGING` | `0` | Por padrão dropa o schema `staging` ao terminar (libera ~27GB na carga completa). `KEEP_STAGING=1` preserva para debug. |

Todas podem ser definidas no `.env` da raiz (o `load.sh` o carrega automaticamente)
ou passadas na linha de comando — a CLI tem prioridade sobre o `.env`.

Veja [`TESTING.md`](TESTING.md) para uma bateria de `curl` cobrindo todos os endpoints.

## Atualização automática (watcher)

[`watcher/watcher.py`](watcher/watcher.py) verifica o share da Receita a cada
`CHECK_INTERVAL_H` horas (PROPFIND leve) e, ao detectar um mês novo, dispara o
`load.sh` — mas só após `LOAD_AFTER_HOUR` (default 22h), para não pesar no horário
comercial. O mesmo código roda no dev (Windows+WSL) e num servidor Linux nativo:
`to_wsl_path` só transforma caminhos `C:/...`, então paths POSIX passam intactos.

| Var | Default (compose) | Efeito |
|---|---|---|
| `CNPJ_DB` | `cnpj_full` | Banco de destino passado ao `load.sh` (criado se não existir). |
| `CNPJ_DATA_DIR` | `/data` | `DATA_DIR` do `load.sh` (onde estão os zips; é um volume). |
| `CHECK_INTERVAL_H` | `24` | Intervalo entre verificações do share. |
| `LOAD_AFTER_HOUR` | `22` | Hora mínima (0–23) para iniciar a carga. |
| `TUNE_RAM_GB` | `6` | Orçamento de RAM da carga (ver tabela do `load.sh`). |

### Rodar com Docker (recomendado)

O watcher tem seu próprio serviço no `docker-compose.yml`. Ele fala com o postgres
**direto por TCP** (`PGHOST=postgres`), então **não precisa do socket do Docker** —
só do cliente `psql` (já na imagem). Aponte o volume `/data` para a pasta dos zips:

```bash
# edite o mapeamento ./data:/data no compose se os zips ficam em outro lugar
docker compose up -d postgres watcher
docker compose logs -f watcher
```

O estado (último mês carregado) persiste no volume `watcher_state`. Na primeira
subida, se houver mês novo no share, a carga dispara após `LOAD_AFTER_HOUR`.

### Rodar no host (alternativa, sem container)

Se preferir rodar fora de container, o `load.sh` cai automaticamente para
`docker compose exec postgres` quando `PGHOST` **não** está setado:

```bash
sudo apt install -y unzip ripgrep            # rg + unzip no PATH; docker já instalado
python3 -m venv watcher/.venv
watcher/.venv/bin/pip install -r watcher/requirements.txt
docker compose up -d postgres
watcher/.venv/bin/python watcher/watcher.py            # loop (ou --check p/ uma vez)
```

Como serviço systemd (restart automático, logs no journal), use o unit pronto em
[`watcher/cnpj-watcher.service`](watcher/cnpj-watcher.service) — ajuste `User=`,
`WorkingDirectory=` e o caminho do venv, depois:

```bash
sudo cp watcher/cnpj-watcher.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now cnpj-watcher
journalctl -u cnpj-watcher -f
```

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
