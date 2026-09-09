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
watcher/          watcher.py — baixa os zips do mês e dispara a carga
.github/workflows/ deploy.yml — CI/CD no runner self-hosted (ver "Deploy")
```

## Como rodar

```bash
cp .env.example .env
docker compose up -d postgres-cnpj-rfb

# 1) cria o schema e carrega os dados (zips da Receita em ./data)
bash analytics/load.sh                   # carga COMPLETA (leva horas)
SAMPLE=20000 bash analytics/load.sh      # amostra COERENTE de ~20k estab. (rápido)

# 1b) regime tributário — fonte SEPARADA; carga incremental sem tocar no resto
REGIME_ONLY=1 DB=cnpj_full bash analytics/load.sh

# 1c) de-para de municípios (código IBGE) — incremental, não toca no resto
IBGE_ONLY=1 DB=cnpj_full bash analytics/load.sh

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

> **Código de município = IBGE.** O `Municipios.csv` da Receita só traz o código do
> **SIAFI** (4 díg.). O `load.sh` baixa o de-para do TABMUN (Tesouro Nacional) e a
> lista de municípios da API do IBGE, e preenche `dim_municipio.codigo_ibge` e
> `dim_municipio.uf` (`analytics/ibge_transform.sql`). Na carga completa isso roda
> automaticamente depois do transform; numa base **já carregada**, use
> `IBGE_ONLY=1 bash analytics/load.sh` — assim não é preciso redisparar a carga
> mensal só para atualizar o de-para. As fontes ficam cacheadas no `DATA_DIR`
> (`tabmun.csv`, `ibge_municipios.json`) e são revalidadas a cada
> `IBGE_CACHE_MAX_DAYS` dias, então uma queda de rede não impede a carga. Detalhes e
> regras de casamento: [`analytics/fontes-dados.md`](analytics/fontes-dados.md).

### Variáveis do `load.sh`

| Var | Default | Efeito |
|---|---|---|
| `SAMPLE` | `0` (completo) | Se `>0`, gera **amostra coerente** ancorada em N estabelecimentos: carrega só as empresas/sócios/simples cujo básico aparece neles, garantindo joins ponta-a-ponta. |
| `DB` | `cnpj` | Banco de destino. Ex.: `DB=cnpj_full bash analytics/load.sh` carrega num banco separado sem tocar na amostra. |
| `DATA_DIR` | `./data` | Pasta dos zips (cai para `../minha-receita/data` se necessário). |
| `TUNE` | `1` | Aplica tuning de carga (reload-only). `TUNE=0` desliga. **`shared_buffers` exige restart** — defina-o antes (ver [`tuning-carga.md`](analytics/tuning-carga.md)). |
| `TUNE_RAM_GB` | `6` | Orçamento de RAM para o tuning. Todos os knobs (`maintenance_work_mem`, `work_mem`, `max_wal_size`) são calculados proporcionalmente. Pico durante índices ≈ 60% deste valor (~3.6 GB). |
| `IBGE_ONLY` | `0` | Se `1`, roda **só** o de-para IBGE (`dim_municipio.codigo_ibge`/`uf`) numa base já carregada e sai. Não aplica tuning, não recria schema, não recarrega zip nenhum. |
| `IBGE_REFRESH` | `0` | Se `1`, rebaixa `tabmun.csv`/`ibge_municipios.json` mesmo com cache válido. |
| `IBGE_CACHE_MAX_DAYS` | `25` | Idade máxima do cache das fontes do IBGE. Vencido, rebaixa; se o download falhar, segue com o cache. |
| `IBGE_CACHE_DIR` | `$DATA_DIR` | Onde ficam `tabmun.csv` e `ibge_municipios.json`. |
| `KEEP_STAGING` | `0` | Por padrão dropa o schema `staging` ao terminar (libera ~27GB na carga completa). `KEEP_STAGING=1` preserva para debug. |

> **Gotcha — `/dev/shm` do postgres:** o serviço `postgres-cnpj-rfb` no `docker-compose.yml` define
> `shm_size: "512m"`. O default do Docker (64MB) é pequeno demais para os *parallel workers*
> e faz a carga falhar ao criar as materialized views (passo [5/5]) com
> `could not resize shared memory segment ... No space left on device`. `shm_size` só é
> aplicado ao **criar** o container — após alterar, rode `docker compose up -d postgres`
> (um `restart` não pega).

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
**direto por TCP** (`PGHOST=postgres-cnpj-rfb`), então **não precisa do socket do Docker** —
só do cliente `psql` (já na imagem). Aponte o volume `/data` para a pasta dos zips:

```bash
# se os zips ficam fora do repo, aponte CNPJ_HOST_DATA_DIR no .env
docker compose up -d postgres-cnpj-rfb watcher-cnpj-rfb
docker compose logs -f watcher
```

O compose usa `${CNPJ_HOST_DATA_DIR:-./data}` nos dois bind mounts (PGDATA do
postgres e `/data` do watcher). Sem a variável, tudo fica em `./data` dentro do
repo — bom para a máquina local. No servidor ela aponta para fora da árvore de
deploy, para que o `rsync --delete` do CI/CD não encoste nos dados.

O estado (último mês carregado) persiste no volume `watcher_state`. Na primeira
subida, se houver mês novo no share, a carga dispara após `LOAD_AFTER_HOUR`.

### Rodar no host (alternativa, sem container)

Se preferir rodar fora de container, o `load.sh` cai automaticamente para
`docker compose exec postgres-cnpj-rfb` quando `PGHOST` **não** está setado (o
serviço vem de `PG_SERVICE`):

```bash
sudo apt install -y unzip ripgrep            # rg + unzip no PATH; docker já instalado
python3 -m venv watcher/.venv
watcher/.venv/bin/pip install -r watcher/requirements.txt
docker compose up -d postgres-cnpj-rfb
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

## Deploy (CI/CD)

`.github/workflows/deploy.yml` roda no **runner self-hosted** do `srv-controladoria`
(org `Porto-Seco-SDM`, `workFolder=/opt/applications`), disparado por push em
`master` ou manualmente (`workflow_dispatch`). Só existe um ambiente: **prod**.

Caminhos no servidor:

| Caminho | O quê |
|---|---|
| `/opt/applications/cnpj-analytics/cnpj-analytics` | checkout do runner (`GITHUB_WORKSPACE`) — descartável, o runner manda nele |
| `/opt/applications/cnpj-analytics/prod` | destino do `rsync --delete`; é daqui que o `docker compose` sobe |
| `/opt/applications/cnpj-analytics/data` | zips da Receita + PGDATA (~72 GB) — **fora** da árvore de deploy |

Etapas: build da imagem da API (não há testes Go; se não compilar, não sobe) →
`bash -n load.sh` + `py_compile watcher.py` → `rsync` → `docker compose up -d
--build --no-deps api-cnpj-rfb watcher-cnpj-rfb` → healthcheck em `/healthz`.

### Gotchas que o workflow existe para evitar

- **O postgres nunca é recriado.** O `up` de build cita só a API e o watcher e usa
  `--no-deps`. Recriar o container do banco por causa de uma mudança de compose
  significaria perder uma carga de horas.
- **O nome do projeto é fixo (`-p cnpj-analytics`).** O volume nomeado
  `cnpj-analytics_watcher_state` guarda o último mês carregado; com outro nome de
  projeto o Docker cria um volume vazio e o watcher dispara uma carga completa.
- **`.env` e `docker-compose.override.yml` são configuração do servidor**, não
  versionados (o override liga o container à rede `services-net`). Estão no
  `--exclude` do rsync, senão o `--delete` os apagaria a cada deploy.
- **`data/` também está no `--exclude`** — cinto e suspensório, já que
  `CNPJ_HOST_DATA_DIR` já a tira de dentro do diretório de deploy.

### Configuração do servidor (não versionada)

Dois arquivos vivem só no diretório de deploy e estão no `--exclude` do rsync:

- **`.env`** — além do `DATABASE_URL`, carrega `CNPJ_HOST_DATA_DIR` apontando para
  os dados fora da árvore de deploy.
- **`docker-compose.override.yml`** — liga os três serviços à rede externa
  `services-net`, por onde as outras stacks (Airflow, Protheus, controladoria)
  acessam este banco em `postgres-cnpj-rfb:5432`. Um modelo comentado está em
  `docker-compose.override.example.yml`.

> **Por que os serviços têm sufixo `-cnpj-rfb`.** O Compose publica o nome do
> serviço como alias de DNS em **toda** rede a que o container se liga. Na
> `services-net`, um serviço chamado `postgres` colidiria com o banco do Airflow,
> que também está lá — o DNS devolveria os dois IPs e as conexões cairiam no banco
> errado de forma intermitente. O sufixo no arquivo base resolve isso de uma vez;
> tentar resolver só com `aliases` no override não funciona, porque o alias com o
> nome do serviço continua sendo publicado.

### Migração (uma vez, do layout antigo)

Antes do primeiro deploy tudo morava em `/opt/applications/api-cnpj/cnpj-analytics`,
com os dados dentro e o compose modificado à mão. Origem e destino estão no mesmo
filesystem (`/dev/sda1`), então o `mv` é um rename instantâneo.

```bash
cd /opt/applications/api-cnpj/cnpj-analytics
docker compose -p cnpj-analytics down          # postgres para aqui, uma única vez

mkdir -p /opt/applications/cnpj-analytics/prod
mv data /opt/applications/cnpj-analytics/data

# configuração do host vai para o diretório de deploy
cp .env /opt/applications/cnpj-analytics/prod/.env
echo 'CNPJ_HOST_DATA_DIR=/opt/applications/cnpj-analytics/data'   >> /opt/applications/cnpj-analytics/prod/.env
# override novo (aliases, sem renomear serviços) — modelo no repo
cp docker-compose.override.example.yml    /opt/applications/cnpj-analytics/prod/docker-compose.override.yml

# o estado do watcher é volume nomeado pelo projeto: sobrevive intacto
docker volume inspect cnpj-analytics_watcher_state
```

Depois disso, um push em `master` no repo da org dispara o workflow e a stack sobe
do novo diretório. A pasta antiga fica como backup do clone git e pode ser
removida quando o deploy estiver validado.
