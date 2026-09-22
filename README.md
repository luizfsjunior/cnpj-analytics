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
  00_carga.sql      schema `carga`: auditoria do rejeito e casts totais
  01_schema.sql     tabelas, partições e parse_date
  02_staging.sql    staging UNLOGGED, espelho dos CSVs
  03_transform.sql  transform sequencial (referência de conteúdo)
  03_transform_v2.sql  transform em blocos paralelos (o que a carga usa)
  fase2_drop_indices.sql / fase4_indices.sql   o ciclo de índices
  recuperar_indices.sh  rede do trap: recria os índices numa falha
  spec-carga.md     o CONTRATO da carga (leia antes de mexer)
watcher/          watcher.py — BIBLIOTECA de download (retry contra o share da
                  Receita); o daemon que rodava em loop foi aposentado (D1,
                  22/09/2026) — ver "Orquestração no Airflow"
watcher/tests/    testes do watcher (pytest + responses, sem rede)
airflow/          a DAG que dispara a carga mensal (ver "Orquestração no Airflow")
  spec-dag-carga.md  o CONTRATO da DAG
  dags/cnpj_carga.py a DAG
  pools.json         o pool de 1 slot que impede carga dupla
  tests/             T1–T10 da spec da DAG
analytics/tests/  testes do load.sh (stubs de shell) e dos .sql
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

### Como a carga funciona (as seis fases)

O `load.sh` é o orquestrador; o trabalho de verdade está nos `.sql`. O contrato
de tudo isto — o que tem de ser verdade no fim, o que é proibido e como se prova
cada ponto — está em [`analytics/spec-carga.md`](analytics/spec-carga.md), e as
medições que o justificam em
[`analytics/redesenho-carga.md`](analytics/redesenho-carga.md).

| Fase | O que faz | Arquivos |
|---|---|---|
| **0 — pré-voo** | Confere o **layout** dos CSVs (número de colunas) antes de qualquer COPY, lê CPU/memória/disco e **deriva** `LOAD_JOBS`, `work_mem` e `maintenance_work_mem`. Host ocupado → degrada e segue. | `load.sh` |
| **1 — COPY** | `unzip -p \| tr -d '\000' \| \copy` direto para a staging, que é **`UNLOGGED`**. | `02_staging.sql` |
| **2 — drop de índices** | Salva a DDL dos 212 índices em `carga.indice_salvo` e os **dropa**. Mantém só a PK de `empresa`. | `fase2_drop_indices.sql` |
| **3 — transform** | staging → `analytics`, com sanitização nomeada e rejeito contado. Em **blocos paralelos** por faixa de `ctid`. | `03_transform_v2.sql` (ou `03_transform.sql`) |
| **4 — índices** | **Recria** os índices a partir do que a Fase 2 salvou, `IDX_JOBS` em paralelo. A duplicata do mês é descoberta pelo próprio `CREATE UNIQUE INDEX` ao falhar — sem varredura preventiva. | `fase4_indices.sql` |
| **5 — o resto** | IBGE, regime tributário, materialized views. | `ibge_transform.sql`, `regime_transform.sql`, `05_materialized_views.sql` |

**Quanto leva.** Carga completa medida na máquina de desenvolvimento, com o
orçamento do servidor (3 GB, 3 blocos): **3h38** num banco novo, e projetada em
**~2h50** na carga mensal, em que os índices já existem e a Fase 4 os reconstrói
em paralelo. Contra 20h+ do servidor hoje. Os números por fase e as ressalvas
(hardware diferente, o que transfere e o que não transfere) estão na seção 6 da
[`spec-carga.md`](analytics/spec-carga.md).

**Por que dropar os índices é a mudança que importa.** Das ~20 horas da carga
antiga, **16 estavam na manutenção de índice de uma tabela só**: 14 GB de índice
mantidos vivos durante o `INSERT`, através de um `shared_buffers` de 128 MB, com
o `INSERT` parado em `DataFileRead`. Construir no fim é ordens de magnitude mais
barato — a Fase 2 leva **1 segundo** e o ciclo inteiro de índices cabe em **32
minutos** na carga mensal (medido sobre a base completa; spec 6.4). Os blocos paralelos da Fase 3 valem 2,5× medidos, mas são a parte
**menor** do ganho — se um mês der problema com eles, `CARGA_TRANSFORM=sequencial`
volta ao caminho conhecido sem desfazer o resto.

**A janela perigosa, e a rede que existe por causa dela.** Entre as Fases 2 e 4 a
base fica **sem índice**. Se a carga morrer aí, a API responderia com seq scan em
73 milhões de linhas até alguém agir — e o watcher só retenta em 24 h. Por isso
o `trap EXIT` do `load.sh` chama
[`analytics/recuperar_indices.sh`](analytics/recuperar_indices.sh) antes de
propagar qualquer erro. O script é idempotente e pode ser rodado na mão:

```bash
DB=cnpj_full bash analytics/recuperar_indices.sh
```

**Nada derruba a carga por causa de uma célula.** Todo cast é *total*: um
`'20200231'` (data que não existe), um `'1.234,56'` no capital ou um `cnpj_ordem`
com 5 dígitos viram `NULL` mais um rejeito contado, em vez da exceção que antes
matava 20 horas de trabalho às 3 da manhã. A validação é por regex e faixa,
**nunca** por bloco `EXCEPTION` — que abriria uma subtransação por linha em 73
milhões de linhas.

### O schema `carga` — onde a carga registra o que fez

A API não enxerga este schema e nada em `analytics` depende dele; se alguém o
dropar, a carga continua correta e só se perde a auditoria. Ele existe porque
`analytics` tem de ficar **idêntico** ao que os `.sql` produzem (é o teste T1),
então não cabe nenhuma tabela de controle lá dentro.

| Tabela | O que guarda |
|---|---|
| `carga.rejeito` | Toda linha/célula descartada, com a **regra** que a pegou (S4, S5, S6, S9, S11, S12) e a linha bruta. |
| `carga.contador` | Um contador por regra e tabela, em toda carga — inclusive das regras que não descartam nada. |
| `carga.duplicata` | Chaves naturais repetidas no mês (a quarentena). |
| `carga.indice_salvo` | A DDL dos índices, salva pela Fase 2. É a **única** cópia. |
| `carga.resumo` | Uma linha por execução: início, fim, parâmetros derivados, recursos do host, pico de memória e `desfecho`. |

```sql
-- o que este mês descartou, por regra
SELECT * FROM carga.resumo_regras WHERE competencia = '2026-09';
-- e o que essas linhas eram
SELECT * FROM carga.rejeito WHERE competencia = '2026-09' LIMIT 50;
```

**Como a duplicata é descoberta sem custo.** A Fase 4 não varre a fonte para
decidir se cria o índice único: ela **tenta** criar. O `CREATE UNIQUE INDEX` já
percorre a tabela inteira para construir o índice, então a verificação de
unicidade sai de graça no mesmo passe. Só quando ele falha é que a varredura
acontece, para montar a quarentena. O desenho anterior varria antes, e isso
custou **51 minutos por carga** na medição de 16/09/2026 — para encontrar 23
linhas.

**`desfecho` tem três estados, e o terceiro é o que evita retrabalho.** Um mês em
que a Receita publique chave natural repetida termina em **`degradado`**, não em
`falha`: o índice afetado sai **não-único**, as chaves vão para
`carga.duplicata`, e a carga termina com sucesso. Não é motivo para recarregar —
a carga seguinte volta ao índice único sozinha se o mês vier limpo. (É o único
ponto em que a estrutura pode divergir do `01_schema.sql`, e é desvio conhecido e
temporário.)

### Gotchas da carga (economizam horas)

**Não edite o `load.sh` enquanto uma carga roda.** O bash não carrega o script
inteiro na memória — lê do disco conforme executa. Editar o arquivo em execução
desloca os offsets e o parser quebra no meio (`unexpected EOF while looking for
matching`), derrubando a carga depois de horas de COPY. Vale também no
servidor: a DAG do Airflow dispara a carga às 22h (agendamento
`schedule="0 22 * * *"` em `airflow/dags/cnpj_carga.py`) — mexer no arquivo
durante essa janela derruba a carga do mês. Edite uma cópia e troque depois.

**`rg` precisa ser o binário do ripgrep, não uma função/alias do shell.** O modo
`SAMPLE` usa `rg` para casar os CNPJs básicos nos zips de empresas/sócios;
funções de shell não passam para subprocessos. Desde a correção o script checa
as dependências antes de começar (`check_deps`) e aborta na hora em vez de
produzir uma amostra vazia reportando sucesso. A carga completa não usa `rg`.

**`Estabelecimentos0.zip` é ~6x maior que os outros.** Ele descomprime para
~6,5 GB contra ~1,0 GB de cada um dos nove seguintes, e sozinho responde por ~29
dos ~72 milhões de estabelecimentos. Na carga de referência levou 8m50s contra
~1m22s dos demais — proporcional ao tamanho, não anomalia. Isso importa ao
paralelizar: dividir os COPYs em 10 tarefas iguais deixaria uma 6x mais longa
que as outras, e o tempo total seria o dela. O COPY roda a ~12 MB/s de CSV
descomprimido, de forma estável.

### Variáveis do `load.sh`

| Var | Default | Efeito |
|---|---|---|
| `SAMPLE` | `0` (completo) | Se `>0`, gera **amostra coerente** ancorada em N estabelecimentos: carrega só as empresas/sócios/simples cujo básico aparece neles, garantindo joins ponta-a-ponta. |
| `DB` | `cnpj` | Banco de destino. Ex.: `DB=cnpj_full bash analytics/load.sh` carrega num banco separado sem tocar na amostra. |
| `DATA_DIR` | `./data` | Pasta dos zips (cai para `../minha-receita/data` se necessário). |
| `TUNE` | `1` | Aplica tuning de carga (reload-only). `TUNE=0` desliga. **`shared_buffers` exige restart** — defina-o antes (ver [`tuning-carga.md`](analytics/tuning-carga.md)). |
| `ORCAMENTO_RAM_MB` | `3072` | Teto de RAM da carga. **Tudo** (`work_mem`, `maintenance_work_mem`, `max_wal_size`) é derivado daqui na Fase 0. |
| `ORCAMENTO_VCPU` | `min(4, nproc)` | vCPU que a carga pode ocupar. Um core sempre fica de fora, para o `unzip`/`tr` e para quem mais dividir o host. |
| `LOAD_JOBS` | derivado (teto **3**) | Blocos simultâneos na Fase 3. Se o host estiver ocupado, a Fase 0 **degrada** este número e segue — nunca aborta. |
| `IDX_JOBS` | `= LOAD_JOBS` | Índices construídos ao mesmo tempo na Fase 4. |
| `CARGA_TRANSFORM` | `blocos` | `sequencial` volta ao `03_transform.sql` (uma tabela por vez). É a válvula de escape se os blocos derem problema num mês. |
| `REJEITO_LIMIAR_PCT` | `0.1` | Acima disto a carga **avisa** que rejeitou muito. Nunca aborta por isso. |
| `CONTAR_DUP_SOCIO` | `0` | Conta duplicatas idênticas em `socio`. **Desligado por padrão**: é um `GROUP BY` de 11 colunas sobre 27,8M linhas e não alimenta nenhuma decisão da carga — só o contador que confirma a hipótese do conjunto congelado (spec 5.2). |
| `COMPETENCIA` | mês corrente | Rótulo do mês em `carga.resumo`/`carga.rejeito`. |
| `TUNE_RAM_GB` | — | Nome antigo do orçamento, em GB. Se definido, vira `ORCAMENTO_RAM_MB`; continua valendo em `.env` já existentes. |
| `IBGE_ONLY` | `0` | Se `1`, roda **só** o de-para IBGE (`dim_municipio.codigo_ibge`/`uf`) numa base já carregada e sai. Não aplica tuning, não recria schema, não recarrega zip nenhum. |
| `IBGE_REFRESH` | `0` | Se `1`, rebaixa `tabmun.csv`/`ibge_municipios.json` mesmo com cache válido. |
| `IBGE_CACHE_MAX_DAYS` | `25` | Idade máxima do cache das fontes do IBGE. Vencido, rebaixa; se o download falhar, segue com o cache. |
| `IBGE_CACHE_DIR` | `$DATA_DIR` | Onde ficam `tabmun.csv` e `ibge_municipios.json`. |
| `KEEP_STAGING` | `0` | Por padrão dropa o schema `staging` ao terminar (libera ~27GB na carga completa). `KEEP_STAGING=1` preserva para debug. |
| `TIMING` | `1` | Imprime o tempo de cada fase da carga (COPY por zip, transform, índices…) e um resumo no fim — inclusive se a carga abortar no meio. `TIMING=0` volta à saída original. |
| `SQL_TIMING` | `1` | Prefixa `	iming on` nos arquivos SQL, então o psql imprime a duração de **cada statement** (é assim que se identifica qual índice do `04_indexes.sql` domina o tempo). Ignorado com `TIMING=0`. |

> **Gotcha — `/dev/shm` do postgres:** o serviço `postgres-cnpj-rfb` no `docker-compose.yml` define
> `shm_size: "512m"`. O default do Docker (64MB) é pequeno demais para os *parallel workers*
> e faz a carga falhar ao criar as materialized views (passo [6/6]) com
> `could not resize shared memory segment ... No space left on device`. `shm_size` só é
> aplicado ao **criar** o container — após alterar, rode `docker compose up -d postgres`
> (um `restart` não pega).

Todas podem ser definidas no `.env` da raiz (o `load.sh` o carrega automaticamente)
ou passadas na linha de comando — a CLI tem prioridade sobre o `.env`.

Veja [`TESTING.md`](TESTING.md) para uma bateria de `curl` cobrindo todos os endpoints.

## Atualização automática (watcher como biblioteca)

Até 22/09/2026, [`watcher/watcher.py`](watcher/watcher.py) rodava como **daemon**:
um serviço próprio no compose que verificava o share da Receita a cada
`CHECK_INTERVAL_H` horas e, ao detectar mês novo, disparava o `load.sh` sozinho.
Esse loop foi **aposentado** (decisão D1 da `spec-dag-carga.md`): quem dispara a
carga mensal agora é a DAG do Airflow — ver "Orquestração no Airflow" abaixo. As
variáveis `CHECK_INTERVAL_H` e `LOAD_AFTER_HOUR` morreram junto com o loop; o
serviço `watcher-cnpj-rfb` não existe mais no `docker-compose.yml`.

O arquivo **continua no repo e continua essencial**, agora só como **biblioteca**:
o retry contra o share da Receita (a parte cara e testada, ver abaixo) não é
reimplementado em lugar nenhum — a DAG chama `fetch_available_months` e
`download_month` de dentro da imagem `cnpj-carga`, que é o próprio
`watcher/Dockerfile`.

| Função exportada | Quem chama | Para quê |
|---|---|---|
| `fetch_available_months()` | task `listar_meses` da DAG | Lista os meses publicados no share (PROPFIND) |
| `download_month(mes)` | task `baixar_zips` da DAG | Baixa os zips do mês, com todo o retry abaixo |

> **Por que importar o módulo não pode ter efeito colateral.** O `import
> schedule` mora dentro de `main()` — é dependência só do loop do daemon, que a
> DAG não precisa instalar. O arquivo de log é **melhor esforço**,
> configurável por `CNPJ_WATCHER_LOG` (vazio desliga; o stdout continua
> sempre): com o repo montado somente leitura, abrir o `FileHandler` como
> efeito colateral do import derrubava o import inteiro com `OSError: [Errno
> 30] Read-only file system`.

### Por que o download tem retry (gotcha importante)

O share da Receita **derruba silenciosamente de 22% a 35% das conexões**: o TLS
completa, o servidor aceita o GET e nunca envia um byte — a requisição só morre no
read timeout. Medido contra o share real: 14 de 37 zips travaram assim numa única
passada, e a taxa oscila ao longo do dia. Não é banda nem arquivo específico; é
sorteio por conexão, e a requisição seguinte costuma ir bem em menos de 1s.

Antes de isso ser tratado, cada trava custava **24h**: o mês inteiro era descartado
e só retentado na verificação seguinte.

O tratamento vive em `_download_one` e `_propfind`:

- **7 tentativas por requisição**, esperando `RETRY_BACKOFF` = 0/2/5/15/30/30/60s
  antes de cada uma — 142s no pior caso, pago só quando falha.
- Só erro **transitório** é retentado (timeout, conexão, 5xx). Um 404/403 falha na
  hora, sem gastar backoff à toa.
- **2ª passada** no fim de `download_month`: os arquivos que esgotaram as tentativas
  ganham um ciclo completo novo antes de o mês ser dado como perdido.
- O **PROPFIND usa a mesma política**. Sem isso, uma listagem de 18 KB que trava
  derruba o ciclo inteiro mesmo com os 20 GB de zips já em disco.

Duas medições explicam as escolhas: as travas se comportam como **sorteio
independente** (retry após 0s teve a mesma taxa de sucesso do retry após 15s, 71%
vs 83% com n pequeno), então o que protege é a **quantidade** de tentativas e não
esperas longas; e um arquivo já foi observado falhando **4 vezes seguidas**, o que
descarta curvas curtas.

O resume (`.part` + header `Range`) trabalha junto: cada tentativa retoma de onde
parou, então uma trava no meio de um `Estabelecimentos0.zip` de 2 GB não custa os
2 GB de novo. Por isso `_download_attempt` relê o tamanho do `.part` **a cada
tentativa** — reaproveitar um valor antigo pediria `Range` do offset errado,
duplicando bytes e corrompendo o zip em silêncio.

### Testes do watcher

`pytest` + `responses` (HTTP mockado — nenhuma rede, roda em ~0,1s):

```bash
pip install -r watcher/requirements-dev.txt
python -m pytest watcher/tests -q
```

Cobrem retry e resume do download e dos PROPFIND: trava sem bytes, trava no meio do
stream (com asserção do `Range` da tentativa seguinte), 4xx sem retry, 5xx com
retry, servidor ignorando o `Range`, 416 com `.part` já completo e a 2ª passada.
As deps de teste ficam em `watcher/requirements-dev.txt` e **não** entram na imagem:
o `Dockerfile` copia só o `requirements.txt`.

### Testes do `load.sh` e dos `.sql`

`pytest`, em `analytics/tests/` — separados dos testes do watcher porque exercitam
shell e SQL, não Python:

```bash
python -m pytest analytics/tests -q          # tudo (~8 min, precisa do container)
python -m pytest analytics/tests -q -k "load_sh"      # só os que não precisam de banco
```

A maior parte deles guarda o **contrato da carga**, definido em
[`analytics/spec-carga.md`](analytics/spec-carga.md). Se você quebrar um, leia a
spec antes de "consertar o teste" — cada um corresponde a um requisito:

| arquivo | o que guarda |
|---|---|
| `test_t1_schema_invariante.py` | **R1**: a estrutura de `analytics` não muda. Inclui a prova de que dropar e recriar índices **pelo pai** reproduz os nomes das 28 partições. |
| `test_t2_equivalencia.py` | O conteúdo produzido bate com `golden_carga_atual.json`. Regravar o golden sem registrar exceção nomeada na spec é mudar o contrato em silêncio. |
| `test_t3_sanitizacao.py` | As regras S1–S12, uma a uma, com o caso que dispara e o que não. |
| `test_t4_a_t8_v2.py` | Rejeito contabilizado (T4), recuperação pelo trap (T5), orçamento (T6/T7/T12), quarentena de duplicata (T8), determinismo dos blocos (T10/T11), robustez a lixo (T13) e layout (T14). |
| `test_t9_amostra_coerente.py` | Carga em blocos × sequencial sobre dado REAL da Receita. **Pula** sem os zips; rode com `CNPJ_DATA_DIR=<pasta>`. Obrigatório antes de ir ao servidor. |
| `fixture_carga.py` | A fixture sintética: cada linha existe para disparar uma regra nomeada. |

* **`test_load_sh_dependencias.py`** e **`test_load_sh_tune_zero.py`** rodam o
  `load.sh` de verdade com um PATH em que `unzip`, `psql`, `curl` e `rg` são
  stubs. Nenhum byte da Receita é lido e nenhum banco é tocado: o que se observa
  é o comportamento do script (o que ele tolera, com que código de saída sai).
* **`test_regime_transform_idempotente.py`** precisa do container de pé
  (`docker compose up -d postgres-cnpj-rfb`); cria e dropa um banco descartável.
  Pula sozinho se o Docker não estiver disponível.

> **Os testes com stub não substituem o T9.** Três bugs que passaram por toda a
> suíte verde só apareceram com zips de verdade — entre eles um `psql` em
> background que consumia o stdin do loop de blocos e fazia a carga terminar
> **com sucesso e quatro tabelas vazias**. Está tudo registrado na seção 6 da
> spec.

**Gotcha de ambiente (Windows).** Os testes procuram o bash em caminho absoluto,
e não é capricho:

* `subprocess.run(["bash", ...])` **não** roda o Git Bash — o `CreateProcess`
  procura em `System32` antes do PATH, e lá mora o `bash.exe` **launcher do
  WSL**, que não herda o ambiente do processo pai. As variáveis do teste somem, o
  `load.sh` cai nos defaults (`DB=cnpj`, `docker compose exec` no container real,
  `DATA_DIR` no fallback `../minha-receita/data`) e o teste vira uma carga de
  verdade.
* Mesmo achando o Git Bash, `Git\bin\bash.exe` é um wrapper que antepõe
  `/usr/bin` ao PATH: aí o `unzip` real vence o stub. O certo é
  `Git\usr\bin\bash.exe`. Sobrescreva com `BASH_PARA_TESTES=<caminho>` se
  precisar.

O `conftest.py` verifica as duas coisas em tempo de execução e aborta com
mensagem explícita em vez de rodar torto.

### Como a carga é disparada hoje

**Não há mais daemon nem serviço systemd.** Até 22/09/2026 havia um serviço
`watcher-cnpj-rfb` no compose e um unit systemd
([`watcher/cnpj-watcher.service`](watcher/cnpj-watcher.service), retirado de uso —
o arquivo fica no repo só como referência histórica) que rodavam `watcher.py` em
loop. Os dois foram removidos com o cutover para a DAG do Airflow (D1); ver
"Orquestração no Airflow" logo abaixo.

Para disparar a carga hoje, dois caminhos:

- **Manual, direto** — a forma mais simples para desenvolvimento: `bash
  analytics/load.sh` (ver "Como rodar", no topo), ou `docker compose exec
  postgres-cnpj-rfb ...` se preferir contra o container.
- **Pela DAG** — a forma de produção: agendada às 22h, ou disparada à mão com
  `airflow dags trigger cnpj_carga_mensal --conf '{"sample": "20000", "db":
  "cnpj_t10"}'`. É ela quem chama `watcher.download_month` internamente — o
  retry contra o share (abaixo) é o mesmo código nos dois casos.

## Orquestração no Airflow

Quem dispara a carga mensal é a DAG `cnpj_carga_mensal`, não mais um daemon
próprio (D1, decidido e implementado em 22/09/2026 — ver a seção anterior). O
contrato está em [`airflow/spec-dag-carga.md`](airflow/spec-dag-carga.md).

```
listar_meses ──▶ detectar_mes ──▶ baixar_zips ──▶ carregar ──▶ conferir_desfecho
                      │                                             │
                      └─(sem mês novo: pula)                        │
                                                                    ▼
                                                 recuperar_indices  (all_done)
```

A DAG **chama**; não reescreve. O retry do download continua no `watcher.py`
(agora só biblioteca, ver acima), as seis fases continuam no `load.sh`, e as
regras de sanitização continuam no transform.

| item | onde |
|---|---|
| a DAG | `airflow/dags/cnpj_carga.py` (um módulo só: decisões puras no topo, operators embaixo) |
| o pool de 1 slot | `airflow/pools.json` — `airflow pools import` |
| os testes | `airflow/tests/` — T1–T12, precisam do Airflow no mesmo interpretador do pytest; T10 é ponta a ponta com zips reais |

Coisas que valem saber antes de mexer:

- **`degradado` é sucesso.** A run lê `carga.resumo`, não o código de saída do
  container: um mês com chave natural repetida termina em `degradado` e **não**
  é motivo para recarregar. Decidir pelo exit code agendaria um retry de 4 horas
  todo mês em que a Receita publicar duplicata.
- **A carga roda em container próprio**, não dentro do worker. Reiniciar o
  Airflow no meio de uma carga de 4h não pode matá-la — matá-la entre as Fases 2
  e 4 deixa a base sem índice.
- **`retries=0` na carga**, de propósito (ver `spec-dag-carga.md`, R8).
- **Nenhuma task depende de repo montado no Airflow.** A listagem do share
  (`listar_meses`) e o download rodam dentro da imagem `cnpj-carga` — decidido
  assim porque o Airflow do servidor não monta o repo de projeto nenhum (medido
  em 22/09/2026). Só `detectar_mes` roda em processo, no worker, e só fala com
  o banco.
- **Falha alerta por e-mail** (R8), pela conexão SMTP `email_notificacao` que a
  instalação do Airflow já tem. Destinatário numa Airflow Variable
  (`cnpj_carga_email_avisos`), não em código.

Para desenvolver, há um Airflow local em `../airflow-local` (3.3.0, a versão do
servidor), que monta `airflow/dags` deste repo.

**Cutover concluído neste repo; falta o servidor.** O serviço `watcher-cnpj-rfb`
já saiu do `docker-compose.yml` e do `deploy.yml`, e o CI/CD passou a construir a
imagem `cnpj-carga`. O que falta é manual, no Airflow do `srv-controladoria`:
criar o pool `cnpj_carga`, a Variable do e-mail de alerta e copiar a DAG para a
pasta de DAGs (não versionada, exige root) — passo a passo na seção 9.5 da
`spec-dag-carga.md`.

## Endpoints

| Método | Rota | Descrição |
|---|---|---|
| GET | `/healthz` | Liveness + ping no banco |
| GET | `/stats/capital-por-natureza?limit=10` | Ranking de capital social por natureza jurídica (via materialized view) |
| GET | `/stats/empresas?uf=SP&cnae=6201501&situacao=2&municipio_ibge=3550308` | Contagem de estabelecimentos com filtros opcionais |
| GET | `/stats/regime?ano=2024` | Distribuição de empresas por forma de tributação (lucro real/presumido/arbitrado/imunes-isentas). `ano` opcional |
| GET | `/empresas/{cnpj}` | Visão completa: empresa + estabelecimentos (com endereço) + QSA + Simples/MEI + **regime tributário** (lista por filial/ano). Aceita **8 ou 14 dígitos** — com 14, marca a filial consultada (`consultado: true` + `cnpj_consultado`) |
| GET | `/filial/{cnpj}?uf=SP` | Dados **só daquela filial** (14 díg.) + empresa-mãe. `uf` é opcional mas recomendado: habilita *partition pruning* (varre 1 partição em vez de 27) |
| GET | `/socios?doc=***509360**&limit=50` | Rede societária: empresas vinculadas a um documento de sócio |

> A rota `/empresas/{cnpj}` retorna **sempre a visão completa da empresa** (todas as
> filiais, QSA e Simples), tanto faz colar o CNPJ básico (8 díg.) ou o completo
> (14 díg.). Com 14 dígitos, a filial correspondente vem com `consultado: true` e o
> CNPJ pedido aparece em `cnpj_consultado`. Qualquer outro tamanho retorna `400`.

### Município na resposta (código IBGE)

O endereço de cada estabelecimento traz o município em três campos — nome, UF e
**código IBGE de 7 dígitos**, que é a chave usada para cruzar com outras bases:

```json
{
  "cnpj": "52809343000103",
  "bairro": "BELA VISTA",
  "municipio": "SAO PAULO",
  "codigo_municipio_ibge": 3550308,
  "uf": "SP"
}
```

Onde cada um aparece:

| Rota | Onde |
|---|---|
| `/empresas/{cnpj}` | em cada item de `estabelecimentos` |
| `/filial/{cnpj}` | no objeto raiz |
| `/stats/*` | não aparece (são agregados); ali o IBGE entra como **filtro**, via `municipio_ibge` |

Detalhes que evitam surpresa em quem consome:

- `codigo_municipio_ibge` é **inteiro**, não string. Códigos IBGE de município têm
  sempre 7 dígitos e nunca começam com zero, então não há dígito a perder — mas se
  o consumidor espera texto, a formatação é do lado dele.
- O código **não** é o da Receita. O `Municipios.csv` traz só o código SIAFI (4
  díg.); a tradução é feita na carga (ver [De-para de municípios](analytics/fontes-dados.md)).
- O único registro sem código é `EXTERIOR` (SIAFI 9707): vem com
  `codigo_municipio_ibge: null` e `uf: "EX"` — não é município e não tem código IBGE.
- Se todos os estabelecimentos vierem com `codigo_municipio_ibge: null`, o de-para
  não foi aplicado nesse banco: rode `IBGE_ONLY=1 DB=<banco> bash analytics/load.sh`.

### Parâmetros (query string)

| Rota | Parâmetro | Tipo | Obrigatório | Default | Observação |
|---|---|---|---|---|---|
| `/stats/empresas` | `uf` | texto (2 letras) | não | — | filtra por UF, ex. `SP` |
| `/stats/empresas` | `cnae` | inteiro | não | — | CNAE fiscal principal, ex. `6201501` |
| `/stats/empresas` | `situacao` | inteiro | não | — | situação cadastral: `2`=ativa, `8`=baixada, `3`=suspensa, `4`=inapta, `1`=nula |
| `/stats/empresas` | `municipio_ibge` | inteiro (7 díg.) | não | — | código **IBGE** do município, ex. `3550308`. Código inexistente devolve `total: 0`, não `404` |
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
curl 'http://localhost:8001/stats/empresas?municipio_ibge=3550308&situacao=2'  # São Paulo
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
`bash -n load.sh` + `py_compile watcher.py` → build da imagem `cnpj-carga`
(mesmo `watcher/Dockerfile`, duas tags: `$GITHUB_SHA` e `latest` — é a que o
`DockerOperator` da DAG executa, sem registry) → `bash -n load.sh` **dentro**
dessa imagem (pega CRLF antes de a DAG rodar) → `rsync` → `docker compose up -d
--no-recreate postgres-cnpj-rfb` seguido de `up -d --build --no-deps
--remove-orphans api-cnpj-rfb` → healthcheck em `/healthz`.

### Gotchas que o workflow existe para evitar

- **O postgres nunca é recriado.** O `up -d --no-recreate` cita só o banco; o
  `up` de build (`--no-deps`) cita só a API. Recriar o container do banco por
  causa de uma mudança de compose significaria perder uma carga de horas.
- **`--remove-orphans` é quem encerra containers de serviços removidos do
  compose** — foi ele que matou o `watcher-cnpj-rfb` no cutover de 22/09/2026
  (D1). Se algum serviço for removido de novo no futuro, é este flag que evita
  deixá-lo órfão e vivo.
- **O volume `cnpj-analytics_watcher_state` (histórico).** Guardava o último mês
  carregado do daemon antigo; hoje `carga.resumo` no Postgres é a fonte da
  verdade (R6 da `spec-dag-carga.md`), e o volume não é mais criado. Ele
  **não** é apagado automaticamente — só com `docker volume rm` à mão, sem
  pressa. O nome do projeto Compose continua fixo (`-p cnpj-analytics`) por
  outros motivos (aliases de rede, ver abaixo).
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

> ⚠️ **O orçamento da carga no servidor tem de ser 3 GB.** O `.env` de
> desenvolvimento usa `TUNE_RAM_GB=16`, o que faz sentido numa máquina dedicada.
> O servidor é **compartilhado** — 8 vCPU e 16 GB com Airflow, Kong e Traefik —
> e **não tem swap**: passar do teto ali não é lentidão, é OOM kill, e a vítima
> pode ser o Postgres de outra stack.
>
> **Isso vale de dois jeitos diferentes, e não confundir um pelo outro:**
>
> - Para a DAG do Airflow (o caminho de produção desde 22/09/2026), o orçamento
>   é uma **constante no arquivo** `airflow/dags/cnpj_carga.py`
>   (`ORCAMENTO_RAM_MB = "3072"`), **não** lido do `.env` — de propósito
>   (spec R3): um `.env` de máquina de desenvolvimento não pode vazar para o
>   container da carga em produção.
> - Para um `bash analytics/load.sh` manual no servidor (fora da DAG), o
>   default do próprio script já é 3072/4; o risco é só se o `.env` do
>   servidor tiver `TUNE_RAM_GB=16` (ou `ORCAMENTO_RAM_MB` maior) copiado do
>   ambiente de dev por engano. Se `TUNE_RAM_GB` sobrou lá de uma instalação
>   antiga, ele vale `TUNE_RAM_GB × 1024` MB quando `ORCAMENTO_RAM_MB` não está
>   definido — vale conferir e remover.
>
> ⚠️ **O `shared_buffers` NÃO entra pelo deploy.** O workflow sobe o postgres com
> `--no-recreate` e o resto com `--no-deps`, justamente para nunca derrubar o
> banco (perder uma carga de horas). Então mudar `shared_buffers` no compose não
> tem efeito até alguém recriar o container **à mão**, numa janela sem carga:
>
> ```bash
> cd /opt/applications/cnpj-analytics/prod
> docker compose -p cnpj-analytics up -d postgres-cnpj-rfb   # recria: API cai por segundos
> ```

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
