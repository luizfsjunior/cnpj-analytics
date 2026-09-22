# Spec da DAG de carga (Airflow)

Especificação para mover a orquestração da carga mensal do `watcher.py` para uma
DAG do Airflow. Escrita em 18/09/2026.

Documento de **contrato**, não de implementação: diz o que tem de ser verdade, o
que é proibido e como se prova cada ponto. Vale o mesmo regime da
[`spec-carga.md`](../analytics/spec-carga.md): nenhuma linha de DAG antes de os
testes da seção 6 existirem.

O que esta spec **não** toca: `load.sh`, os `.sql` e o contrato da carga. A DAG
troca **quem dispara** a carga, nunca o que ela faz. Qualquer mudança de
comportamento da carga é matéria da `spec-carga.md`, não desta.

---

## 1. Decisões já fechadas (18/09/2026)

| # | Decisão |
|---|---|
| D1 | O **watcher sai de cena**. O serviço `watcher-cnpj-rfb` é removido do `docker-compose.yml`. Não fica desligado, não fica de fallback. |
| D2 | A DAG mora, no fim, no **repo do Airflow**. Mas é desenvolvida e testada **aqui**, em `airflow/`, contra um Airflow local. |
| D3 | O **download é uma task própria**, antes da carga. |
| D4 | A imagem da carga é construída pelo CI/CD deste repo e lida pelo Airflow **do daemon local** — o runner é self-hosted no mesmo servidor, então **não há registry**. |
| D5 | O modo amostra entra por **`dag_run.conf`** (params `db`, `data_dir`, `sample`), com os defaults de produção nos params da DAG. Decidido em 18/09/2026, ao descobrir que o T10 passava `--conf` e a DAG o ignorava em silêncio — rodaria a carga **completa em `cnpj_full`** achando que testava uma amostra. O orçamento fica de fora de propósito (R3): um param de RAM é um param que alguém sobe "só desta vez" num host sem swap. |

---

## 2. Os requisitos, como invariantes

### R1 — Nunca duas cargas ao mesmo tempo

É o invariante mais caro de violar: duas cargas concorrentes disputam o mesmo
schema `staging`, o mesmo `carga.indice_salvo` (a **única** cópia da DDL dos 212
índices) e o mesmo orçamento de RAM num host sem swap.

Garantido em três camadas, e as três são obrigatórias:

- `max_active_runs=1` na DAG;
- um **pool** do Airflow com **1 slot**, usado pela task de carga — isto também a
  protege de qualquer outra DAG pesada da equipe;
- o serviço `watcher-cnpj-rfb` **não existe mais** (D1). Enquanto os dois
  coexistirem, nenhuma das duas camadas acima vale nada.

> **Como se prova:** T1 e T2 da seção 6.

### R2 — A DAG não reimplementa nada

A DAG **chama**; não reescreve. Especificamente, continuam intocados e são
invocados como estão:

- o retry de download (`_download_one`, `_propfind`): 7 tentativas, backoff
  0/2/5/15/30/30/60s, resume por `.part` com header `Range`, 2ª passada. O share
  da Receita derruba 22–35% das conexões — retry de task inteira é grosso demais
  para 37 zips;
- `purge_orphan_zips`;
- `load.sh` inteiro, com suas seis fases e seu `trap EXIT`.

É **proibido** à DAG: montar comando `psql`, ler CSV, decidir fase, ou replicar
qualquer regra de sanitização.

### R3 — O orçamento do servidor continua valendo

8 vCPU e 16 GB divididos com Airflow, Kong e Traefik, **sem swap**. A carga fica
em `ORCAMENTO_RAM_MB=3072` e `ORCAMENTO_VCPU=4`, passados explicitamente pela DAG
ao container — **não** herdados de um `.env` que a DAG não controla.

Medido em 22/09/2026: por quatro dias esses dois valores foram
`os.getenv("CNPJ_ORCAMENTO_*")`, para permitir medir a carga numa máquina de
desenvolvimento sem teto. Voltaram a ser **constantes**, porque a brecha
desarmava o próprio guarda: o T8 lê as constantes do módulo, então, com a
variável definida no ambiente, ele passava a afirmar o valor da máquina — e
ficou vermelho dentro do Airflow local, que define 18432. Um teto que o teste
não consegue cobrar não é teto. Para medir sem teto, edita-se o arquivo.

O worker do Airflow não conta como consumidor durante a carga: o container é
efêmero e separado (R7), então o custo é o da carga, não o da carga somado a um
worker segurando 4 horas de `subprocess`.

### R4 — A janela sem índice sempre fecha

Entre as Fases 2 e 4 a base fica sem índice. Hoje a rede é o `trap EXIT` do
`load.sh`, que chama `recuperar_indices.sh`. Um `trap` é uma garantia de
**processo**: se o Airflow matar o container (timeout, zombie, restart), ele pode
não completar.

Portanto a DAG tem uma task `recuperar_indices` com `trigger_rule="all_done"`,
que roda **sempre** — sucesso, falha ou carga pulada. O script é idempotente e
custa ~1s quando não há nada a recuperar.

### R5 — `degradado` é sucesso

O `load.sh` tem três desfechos em `carga.resumo.desfecho`: `sucesso`, `degradado`
e `falha`. `degradado` é o mês em que a Receita publicou chave natural repetida:
o índice sai não-único, as chaves vão para `carga.duplicata` e **a carga
terminou**. Não é motivo para recarregar.

A DAG **lê `carga.resumo`** para decidir o resultado da run. É proibido decidir só
pelo exit code do container. `degradado` marca a run como sucesso e emite aviso;
`falha` reprova.

### R6 — O estado deixa de ser um arquivo

O `state.json` no volume `cnpj-analytics_watcher_state` morre junto com o watcher,
e com ele o gotcha do nome de projeto do Compose (volume novo vazio = carga
completa disparada do nada).

A fonte da verdade sobre "qual foi o último mês carregado" passa a ser
**`carga.resumo`**: a maior `competencia` com `desfecho IN ('sucesso',
'degradado')`. Já existe, já é escrita pela carga, e é o mesmo dado que a
auditoria usa. Nenhuma Airflow Variable duplicando isso.

### R7 — A carga sobrevive a um restart do Airflow

A carga roda num container **próprio** (`DockerOperator` ou equivalente), não
dentro do worker. Reiniciar ou deployar o Airflow no meio de uma carga de 4h não
pode matá-la — e matá-la entre as Fases 2 e 4 é o cenário caro.

Corolário: a task tem de ser capaz de **reencontrar** um container ainda vivo ao
voltar, ou, no mínimo, não iniciar um segundo (R1).

### R8 — Nada de intervenção de madrugada

Retry automático é explícito e restrito:

| task | retries | por quê |
|---|---|---|
| `listar_meses` | 3 | barato, é rede — herdou o motivo de `detectar_mes` quando a ida ao share saiu dele (9.2) |
| `detectar_mes` | 3 | leitura de `carga.resumo`; barato mesmo sem tocar rede externa |
| `baixar_zips` | 1 | o retry real já está dentro do download; o da task cobre o processo morto |
| `carregar` | **0** | retentar 4h de carga às 2h da manhã pode ser pior que não retentar. Falhou, avisa e espera a próxima janela |
| `recuperar_indices` | 2 | tem de fechar de qualquer jeito |

Alerta de falha é obrigatório e vai para onde a equipe lê — não para o
`journalctl`. O canal foi decidido em **22/09/2026**, e é o que a instalação já
usa (levantamento em 9.4), não um canal novo:

- `on_failure_callback` em **`default_args`**, para valer em todas as tasks —
  posto task a task, vira a linha que alguém esquece de repetir, e a task
  esquecida é justamente a que falha calada;
- o envio é `send_smtp_notification` pela conexão **`email_notificacao`**;
- o **remetente** é dito explicitamente, como `{{ conn.email_notificacao.login }}`:
  o `extra` dessa conexão está vazio e o `SmtpHook` não tem fallback — sem isso
  o envio morre em "You should provide `from_email`", ou seja, o alerta falharia
  na hora de alertar;
- o **destinatário** sai da Airflow Variable `cnpj_carga_email_avisos`. Endereço
  escrito no fonte continua avisando quem já saiu da equipe;
- o aviso cita `dag_id`, `task_id` e `log_url`. "A DAG falhou" obriga a abrir a
  interface para descobrir o quê — que é o trabalho de madrugada que o R8 evita;
- `email_on_failure` fica **desligado**: com o callback ligado, ele manda um
  segundo e-mail por falha, e o segundo não traz o link do log.

Não alertam, de propósito: mês já carregado (short-circuit, T7) e desfecho
`degradado` (R5). Um alerta por mês sujo da Receita treina a equipe a ignorar o
alerta — e aí o que importa passa junto.

> **Como se prova:** T11. O envio de verdade só se prova disparando: é o passo 5
> da seção 9.5.

---

## 3. O fluxo

```
listar_meses ──▶ detectar_mes ──▶ baixar_zips ──▶ carregar ──▶ conferir_desfecho
                      │                                              │
                      └─(sem mês novo: pula)                         │
                                                                     ▼
                                                  recuperar_indices  (all_done)
```

**`schedule`:** diário às 22h (`0 22 * * *`). Substitui o par
`CHECK_INTERVAL_H=24` + `LOAD_AFTER_HOUR=22`, que eram um cron artesanal.

### listar_meses

Decidida em **22/09/2026** (era parte do `detectar_mes`; ver 9.2). Roda **na
imagem da carga**, não no worker: chama `fetch_available_months()` do watcher
(R2) e publica a lista por XCom.

A razão de existir é de implantação, não de desenho: o `detectar_mes` importava
o watcher de um repo montado no Airflow, e o Airflow do servidor não monta repo
nenhum. Rodando na imagem, o código do watcher vem do **mesmo lugar** que as
tasks de download e carga usam — com a mesma tag de SHA, o que elimina a
possibilidade de a detecção rodar uma versão e a carga, outra.

> **Gotcha:** o XCom do `DockerOperator` é a **última linha** do stdout. O
> watcher loga em stdout, então o JSON tem de ser a última coisa impressa, e o
> comando não pode terminar com nada tagarela depois dele.

### detectar_mes

Recebe a lista por XCom, lê o último de `carga.resumo` (R6) e compara. Não
importa o watcher e não fala com o share — as duas coisas que exigiam o repo
montado. Continua em processo no worker, porque é onde o short-circuit existe:
sem mês novo, a run **pula** as tasks seguintes e termina como sucesso — não
como falha, nem como "skipped" que dispare alerta.

O `psycopg2` que ela usa já está no worker do servidor (2.9.12, medido em
22/09/2026).

Publica a competência detectada por XCom. **A competência é fixada aqui** e
passada explicitamente como `COMPETENCIA` ao `load.sh`: uma carga que começa às
22h do dia 30 atravessa a meia-noite, e o default "mês corrente" rotularia a
carga no mês errado.

### baixar_zips

Chama `download_month(mes)` do watcher, como está. Falha se o mês não vier
completo depois da 2ª passada.

### carregar

Container efêmero a partir da imagem `cnpj-carga:<sha>`:

| item | valor |
|---|---|
| comando | `bash analytics/load.sh` |
| rede | `services-net` (`PGHOST=postgres-cnpj-rfb`) |
| mount | `CNPJ_HOST_DATA_DIR` → `/data` |
| env | `DB=cnpj_full`, `DATA_DIR=/data`, `COMPETENCIA=<mês>`, `ORCAMENTO_RAM_MB=3072`, `ORCAMENTO_VCPU=4` |
| pool | `cnpj_carga` (1 slot) |
| `execution_timeout` | 20h (o `LOAD_TIMEOUT_H` de hoje) |

### conferir_desfecho

Lê `carga.resumo` da competência (R5) e decide o resultado da run.

### recuperar_indices

`bash analytics/recuperar_indices.sh`, `trigger_rule="all_done"` (R4).

---

## 4. A imagem e o CI/CD

A imagem da carga **já existe**: `watcher/Dockerfile` tem `bash`,
`postgresql-client`, `unzip`, `ripgrep`, `curl` e copia `analytics/` inteiro. Só o
`CMD` é do daemon. Com D1 o `CMD` deixa de importar — a DAG passa o comando.

No `deploy.yml`, um passo novo:

```
docker build -f watcher/Dockerfile -t cnpj-carga:${GITHUB_SHA} -t cnpj-carga:latest .
```

- **Duas tags de propósito.** A DAG fixa o **SHA** no run. Um deploy no meio de
  uma carga não afeta o container em execução, mas um retry que pegasse `latest`
  rodaria outro código no meio do mesmo mês.
- **Sem registry** (D4): o runner é self-hosted no servidor, o build cai no mesmo
  daemon que o Airflow usa.
- O `--remove-orphans` que já está no workflow **mata o watcher sozinho** quando o
  serviço sair do compose. Não é preciso passo de limpeza.

---

## 5. Fora de escopo

| item | por quê |
|---|---|
| Quebrar as 6 fases em 6 tasks | A Fase 0 deriva o orçamento e o passa por variável de shell; o `trap` cobre 2→4 como unidade; `carga.indice_salvo` é a única cópia da DDL. Retomar do meio vale pouco com 3h38 de carga e Fase 2 de 1s. |
| Reescrever o retry de download em Airflow | R2. |
| `REGIME_ONLY` / `IBGE_ONLY` como DAGs separadas | Podem virar DAGs depois; nesta entrega seguem como estão (o `load.sh` completo já os inclui). |
| Mover o postgres para o Airflow | O banco nunca é recriado. |

---

## 6. Testes — escrever antes

| # | o que guarda | como |
|---|---|---|
| **T1** | Duas runs da DAG não se sobrepõem | `max_active_runs=1` + pool de 1 slot verificados no objeto DAG; tentativa de disparar run concorrente fica em fila |
| **T2** | O watcher não existe mais | `docker-compose.yml` não tem `watcher-cnpj-rfb`; nenhum serviço cujo comando chame `watcher.py` em loop |
| **T3** | A DAG não reimplementa (R2) | a task de download chama `download_month`; a de carga invoca `load.sh` sem construir `psql` na mão |
| **T4** | `COMPETENCIA` é a detectada, não o mês corrente | run com data simulada no dia 30 às 23h carrega o rótulo certo |
| **T5** | `degradado` marca sucesso (R5) | `carga.resumo` mockada com os três desfechos → run sucesso/sucesso/falha |
| **T6** | `recuperar_indices` roda mesmo com a carga falhando (R4) | carga forçada a falhar; a task executa |
| **T7** | Sem mês novo, a run pula limpa | `latest_month` = último de `carga.resumo` → short-circuit, sem alerta |
| **T8** | O orçamento é passado explícito (R3) | env do container contém `ORCAMENTO_RAM_MB=3072` e `ORCAMENTO_VCPU=4` |
| **T9** | Retry da carga é 0 (R8) | `retries=0` na task de carga |
| **T10** | **Ponta a ponta com dado real, em amostra** | Airflow local + `SAMPLE=20000`: detecta, baixa, carrega, confere desfecho. É o T9 da `spec-carga.md` aplicado à DAG — os testes com stub não pegam o que só aparece com zip de verdade |
| **T11** | A falha avisa alguém (R8) | forma: `on_failure_callback` em todas as tasks, saindo pela conexão SMTP da instalação, com destinatário em Airflow Variable e nenhum e-mail escrito no fonte |
| **T12** | A DAG não depende do repo montado no Airflow (9.2) | forma: sem `sys.path`/import do watcher; `listar_meses` é um container da imagem da carga, com `do_xcom_push`, e `detectar_mes` lê a lista do XCom |

T1–T9, T11 e T12 rodam sem rede e sem banco. **T10 é obrigatório antes de ir ao servidor** — e, com o T12 implementado, ele precisa rodar de novo: a task nova está no caminho de todas as runs.

### Onde cada um mora

| arquivo | cobre |
|---|---|
| `airflow/tests/test_t1_t2_exclusao_mutua.py` | T1, T2 |
| `airflow/tests/test_t3_nao_reimplementa.py` | T3 |
| `airflow/tests/test_t4_a_t9_dag.py` | T4–T9 |
| `airflow/tests/test_t10_ponta_a_ponta.py` | T10 |
| `airflow/tests/test_t11_alerta.py` | T11 |
| `airflow/tests/test_t12_sem_repo_montado.py` | T12 |

```bash
pip install -r airflow/requirements-dev.txt
python -m pytest airflow/tests -q
```

Sem `apache-airflow` instalado, tudo que toca o módulo da DAG pula com motivo
explícito; os testes que olham só arquivos do repo (T2, e parte do T3) rodam
mesmo assim.

### O contrato que os testes pinam

**Um módulo só: `airflow/dags/cnpj_carga.py`** — as decisões puras no topo
(`proxima_competencia`, `SQL_ULTIMA_COMPETENCIA`, `avaliar_desfecho`,
`CargaFalhou`, `env_carga`) e os seis operators embaixo.

> Uma versão anterior desta spec pedia um segundo módulo, `cnpj_carga_lib.py`,
> para que as decisões puras rodassem sem o Airflow instalado. Descartado em
> 18/09/2026: a suíte exige o Airflow de qualquer jeito (T1, T6, T9, T10), então
> o split entregava rodar *parte* dos testes sem a dependência — e cobrava dois
> arquivos para mover e manter em sincronia quando a DAG migrar para o repo do
> Airflow (D2).

Mais `airflow/pools.json`, versionado, declarando o pool `cnpj_carga` com 1 slot
(R1) — um pool que só existe na configuração do servidor é um pool que ninguém
recria depois de um reset.

> ⚠️ **Gotcha: a pasta `airflow/` sombreia o pacote `airflow`.** Com a raiz do
> repo no `sys.path` — que é o que `python -m pytest` faz — `import airflow`
> encontra **este diretório** como namespace package em vez do Apache Airflow, e
> a suíte quebra com um erro sem relação nenhuma com o que se está testando. Não
> é hipótese: derrubou a primeira execução dos testes. O `conftest.py` remove a
> raiz do `sys.path` e verifica o módulo importado, abortando com mensagem
> explícita — mesmo espírito do guard de bash em `analytics/tests/conftest.py`.
> A alternativa definitiva seria renomear a pasta (`orquestracao/`), e ela
> continua sobre a mesa.

### Gotcha: `template_ext` engole o `command`

`DockerOperator` declara `template_ext = ('.sh', '.bash', '.env')` e `command`
é campo templated. Com isso `command=["bash", "analytics/load.sh"]` é lido como
**caminho de um arquivo de template** relativo à pasta de dags: a task morre com
`TemplateNotFound` no servidor (onde a DAG mora fora do repo, D2) e, onde o
arquivo existisse, o conteúdo inteiro do script entraria no lugar do argumento.

A DAG usa `CargaDockerOperator`, uma subclasse de três linhas com
`template_ext = ()`. O Jinja dos params continua valendo — o que sai é só a
leitura de arquivo por extensão.

Não é hipótese: apareceu ao renderizar as tasks de fato, em 18/09/2026, e
**nenhum** dos testes de forma (T1–T9) o pega. É o argumento do T10 em miniatura.

### Estado em 18/09/2026

A DAG e o `pools.json` estão implementados. Rodando no Airflow local (3.3.0):

```
4 failed, 45 passed, 7 skipped
```

| o que | estado |
|---|---|
| T1, T3 a T9 | **verdes** |
| **T2** (4 falhas) | vermelho **de propósito**: é o D1, e o cutover vem depois de a DAG ser validada (seção 8, item 2) |
| T10 (7 pulados) | espera `CNPJ_DATA_DIR` com os zips da Receita |

> Estado em 22/09/2026: **T1–T9, T11 e T12 verdes** (75 no total) e **T10 verde**
> (7, com dado real). O T2 fechou junto com o cutover completo (D1 nesse mesmo
> push). Ver "T10 verde em 22/09/2026" abaixo.

Atualização do mesmo dia, ao preparar o T10 para rodar de verdade:

- a DAG passou a ler `dag_run.conf` (D5) — `db` e `sample` vão ao ambiente do
  container, `data_dir` é a origem do bind mount (no container o caminho é
  sempre `/data`, que é o que o `load.sh` conhece);
- `CargaDockerOperator` corrige o `template_ext` (gotcha acima);
- o T10 encontra a CLI do Airflow **num container** quando ela não está no PATH
  (`T10_AIRFLOW_CONTAINER`): na máquina de desenvolvimento o Airflow local é um
  compose, e não há `airflow` no host;
- a imagem `cnpj-carga:latest` foi construída localmente, e o Airflow local já
  tem `CNPJ_REDE`, `CNPJ_REPO_DIR` e `CNPJ_HOST_DATA_DIR` apontados, com o
  scheduler nas duas redes e o socket do Docker montado.

### T10 verde em 18/09/2026

```
7 passed in 149.47s (0:02:29)
```

Os 41 zips de `2026-09` (7,3 GB — 37 do mês + 4 `entidades-*`) baixados em 33
min pelo **mesmo comando da task** `baixar_zips`. O retry do watcher (R2)
mostrou serviço: 4 quedas do share, 4 recuperadas na 2ª tentativa, nenhuma perto
do teto de 7 — e cada uma custou segundos porque retoma pelo `.part`, em vez de
reiniciar a passada inteira.

A run em amostra, contra `cnpj_t10`:

| | |
|---|---|
| `carga.resumo` | `2026-09` · sucesso · fechado · 124s · pico 3 MB RSS |
| `analytics.estabelecimento` | 20.000 (o `SAMPLE`) |
| `analytics.empresa` | 19.969 |
| `analytics.socio` / `simples` | 6.794 / 15.823 |

> ⚠️ `test_t10_os_indices_voltaram` passou **trivialmente**: `5 salvos / 239
> atuais`, porque a base nasceu nesta run e a Fase 2 não tinha o que salvar. Ele
> só cobra o que promete na **segunda** carga sobre a mesma base.

### Gotcha: CRLF mata o `load.sh` antes da linha 1

A primeira execução do T10 falhou com a task `carregar` em `StatusCode: 2`:

```
analytics/load.sh: line 58: set: pipefail: invalid option name
```

O argumento lido era `pipefail`. No índice do Git o arquivo é LF; o
`core.autocrlf=true` do Git for Windows o converte no checkout, e o
`docker build` assa o CRLF na imagem. **No servidor não aparece** — o runner é
Linux — o que o torna um erro que só morde quem desenvolve no Windows.

A pista foi `recuperar_indices` ter passado na mesma run: o
`recuperar_indices.sh` estava com LF no working tree e o `load.sh` não, vindos
do mesmo commit.

Corrigido com um **`.gitattributes`** (que não existia) fixando `eol=lf` para
`.sh`, `.sql`, `.py`, `.yml` e `Dockerfile`.

De quebra, a falha exercitou o **R4** em condição real: a carga morreu e o
`recuperar_indices` rodou assim mesmo, pelo `trigger_rule="all_done"`.

A DAG carrega no Airflow de verdade (`cnpj_carga_mensal`, zero erros de import) e
o pool foi importado com 1 slot. Fumaça contra o share real e um banco
descartável com o schema `carga`: `detectar_mes` listou os 41 meses e devolveu
`2026-09`; com a competência já em `carga.resumo`, devolveu `None` e
curto-circuitou; `conferir_desfecho` traduziu `degradado` em sucesso.

### T10 verde em 22/09/2026

```
7 passed in 193.72s (0:03:13)
```

Repetido depois de implementar `listar_meses` (T12) e o alerta SMTP (T11), com
`carga.resumo` de `cnpj_t10` resetado para o mês voltar a ser "novo". As seis
tasks rodaram, incluindo a nova `listar_meses` — encontrou os dois gotchas
documentados abaixo (XCom como string, `mount_tmp_dir`), nenhum deles hipotético:
os dois só aparecem com o container de verdade respondendo, exatamente a razão
de existir do T10.

### Gotcha: o XCom do DockerOperator é STRING, não a lista pronta

Descoberto rodando o T10 pela primeira vez com `listar_meses` (T12), em
22/09/2026: `baixar_zips` pediu o download do mês `]`. A causa era
`detectar_mes` fazer `max(disponiveis)` sobre `disponiveis = xcom_pull(...)`
sem desserializar — o XCom de um `DockerOperator` é a **última linha do
stdout como texto**, então `disponiveis` era a string `'["2026-09"]'`, e
`max()` sobre uma string itera **caracteres**: `]` (código 93) é maior que
qualquer dígito ou aspas. Corrigido com `json.loads` antes do `max`. Nenhum
teste de forma pega isto — só apareceu com o container de verdade respondendo.

### Gotcha: `mount_tmp_dir` do provider falha, mesmo avisando que não vai montar

O provider Docker tenta montar um diretório temporário do HOST em toda task
`DockerOperator`, e contra um engine remoto (o Airflow local fala com o Docker
Desktop por named pipe, não socket Unix local) ele detecta isso e avisa:
"Falling back to `mount_tmp_dir=False` mode". Medido em 22/09/2026: o aviso
apareceu em **toda** task, mas o fallback **nem sempre foi honrado** — `listar_meses`
e `recuperar_indices` rodaram limpas, `baixar_zips` morreu com
`invalid mount config for type "bind": bind source path does not exist:
/tmp/airflowtmp...`, em runs diferentes com paths diferentes. Corrigido
declarando `mount_tmp_dir=False` explicitamente em `_docker_comum`, em vez de
confiar no fallback automático — nenhuma task usa esse scratch dir (XCom sai
do stdout, não de arquivo).

### Duas mudanças que o `watcher.py` precisou para virar biblioteca

A DAG importa `fetch_available_months` e `download_month` (R2). Importar o
módulo, porém, tinha dois efeitos colaterais que só aparecem fora do daemon:

1. **`import schedule` no topo.** É dependência só do loop — o que a D1 elimina —
   e obrigaria o ambiente do Airflow a instalar uma biblioteca de agendamento
   para poder listar os meses do share. Passou para dentro de `main()`.
2. **`FileHandler` aberto no import.** Com o repo montado somente leitura, o
   import inteiro morria com `OSError: [Errno 30] Read-only file system`. O
   arquivo de log virou **melhor esforço**, configurável por
   `CNPJ_WATCHER_LOG`; o stdout continua sempre. Quem executa é o Airflow, que
   já captura o stdout.

Nenhuma das duas altera o comportamento do watcher como daemon — os 16 testes
dele continuam passando.

---

## 7. Desenvolvimento local (D2)

A DAG nasce aqui, em `airflow/dags/`. `SAMPLE=20000` deixa o ciclo em minutos em
vez de horas.

O Airflow local é **um só, compartilhado entre os projetos** — não um compose por
repo. Ele monta `airflow/dags/` deste repo na sua pasta de DAGs (bind mount ou
link simbólico), o que também é o mais parecido com o servidor, onde a DAG vai
morar fora deste repo (D2). Decidido em 18/09/2026: um Airflow por repo
multiplicaria metadados, portas e versões sem entregar nada.

Validada, a DAG é **movida** para o repo do Airflow. O que fica aqui depois disso
é matéria de decisão — ver seção 8.

---

## 8. Em aberto

Quase toda fechada pelo levantamento do servidor de **22/09/2026** (seção 9).
O que sobrou:

1. ~~Onde o `DockerOperator` encontra o socket.~~ **Resolvido**: o worker já monta
   `/var/run/docker.sock` e está na `services-net`. Não é preciso cair para
   `BashOperator`. Ver 9.1.
2. ~~Ordem do cutover.~~ **Decidido em 22/09/2026**: o watcher sai **no mesmo
   push** (D1 completo). O que protege o mês é a janela — a carga de 2026-09 já
   terminou em 18/09 e a próxima competência só aparece no share em outubro,
   então entre o cutover e a primeira janela real há semanas para validar a DAG
   com disparo à mão.
3. ~~Depois do D2, a DAG fica duplicada aqui?~~ **Decidido em 22/09/2026**: a DAG
   é **copiada para a pasta de DAGs do Airflow** e `airflow/` deste repo fica
   como ambiente de desenvolvimento e teste (é onde os T1–T10 moram). Consequência
   assumida: são duas cópias, e a daqui é a fonte. Ver 9.3 — a cópia é manual e
   exige root, porque a pasta de DAGs não é versionada nem tocada por CI/CD.
4. ~~Canal do alerta de falha (R8).~~ **Implementado em 22/09/2026**: SMTP pela
   conexão `email_notificacao` que a instalação já tem. T11 verde. Ver 9.4.
5. ~~Como `detectar_mes` alcança o `watcher.py`.~~ **Implementado em
   22/09/2026**: a listagem do share virou a task `listar_meses`, na imagem da
   carga, e a DAG deixou de depender do repo montado. T12 verde, T10 confirmou
   com dado real (achou e corrigiu dois gotchas: XCom como string,
   `mount_tmp_dir`). Ver 9.2.
6. **Bootstrap de base virgem.** `detectar_mes` faz
   `SELECT max(competencia) FROM carga.resumo` e morre se o banco ou o schema
   não existirem — quem os cria é o `load.sh`, na task seguinte. Em produção
   não morde (o `cnpj_full` tem histórico desde 2026-09), mas é o que torna o
   T10 não repetível sozinho: rodar contra um `cnpj_t10` recém-dropado exige
   aplicar à mão `CREATE SCHEMA carga` + `carga.resumo`
   (DDL em `analytics/00_carga.sql:177`). Segue em aberto de propósito: tornar
   `detectar_mes` tolerante ao bootstrap é mudança de contrato, e vem depois de
   spec e teste.

---

## 9. Implantação no servidor (levantado em 22/09/2026)

Tudo abaixo foi **verificado por leitura** no `srv-controladoria`, sem alterar
nada. Sem carga em andamento no momento do levantamento (`load average` 0,22 e o
watcher parado em "Dados já atualizados para 2026-09" desde 18/09).

### 9.1 O que já está pronto

| requisito (ver o fim de `dags/cnpj_carga.py`) | estado |
|---|---|
| Airflow alcança `/var/run/docker.sock` | **OK** — o worker monta o socket em rw |
| Airflow na rede do banco | **OK** — worker, scheduler e dag-processor estão na `services-net`, e `getent hosts postgres-cnpj-rfb` resolve de dentro do worker |
| Provider do Docker instalado | **OK** — `apache-airflow-providers-docker` 4.5.7, `docker` 7.1.0, Airflow 3.3.0 (a mesma versão do Airflow local onde os testes rodaram) |
| `CNPJ_HOST_DATA_DIR` | **OK** — `/opt/applications/cnpj-analytics/data`, idêntico ao default da DAG |
| Imagem `cnpj-carga` | **passa a existir** com o passo novo do `deploy.yml` (seção 4). Hoje o daemon só tem `cnpj-analytics-watcher*` e `cnpj-analytics-api*` |

### 9.2 O repo dentro do Airflow — resolvido tirando a dependência

`detectar_mes` rodava **em processo**, no worker, e importava
`fetch_available_months` de `CNPJ_REPO_DIR` (default `/opt/cnpj-analytics`). No
servidor **o repo não está montado em lugar nenhum do Airflow**: os binds do
worker são `logs/`, `config/`, `plugins/`, `dags/`, o socket do Docker e uma
pasta do comparador. Do jeito que estava, a task morreria no import — e o
Airflow local não pegava isso porque lá o compose monta o repo em
`/opt/cnpj-analytics`, exatamente o caminho do default.

**Decidido em 22/09/2026:** em vez de montar o repo, a listagem do share sai do
worker e vai para a imagem da carga — a task `listar_meses` (seção 3). Montar
seria mais curto, mas custava duas coisas: root num compose que é de outra
equipe (`/root/applications/airflow`), e um segundo lugar de onde o código do
watcher pode vir, livre para divergir do que está dentro da imagem.

O que muda no arquivo da DAG: `CNPJ_REPO_DIR`, `_importar_watcher` e o
`sys.path.insert` deixam de existir, e com eles o requisito 3 da lista de
"Requisitos de implantação". O que **não** muda: `conferir_desfecho` continua no
worker falando com o banco por `psycopg2` (presente, 2.9.12), o que é leitura de
uma linha, não carga.

> **Como se prova:** T12, e o T3 continua cobrando que quem lista os meses é o
> `fetch_available_months` do watcher, não um PROPFIND reescrito.

### 9.3 A pasta de DAGs é mantida à mão

`/root/applications/airflow/dags` é **root**, não é um clone git e nenhum CI/CD
escreve nela — as DAGs de lá (`bots/`, `silvert_etl/`, `dags_maintenance/`) foram
copiadas à mão, com direito a um `.bak-20260910-182557` ao lado. O repo do
comparador, que é o precedente mais próximo deste (batch invocado por
DockerOperator), **também** só constrói e tagueia a imagem no CI/CD; a DAG dele
chegou lá por fora.

Portanto o deploy da DAG é, hoje, um passo manual com root:

```
sudo install -o root -g root -m 644 \
  /opt/applications/cnpj-analytics/prod/airflow/dags/cnpj_carga.py \
  /root/applications/airflow/dags/cnpj_analytics/cnpj_carga.py
```

E o pool (R1) **não existe** — a instalação só tem o `default_pool`. Sem ele a
task de carga falha ao ser agendada:

```
docker exec -u airflow airflow-airflow-worker-1 \
  airflow pools set cnpj_carga 1 "Carga mensal do CNPJ: uma de cada vez"
```

### 9.4 O alerta de falha (R8)

Não é preciso inventar canal: a instalação já tem a conexão SMTP
`email_notificacao` e uma DAG usando `send_smtp_notification` como
`on_failure_callback` — a do comparador Protheus × Receita. O contrato está no
R8 (seção 2) e o teste é o T11.

**Estado em 22/09/2026:** contrato e teste escritos, implementação **não**. A
DAG ainda tem o `alertar_falha` provisório, que só grava um ERROR nomeado no
log; por isso o T11 está vermelho em 6 dos 12 casos, de propósito. Trocar o
provisório pelo `send_smtp_notification` é mudança de uma função.

Duas coisas que o servidor precisa antes do primeiro alerta valer:

```
docker exec -u airflow airflow-airflow-worker-1 \
  airflow variables set cnpj_carga_email_avisos "<quem recebe>"
```

e conferir que o provider `apache-airflow-providers-smtp` está instalado — a DAG
do comparador o usa, então deve estar, mas isso não foi medido.

### 9.5 Ordem de execução do cutover

T11 e T12 implementados e verdes em 22/09/2026; o T10 confirmou com dado real
(seção 6, "T10 verde em 22/09/2026") — os dois gotchas que ele pegou (XCom como
string, `mount_tmp_dir`) já estão corrigidos e documentados. Falta só o
servidor:

1. `git push deploy master` — o CI/CD constrói `cnpj-carga:<sha>` e `:latest`, e o
   `--remove-orphans` encerra o watcher (D1).
2. Criar o pool `cnpj_carga` com 1 slot e a Variable `cnpj_carga_email_avisos`
   (9.3 e 9.4).
3. Copiar a DAG para a pasta de DAGs (9.3) — passo manual, com root.
4. Conferir que a DAG carrega sem erro de import e **deixá-la pausada**.
5. Disparar à mão, em amostra, contra um banco descartável:
   `--conf '{"db": "cnpj_t10", "sample": "20000"}'`. É aqui que se prova o que
   nenhum teste de forma prova: que o `DockerOperator` sobe o container pelo
   socket (que no servidor é local, não remoto como no Airflow de
   desenvolvimento — o gotcha do `mount_tmp_dir` pode nem aparecer lá) e que o
   e-mail sai de verdade pela conexão `email_notificacao`.
6. Só então despausar, para a janela das 22h.

A janela é folgada: a carga de `2026-09` terminou em 18/09 e a próxima
competência só aparece no share em outubro. Nada obriga a fazer os seis passos
no mesmo dia.
