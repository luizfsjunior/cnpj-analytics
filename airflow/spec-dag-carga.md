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
| `detectar_mes` | 3 | barato, é rede |
| `baixar_zips` | 1 | o retry real já está dentro do download; o da task cobre o processo morto |
| `carregar` | **0** | retentar 4h de carga às 2h da manhã pode ser pior que não retentar. Falhou, avisa e espera a próxima janela |
| `recuperar_indices` | 2 | tem de fechar de qualquer jeito |

Alerta de falha é obrigatório e vai para onde a equipe lê — não para o
`journalctl`.

---

## 3. O fluxo

```
detectar_mes ──▶ baixar_zips ──▶ carregar ──▶ conferir_desfecho
      │                                              │
      └─(sem mês novo: pula)                         │
                                                     ▼
                                      recuperar_indices  (all_done)
```

**`schedule`:** diário às 22h (`0 22 * * *`). Substitui o par
`CHECK_INTERVAL_H=24` + `LOAD_AFTER_HOUR=22`, que eram um cron artesanal.

### detectar_mes

Chama `fetch_available_months()` / `latest_month()` do watcher, compara com o
último de `carga.resumo` (R6). Sem mês novo, a run **pula** as tasks seguintes
(short-circuit) e termina como sucesso — não como falha, nem como "skipped" que
dispare alerta.

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

T1–T9 rodam sem rede e sem banco. **T10 é obrigatório antes de ir ao servidor.**

### Onde cada um mora

| arquivo | cobre |
|---|---|
| `airflow/tests/test_t1_t2_exclusao_mutua.py` | T1, T2 |
| `airflow/tests/test_t3_nao_reimplementa.py` | T3 |
| `airflow/tests/test_t4_a_t9_dag.py` | T4–T9 |
| `airflow/tests/test_t10_ponta_a_ponta.py` | T10 |

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
`CargaFalhou`, `env_carga`) e os cinco operators embaixo.

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

A DAG carrega no Airflow de verdade (`cnpj_carga_mensal`, zero erros de import) e
o pool foi importado com 1 slot. Fumaça contra o share real e um banco
descartável com o schema `carga`: `detectar_mes` listou os 41 meses e devolveu
`2026-09`; com a competência já em `carga.resumo`, devolveu `None` e
curto-circuitou; `conferir_desfecho` traduziu `degradado` em sucesso.

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

## 8. Em aberto (precisa de decisão antes do código)

1. **Onde o `DockerOperator` encontra o socket.** Exige que o worker do Airflow
   alcance `/var/run/docker.sock`. Se os workers rodam em container sem o socket
   montado, cai para `BashOperator` com `docker compose run` — ou o socket passa a
   ser montado. **Não verificado** (exige acesso ao servidor).
2. **Ordem do cutover.** Remover o watcher antes de a DAG estar de pé deixa o mês
   sem carga. Proposta: DAG validada e disparada à mão uma vez → remove o watcher
   no commit seguinte.
3. **Depois do D2, a DAG fica duplicada aqui?** Ou `airflow/` vira só o ambiente
   de teste, com a DAG saindo do repo ao migrar.
4. **Canal do alerta de falha** (R8).
