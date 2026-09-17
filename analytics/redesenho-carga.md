# Redesenho da carga — escrita única, blocos e paralelismo

Documento de **desenho**, não de implementação. Reúne o que foi medido em
15–16/09/2026 (máquina de desenvolvimento e servidor de produção), propõe um fluxo
novo para `load.sh` + `03_transform.sql`, e lista os trade-offs, o que ainda
precisa ser **medido** (seção 6) e o que já foi **decidido** (seção 7) antes de
escrever código. As duas decisões da seção 7 estão fechadas em 16/09/2026: a
carga termina sozinha e degradada em vez de parar de madrugada (7.1), e a
equivalência com a carga atual é provada em três níveis, o primeiro deles
escrito antes do código (7.2). O que ainda bloqueia a spec é a seção 6.

Complementa o [`tuning-carga.md`](tuning-carga.md), que trata de configuração do
Postgres. Aqui o assunto é o **caminho do dado**.

---

## 1. O problema

Hoje cada linha é escrita **duas vezes** dentro do Postgres:

```
unzip -p Estabelecimentos0.zip
  │
  ├─► \copy staging.estabelecimentos      ESCRITA 1  (30 colunas text, sem constraint)
  │
  └─► INSERT ... SELECT (03_transform)    LEITURA + ESCRITA 2
         cast, parse_date, regex, concat de CNPJ/DDD, roteamento de partição
         ↓
      analytics.estabelecimento
```

O schema `staging` existe apenas para dar ao SQL um lugar de onde fazer `SELECT`,
e o próprio `load.sh` o dropa no fim ("libera ~27GB"). É trabalho integralmente
descartável, e ele é caro.

---

## 2. Medições

### 2.1 Carga completa na máquina de desenvolvimento

16 CPUs, 23,5 GB para o WSL2, `shared_buffers` 8 GB, `TUNE_RAM_GB=16`.
Total: **3h05**.

| fase | tempo | fatia |
|---|---|---|
| COPY (5 tabelas → staging) | 42m17s | 23% |
| **transform** (staging → analytics) | **1h32m42s** | **50%** |
| índices (15) | 44m46s | 24% |
| matviews + regime | 5m37s | 3% |

Dentro do transform:

| tabela | linhas | tempo | M linhas/min |
|---|---|---|---|
| empresa | 68,6 mi | 14,6 min | 4,70 |
| estabelecimento | 71,9 mi | 30,4 min | 2,36 |
| estabelecimento_cnae_secundario | 121,7 mi | 30,3 min | 4,02 |
| socio | 27,8 mi | 4,5 min | **6,18** |
| simples | 49,0 mi | 12,9 min | 3,80 |

`socio` é o mais rápido por linha e é o **único INSERT sem `ON CONFLICT`**.

Índice mais caro: `ix_empresa_razao_trgm`, 13,7 min e 3,4 GB (GIN trgm).

### 2.2 Experimentos A/B (volume total, 71,9 mi de estabelecimentos)

| caminho | tempo |
|---|---|
| **A** — COPY→staging (22m19s) + INSERT...SELECT (30m42s) | **53m01s** |
| **B** — COPY direto na tabela final, CSV já tipado | **29m05s** (−45%) |
| **C** — INSERT...SELECT de `file_fdw` (medido com 1 zip) | **−34%** sobre o A |

O A bateu com a carga real (53m01s vs 51m38s), o que valida o método.

**O achado que importa:** carregar direto na tabela final (29m05s) custa
praticamente o mesmo que só o INSERT do transform (30m42s). Os 22m19s do COPY
para o staging são economia pura.

No caminho C, o `INSERT` é **o mesmo SQL** — só muda o `FROM`.

> ⚠️ **A 2.9 mediu o C no volume completo e ele perdeu.** "Só muda o `FROM`" é
> verdade no texto e falso no relógio: no volume completo o `FROM` de `file_fdw`
> custa **86% a mais** que o do heap. Os −34% desta tabela são de 1 zip e não
> escalaram.

### 2.3 Servidor de produção (carga de 15/09, 22:00)

`srv-controladoria`: 16 GB de RAM (5,7 GB livres, **sem swap**), `shared_buffers`
**128 MB** (default, nunca configurado), `TUNE_RAM_GB=6`. O resto do inventário
do host — 8 vCPU, disco, e os outros 30 containers que dividem tudo isso — está
na **2.7**.

| fase | servidor | desenvolvimento | |
|---|---|---|---|
| COPY empresas | 1,1 min | 8,8 min | **8,3× mais rápido** |
| COPY estabelecimentos | 4,4 min | 21,3 min | 4,9× mais rápido |
| COPY socios | 0,6 min | 4,4 min | 7,7× mais rápido |
| COPY simples | 0,7 min | 6,5 min | 10,1× mais rápido |
| **COPY total** | **6,9 min** | **42,1 min** | **6× mais rápido** |
| **INSERT empresa** | **34,8 min** | **14,6 min** | **2,3× MAIS LENTO** |

> O servidor escreve muito mais rápido que a máquina de desenvolvimento (disco
> melhor, sem a virtualização de I/O do WSL2), mas é **2,3× mais lento no
> `INSERT ... SELECT`**.
>
> **Atenção à armadilha desta comparação** (ela custou uma conclusão errada):
> em desenvolvimento a medição foi feita num banco **criado do zero**
> (`cnpj_bench`), onde a tabela de destino só tinha a PK; no servidor é uma
> **recarga** sobre `cnpj_full`, cuja tabela já carrega os índices das cargas
> anteriores. Não é o mesmo experimento. A causa da diferença está na seção 2.4,
> e **não** é o `shared_buffers`.

Extrapolando pela proporção do transform medida em desenvolvimento (empresa =
15,8% do total), o transform no servidor deve levar **~3h40**. É ele, e não o
COPY, que explica as mais de 20h da carga de produção.

**Consequência para o desenho:** no servidor, paralelizar o COPY é irrelevante
(7 minutos de 20 horas). O que vale é **não reler o staging** — exatamente o que
a escrita única elimina.

### 2.4 O custo escondido dos índices na recarga (o maior achado)

`03_transform.sql` faz `TRUNCATE` + `INSERT` de 100% das linhas, e **não dropa
índice nenhum**. `TRUNCATE` esvazia a tabela mas preserva os índices, e
`04_indexes.sql` cria os 13 índices com `CREATE INDEX IF NOT EXISTS`. Logo:

* **1ª carga da vida do banco** — a tabela só tem a PK durante o INSERT, e os
  índices são construídos depois, em massa. É o que o cabeçalho do
  `04_indexes.sql` pretende ("criar índices no fim é ordens de magnitude mais
  rápido").
* **Toda recarga seguinte** (ou seja, toda carga mensal do watcher) — os índices
  já existem e são mantidos **linha a linha** durante o INSERT. O
  `CREATE INDEX IF NOT EXISTS` no fim responde em 0,36s e dá a impressão de que a
  fase de índices é barata: o trabalho dela já foi pago, mais caro, dentro do
  transform.

Medido com `analytics.empresa` e `Empresas1.zip` (4,5 mi de linhas):

| caminho | tempo |
|---|---|
| **Recarga de hoje** — INSERT com 3 índices vivos | 3m07s |
| **Proposto** — INSERT sem índices (1m05s) + `CREATE INDEX` (0m52s) | **1m57s (−37%)** |

O INSERT sozinho fica **2,87× mais lento** com os índices vivos.

Decomposição por tipo de índice (1,5 mi de linhas), que mostra *por que*:

| | incremental (dentro do INSERT) | em massa (`CREATE INDEX`) | |
|---|---|---|---|
| 2 × btree | 14,3s | 2,8s | **5,1× mais barato** |
| 1 × GIN trgm | 31,0s | 10,1s | **3,1× mais barato** |
| `CREATE INDEX IF NOT EXISTS` (já existe) | — | 0,36s | no-op |

Construir em massa lê a tabela sequencialmente, ordena de uma vez com
`maintenance_work_mem` e escreve páginas densas e ordenadas. A manutenção
incremental faz, por linha, uma descida em árvore (leitura aleatória), o encaixe
da entrada, eventuais *page splits* e WAL de cada página tocada. O GIN sofre mais
porque cada linha toca várias listas de trigramas.

> Isto explica a diferença servidor × desenvolvimento da seção 2.3: os 2,38×
> observados são praticamente os 2,87× de penalidade por índices vivos. O
> `shared_buffers` de 128 MB continua sendo um problema, mas **não é a causa
> principal** daquela diferença.
>
> A correção vale apenas porque aqui se reinsere **100%** das linhas. Numa carga
> verdadeiramente incremental (menos de ~10–20% da tabela), manter os índices
> vivos seria o caminho certo.

### 2.5 Outros dados coletados

- **Zero duplicatas** `(cnpj, uf)` em estabelecimento: o staging tem 71.874.448
  linhas, **todas** passam no filtro `^\d{8}$`, e foi esse o total carregado.
  O `ON CONFLICT DO NOTHING` não descartou nada. Vale **só para estabelecimento**
  — as outras tabelas estão na seção 2.6, e lá há duplicata.

  > **Correção de 16/09/2026.** Esta linha dizia "o staging tem 71.900.890
  > linhas, 71.874.448 passam no filtro", e a diferença de **26.442** virou, na
  > spec-carga.md, a prova de que a sanitização descartava linhas em silêncio.
  > O número estava errado. Conferido de três formas independentes durante a
  > carga de ensaio da v2: os 10 `Estabelecimentos*.zip` têm 71.874.455 linhas
  > físicas; `count(*)` exato em `cnpj_bench.staging.estabelecimentos` (a base
  > desta própria medição) dá 71.874.448; e a staging da carga nova dá o mesmo.
  > O filtro `^\d{8}$` descarta **zero** linhas — nas quatro tabelas, não só
  > nesta. Não existem 26.442 linhas descartadas.
- **`Estabelecimentos0.zip` é ~6× maior** que os outros (6,5 GB descomprimido
  contra ~1,0 GB), com ~30 dos ~73 milhões de registros. O mesmo vale para
  `Empresas0.zip` (29,6 mi de 70,1 mi) e `Socios0.zip`.
- **Disco**: staging 27 GB + analytics 58 GB (dos quais **28 GB são índices**).
  Pico de ~85 GB durante a carga. No servidor o `cnpj_full` está em **89 GB**
  (2.7) — este número é da máquina de desenvolvimento.
- `file_fdw` **já vem** na imagem `postgres:18-trixie`; o container **não tem**
  `unzip`.
- **Nenhum campo tem quebra de linha embutida.** Nos 2 primeiros milhões de
  linhas de `Estabelecimentos1`, `Empresas1` e `Socios1` não há uma única linha
  com número ímpar de aspas — logo, 1 linha = 1 registro. É o pré-requisito que
  torna o `split -l` da Fase 0 seguro (comando na Fase 0). Amostra, não prova:
  vale repetir se o layout da RFB mudar.

### 2.6 Duplicatas na fonte — varredura completa

A seção 2.5 tinha verificado só `estabelecimento`. Esta é a varredura das cinco
tabelas, feita em 16/09/2026 sobre o `staging` do `cnpj_bench` — a fonte **crua**,
antes de qualquer `ON CONFLICT`.

| tabela | chave verificada | chaves duplicadas | linhas excedentes | varredura |
|---|---|---|---|---|
| `empresa` | `cnpj_basico` | **1** | 1 | 1m28s |
| `estabelecimento` | `(cnpj, uf)` | 0 | 0 | 2m53s |
| `estabelecimento` | `cnpj` (ignorando a UF) | 0 | 0 | 2m21s |
| `estabelecimento_cnae_secundario` | `(cnpj, cnae)` pós-`unnest` | **3.628** | **3.939** | 2m16s |
| `socio` | linha inteira (11 colunas) | **22** | 22 | 26s |
| `simples` | `cnpj_basico` | 0 | 0 | 40s |
| 6 dimensões | `codigo` | 0 | 0 | <1s |
| `regime_tributario` | `(cnpj, ano, scp, forma)` | 0 | 0 | 9s |

Há duplicata em **três** tabelas, e cada uma é de um tipo diferente.

**`empresa` — 1 ocorrência, e as linhas divergem.** O `cnpj_basico` `08314885`
aparece duas vezes:

```
FLAVIO PAVAO DE SOUZA | nat 4120 | qual 59 | porte 05
(vazio)               | nat 0000 | qual 00 | porte (vazio)
```

Uma é o registro real, a outra é lixo. O `ON CONFLICT DO NOTHING` fica com a
**primeira linha do arquivo**, que neste mês é a boa (ctid `(422954,25)` contra
`(422954,26)`, adjacentes) — `analytics.empresa` está correta. Mas é sorte: com a
ordem invertida o lixo vence, em silêncio. O problema aqui não é volume, é a
**ausência de regra de desempate**, e ele já existe hoje.

**`estabelecimento_cnae_secundario` — 3.939 linhas descartadas todo mês.** É
duplicata gerada pelo próprio `unnest`: a RFB repete o mesmo código dentro da
string `cnae_secundaria` do mesmo estabelecimento (até **6 vezes** no pior caso).
Não é o arquivo que está duplicado, é o formato de lista. O `ON CONFLICT` da
linha 112 do `03_transform.sql` é a única coisa que segura isso — removê-lo sem
substituto quebra a carga com certeza, não com probabilidade.

**`socio` — 22 linhas duplicadas já estão no banco.** `analytics.socio` tem PK
sintética (`id` gerado) e nenhuma chave natural, então nunca houve descarte. São
22 linhas 100% idênticas, todas com o mesmo padrão: `identificador_socio = 3`
(sócio estrangeiro), qualificação 37, sem CPF/CNPJ. Único caso em que o banco
**hoje** contém duplicata — e independe deste redesenho.

**`regime_tributario` está limpo.** 36.263 CNPJs têm mais de uma linha no mesmo
ano, mas é dado legítimo: uma linha por SCP mais a da própria empresa. A chave
real é `(cnpj, ano, cnpj_da_scp, forma_de_tributacao)`, e nela não há repetição.
O `SELECT DISTINCT` do `regime_transform.sql` é o precedente desse padrão aqui.

Duas ressalvas sobre estes números:

* A varredura inteira custou **~8 min** nesta máquina (16 CPUs, `work_mem` 2 GB,
  8 workers, cache quente). No servidor, com `shared_buffers` de 128 MB, seria
  bem mais lenta — não extrapole.
* É **um mês**. A duplicata de `empresa` é 1 hoje e pode ser 0 ou 50 na próxima
  carga. O tratamento tem de ser permanente, não uma limpeza pontual. Sinal
  disso: a duplicata de `empresa` é real e reaparece a cada mês, ou não.

  > **Nota de 16/09/2026.** Este parágrafo dizia que a divergência com a 2.5
  > (71.900.890 linhas com 26.442 reprovadas, contra 71.874.448 com zero aqui)
  > se explicava por serem "cargas de meses diferentes". Não era: as duas seções
  > mediram o **mesmo** `cnpj_bench`, e o `count(*)` exato dele é 71.874.448 com
  > zero reprovadas. O 71.900.890 da 2.5 é que estava errado — ver a correção
  > registrada lá.

### 2.7 O servidor por dentro — os números que faltavam (16/09/2026)

Leitura por SSH, sem escrever nada. Fecha o item 5 da seção 6 e derruba duas
premissas deste documento.

| o que | valor | como estava no documento |
|---|---|---|
| CPU | **8 vCPU** — Xeon Gold 6542Y, 4 núcleos físicos com HT | desconhecido |
| RAM | 16 GB, **5,8 GB disponíveis**, **sem swap** | "5,7 GB livres, sem swap" ✔ |
| disco | 451 GB, 167 GB usados, **267 GB livres** | desconhecido |
| `cnpj_full` | **89 GB** | "~58 GB" ✗ |
| `shared_buffers` | 128 MB (nunca configurado) | ✔ |
| `work_mem` / `maintenance_work_mem` / `max_wal_size` | 92 MB / 737 MB / 3 GB | ✔ |
| carga do host | **load average 2,0–2,4** — ⚠️ **não era repouso**, ver abaixo | desconhecido |

**Disco deixou de ser o trade-off que a seção 4 anunciava.** Com 267 GB livres,
os ~30 GB de CSV temporário não competem com nada, e a variante "carregar em
banco paralelo e trocar" — que exige ~89 GB de segunda cópia — passa a ser
**viável**, não teórica. Ela resolveria de graça o risco da Fase 2/Fase 3 (a base
servida nunca fica sem índice) que a seção 7.1 tratou com quarentena. Vale
reabrir a comparação; não há mais o argumento de espaço contra ela.

**CPU é a restrição real, e é pior que 8.** O servidor **não é dedicado**: 30
containers de pé (Airflow com 4 workers, Kong, Traefik, Memgraph, Vaultwarden,
dois Postgres de controladoria), com `load average` de 2,0–2,4 no momento da
medição.

> ⚠️ **Correção (2.11): esse 2,0–2,4 NÃO era repouso.** A carga do CNPJ de 15/09
> ainda estava rodando durante toda esta coleta. A linha de base dos outros
> serviços é **mais baixa** que 2,2 — sobra mais folga de CPU do que este
> documento supôs, e o orçamento da 7.3 é conservador, não apertado. Quanto mais
> baixa, não se sabe: exige medir com o servidor realmente ocioso. Três consequências
diretas sobre o desenho:

- o teto de `LOAD_JOBS` não é 8, é ~5–6 vCPU efetivas, **compartilhadas** entre
  os blocos do INSERT e a descompressão da Fase 0 (que aqui deixa de estar
  sobreposta ao COPY e vira concorrente);
- a memória é o limite mais duro: 5,8 GB disponíveis, sem swap, com `work_mem` de
  92 MB **por conexão** e os limites do Airflow (4+2+2 GB) já reservados. Estourar
  não é lentidão, é OOM kill — e o OOM killer pode escolher o Postgres **de
  outro** serviço;
- qualquer medição de paralelismo feita na máquina de desenvolvimento (16 vCPU
  ociosas) é **teto otimista**, não previsão.

---

### 2.8 Paralelismo da Fase 2, medido (16/09/2026)

Experimento controlado em `cnpj_bench` (16 vCPU ociosas, `shared_buffers` 8 GB,
`work_mem` 245 MB, `synchronous_commit=off`). Alvo: cópia de
`analytics.estabelecimento` particionada por UF, **sem PK e sem índices** — o
estado da Fase 2. Fontes pré-materializadas a partir da `staging` e **aquecidas
antes de cada rodada**, para que só o caminho de escrita variasse. O `INSERT` é o
mesmo do `03_transform.sql`, **sem** `ON CONFLICT`.

| configuração | linhas | tempo | vazão | speedup | vazão **por job** |
|---|---|---|---|---|---|
| 1 job | 8.043.912 | **97,4 s** | 4,96 mi/min | 1,00× | 4,96 |
| 2 jobs | 8.043.912 | **55,6 s** | 8,68 mi/min | 1,75× | 4,45 |
| 3 jobs | 6.049.765 | **37,3 s** | 9,73 mi/min | — | 3,44 |
| 4 jobs | 8.043.912 | **38,8 s** | 12,44 mi/min | 2,51× | 3,29 |
| 4 jobs, fatiado **por UF** | 8.043.912 | **42,3 s** | 11,41 mi/min | 2,30× | 3,10 |
| 4 jobs, **4 tabelas independentes** | 8.043.912 | **43,9 s** | 11,00 mi/min | 2,22× | 3,05 |

(A linha de 3 jobs carrega um volume menor — três fatias em vez de quatro. A
coluna que a torna comparável é a vazão, não o tempo.)

**4 jobs rendem 2,5×, não 4×.** A vazão por job cai um terço já na terceira
conexão: 4,96 → 3,44 mi/min. O ganho não some, mas metade dele sim.

**Fatiar por UF não resolve — e a hipótese da seção 6 estava errada.** O desenho
supunha que o teto fosse o *relation extension lock* das 28 partições
compartilhadas, e que a saída seria fatiar os blocos por UF em vez de por posição
no arquivo (o que complicaria a Fase 0). Duas medições derrubam isso:

- fatiar por UF ficou **mais lento** (42,3 s contra 38,8 s). SP sozinho é 28,7%
  do recorte, então o bloco de SP vira o caminho crítico e o teto de speedup com
  4 blocos por UF é 3,5×, não 4× — antes de qualquer contenção;
- o controle decisivo: 4 jobs escrevendo em **4 tabelas independentes**, sem
  nenhuma relação compartilhada nem partição, levou **43,9 s** — pior que os
  38,8 s na tabela particionada. Se a contenção fosse de relação, este era o
  cenário que voaria.

O teto é **global** (WAL, I/O, CPU), não de relação. Consequência prática: a
Fase 0 continua fatiando por posição no arquivo, que é o corte barato.

**O que esta medição não diz.** A amostragem de `wait_event` falhou na execução e
foi descartada, então não há evidência direta para separar WAL de I/O de CPU —
sabe-se onde o teto **não** está, não exatamente onde ele está. E tudo isto é a
máquina de desenvolvimento, com 16 vCPU ociosas e `shared_buffers` de 8 GB: pela
2.7 e pela 7.3, no servidor há ~4 vCPU de orçamento e 128 MB de
`shared_buffers`. **Estes números são teto otimista, não previsão.**

**Escolha que isto sustenta:** `LOAD_JOBS = 3`. Fica dentro do orçamento de 4
vCPU da seção 7.3 deixando ~1 core para a descompressão da Fase 0, e entrega a
maior parte do que 4 jobs dariam — num servidor onde o quarto job disputaria CPU
com o `unzip` e com os outros 30 containers.

### 2.9 `file_fdw` em escala completa e a dupla leitura (16/09/2026)

Fecha os itens 3 e 6 da seção 6. Mesma máquina, mas **`shared_buffers` em 2 GB**,
não 8 GB: o host ficou sem memória livre durante a bateria e foi preciso devolver
RAM ao Windows. Os números desta seção **não são comparáveis linha a linha com a
2.8**; o que vale aqui é a comparação **interna**, entre braços rodados na mesma
configuração. Alvo sem PK e sem índices (Fase 2), `INSERT` idêntico ao do
`03_transform.sql` sem `ON CONFLICT`. O CSV de 13,9 GB saiu da própria `staging`
por `COPY TO`, em 12m36s.

#### Item 3 — o `file_fdw` **perde** no volume completo

| braço | fonte | linhas | tempo | vazão |
|---|---|---|---|---|
| **A** | `staging.estabelecimentos` (heap) | 71.874.448 | **17m23s** | 4,13 mi/min |
| **C** | foreign table `file_fdw` sobre o CSV | 71.874.448 | **32m20s** | 2,22 mi/min |

**C é 86% mais lento que A.** O documento carregava o oposto: a 2.2 registrou
"−34% sobre o A" para o caminho C, **medido com 1 zip de 4,7 mi linhas**. O ganho
não sobreviveu à escala — e este é exatamente o risco que o item 3 da seção 6
levantou.

Não é falta de paralelismo: o `EXPLAIN` dos dois lados mostra varredura
**serial** (`Seq Scan` e `Foreign Scan`, nenhum `Parallel`), porque
`INSERT ... SELECT` não paraleliza o `SELECT` aqui. A diferença é o custo de
**parsear CSV**: o planejador estima o `Foreign Scan` em 10× o `Seq Scan` do
heap, e o relógio confirma a ordem de grandeza.

**O que isso faz com o desenho.** A premissa da 2.2 — "o `INSERT` é o mesmo SQL,
só muda o `FROM`" — é verdadeira no texto e falsa no relógio: o `FROM` é metade
do custo. Somando os caminhos completos, com o COPY para staging de 22m19s
(2.2) e a escrita do CSV da Fase 0 aproximada pelos 12m36s do `COPY TO`:

| caminho | composição | total |
|---|---|---|
| **atual** | COPY→staging 22m19s + INSERT 17m23s | **~39m42s** |
| **novo** | Fase 0 ~12m36s + INSERT de `file_fdw` 32m20s | **~44m56s** |

A escrita única continua economizando uma escrita, mas **paga mais caro na
leitura do que economiza na escrita**. Com estes números o redesenho do caminho
do dado deixa de se pagar sozinho — e a ação isolada da seção 8 (dropar e recriar
índices, −37% medidos) passa a ser a alavanca de maior retorno do documento
inteiro, não a menor.

Ressalva honesta: os 12m36s são um `COPY TO` do Postgres, não o `unzip | split`
que a Fase 0 faria de verdade, e o braço A não inclui a descompressão que o COPY
para staging embute. A margem de erro não é pequena — mas ela teria de ser de
~15 minutos para inverter o resultado.

#### Item 6 — a dupla leitura compensa em paralelo

Fatia de 8 mi linhas (CSV de 1,5 GB), `estabelecimento` e
`estabelecimento_cnae_secundario` saindo do **mesmo** arquivo:

| cenário | tempo |
|---|---|
| em sequência (duas passadas) | **409 s** (estab 233 s + cnae 175 s) |
| **em paralelo** (dois leitores no mesmo CSV) | **320 s** (−22%) |

Ler o mesmo CSV duas vezes ao mesmo tempo é **22% mais barato** que ler duas
vezes em sequência: o segundo leitor encontra no cache o que o primeiro acabou de
trazer, e a sobreposição paga o custo da disputa. A Fase 2 deve disparar os dois
consumidores do CSV de estabelecimentos **juntos** — e isso ocupa 2 das 4 vCPU do
orçamento da 7.3, o que precisa ser contado no `LOAD_JOBS`.

(O `cnae_secundario` gerou 13,2 mi linhas a partir de 8 mi estabelecimentos.)

### 2.10 O que dá para afirmar sobre o servidor sem rodar lá

Esta seção é **raciocínio sobre medições**, não medição — está separada de
propósito. Mas ela não é chute: as duas pontas da conta estão medidas, uma na
2.3 (servidor) e outra na 2.9 (aqui).

**A conta que decide.** A escrita única troca uma coisa por outra:

| | o que ela **economiza** | o que ela **custa** |
|---|---|---|
| medida onde | servidor, 2.3 | desenvolvimento, 2.9 |
| o quê | o COPY para a staging | trocar a leitura do heap pelo parse do CSV |
| quanto | **6,9 min** (as 5 tabelas) | **+15 min** (só `estabelecimento`) |

No servidor, o COPY que a escrita única elimina custa **6,9 minutos de uma carga
de 20 horas** — a própria 2.3 já dizia isso ("paralelizar o COPY é irrelevante").
O que ela não dizia, porque ainda não estava medido, é que o preço do outro lado
é **mais que o dobro do que se economiza**, e só na maior das cinco tabelas.

**Por que a penalidade do `file_fdw` tende a ser pior lá, não melhor.** O custo
extra medido na 2.9 é **CPU**: parse de CSV, linha a linha, num plano serial dos
dois lados. Isso muda o sinal do argumento que a seção 5 usava:

- o argumento antigo — "lá a releitura do staging é aleatória com 128 MB de
  `shared_buffers`, o CSV é sequencial" — não se sustenta: uma releitura de
  staging é `Seq Scan`, e um `Seq Scan` com readahead não vira I/O aleatório por
  ter pouco cache. O I/O aleatório que o `tuning-carga.md` descreve é o da
  **criação de índices**, não o da varredura;
- e a 2.3 mostra que o servidor **escreve 6× mais rápido** que esta máquina, o
  que indica disco melhor, não pior. O gargalo dele não é ler sequencialmente,
  é CPU e é índice vivo (2.4);
- do lado da CPU, o servidor tem **8 vCPU compartilhadas** com `load average`
  2,0–2,4 em repouso (2.7), contra 16 vCPU ociosas de um i7-13620H aqui. Uma
  penalidade que é CPU pura não melhora nessa troca.

**Conclusão que se pode assinar sem SSH:** no servidor, a escrita única via
`file_fdw` é **líquido negativo** — economiza ~7 min e paga ≥15 min, antes de
contar o custo próprio da Fase 0 (descompressão + escrita dos 14 GB), que lá
compete por CPU com o resto do host.

**O que isso NÃO permite afirmar.** O item 4 da seção 6 continua aberto para o
que depende de constantes locais: o ganho real do `LOAD_JOBS=3` sob contenção, o
pico de RSS sem swap (7.3) e o efeito de subir o `shared_buffers` para 1 GB
(seção 8). Nenhum desses é derivável daqui.

**O que falsificaria o raciocínio acima**, e vale medir se alguém quiser insistir
no `file_fdw`: um `INSERT` de foreign table no servidor que fique **dentro** de
~7 minutos do equivalente a partir da staging. Se ficar, a conta se inverte. É um
teste de um comando, num banco descartável, e não precisa de carga completa —
mas precisa rodar lá.

> **Decidido em 16/09/2026: não rodar.** Dois motivos. O primeiro é operacional —
> havia carga real em andamento (2.11), e não se disputa RAM com ela num host sem
> swap. O segundo é que o teste deixou de importar: a 2.11 mostrou que **16 das
> ~20 horas** da carga estão na manutenção de índice de uma tabela só. Ganhar ou
> perder 7 minutos no caminho do dado é irrelevante diante disso. O teste fica
> documentado aqui para quem quiser reabrir o `file_fdw` no futuro; não é
> pré-requisito de nada.

### 2.11 A carga de 15/09 vista por dentro, 18h depois (16/09/2026)

Ao preparar o teste da 2.10 no servidor, descobriu-se que **a carga de 15/09
ainda estava rodando** — 18,5 h depois de começar, ainda no transform. O teste
foi cancelado (não se disputa RAM com uma carga real num host sem swap), mas a
carga em andamento é, ela própria, a melhor medição do documento.

**Linha do tempo real, do log do watcher:**

| marco | horário | duração |
|---|---|---|
| COPY das 5 tabelas termina | 15/09 22:06:55 | ~7 min (confirma a 2.3) |
| `INSERT` de `empresa` termina | 15/09 22:41:47 | 34,8 min (confirma a 2.3) |
| `INSERT` de `estabelecimento` termina | **16/09 14:40:33** | **15h59m** |
| `INSERT` de `estabelecimento_cnae_secundario` | em andamento | 1h40 e contando |

**A extrapolação da 2.3 estava errada por 4–5×.** Ela previa "~3h40 para o
transform" projetando a proporção medida em desenvolvimento. O real é que **uma
única tabela levou 16 horas**. A proporção de dev não transporta para o servidor,
e o motivo está abaixo.

**A causa, medida e não inferida.** O `INSERT` ativo agora espera em
`IO / DataFileRead` — leitura aleatória de página, não escrita. E o inventário
explica por quê:

| | valor |
|---|---|
| índices nas tabelas `estabelecimento*` | **212 índices, 14 GB** (6 por partição × 28 + PKs) |
| índice de `estabelecimento_cnae_secundario` | **5,15 GB** |
| `shared_buffers` | **128 MB** |

São **14 GB de índice mantidos vivos através de um cache de 128 MB**. Cada uma
das ~500 milhões de inserções de entrada de índice (73 mi linhas × 6–7 índices)
tem chance alta de ser uma leitura aleatória de disco. Não é o `INSERT` que
demora: é a manutenção de índice batendo em disco, linha a linha, por 16 horas.

**Consequência, e ela reordena o documento inteiro.** A seção 2.4 já dizia que o
índice vivo na recarga era "o maior achado"; a 2.11 põe número nele: **16 horas
de uma carga de ~20**. Dropar os índices antes do transform e recriá-los depois
(seção 8, item 1) deixa de ser "a ação de maior retorno por esforço" e passa a
ser **a única que ataca o gargalo real**. Nem a escrita única (2.9, que perde),
nem o paralelismo (2.8, que vale 2×), nem o `file_fdw` chegam perto disso — eles
disputam os ~4 h restantes enquanto 16 h ficam intocadas.

Ressalva: `blks_read` acumulado do banco (9,2 TB, 97,6% de hit) vem de todas as
cargas desde a criação — `stats_reset` está nulo. Ele não isola esta carga, mas o
`wait_event` do `INSERT` ativo e o inventário de índices, esses sim, são desta.

---

## 3. O desenho proposto

```
        ┌─ FASE 0: preparo (paralelo) ──────────────────────────┐
        │  zips ─► unzip -p | tr -d '\000' ─► csv/<tabela>_NN   │
        │  blocos equilibrados (o zip 0 é fatiado)              │
        └───────────────────────────────────────────────────────┘
                              │
        ┌─ FASE 1: schema ─────────────────────────────────────┐
        │  tabelas SEM PK/unicidade                            │
        │  1 FOREIGN TABLE (file_fdw) por bloco                │
        └──────────────────────────────────────────────────────┘
                              │
        ┌─ FASE 2: INSERT ... SELECT, escrita ÚNICA ───────────┐
        │  empresa │ estabelec.(+cnae_sec) │ socio │ simples   │
        │  em paralelo, LOAD_JOBS configurável                 │
        └──────────────────────────────────────────────────────┘
                              │
        ┌─ FASE 3: unicidade + índices ────────────────────────┐
        │  CREATE UNIQUE INDEX (ex-PKs) + 04_indexes.sql       │
        └──────────────────────────────────────────────────────┘
                              │
        ┌─ FASE 4: IBGE, regime, matviews ─────────────────────┐
```

### Fase 0 — Preparo

Descompacta em paralelo para uma pasta montada **read-only** no container do
Postgres. Custo: ~30 GB temporários (contra os 27 GB de staging que deixam de
existir) e um passo que hoje é sobreposto ao COPY.

**Balanceamento é obrigatório.** Com 10 tarefas iguais, o zip 0 (6× maior)
determinaria o tempo total. Ele é fatiado em ~6 pedaços equivalentes aos demais,
resultando em ~15 blocos parelhos de ~1 GB.

**Mas o balanceamento resolve a Fase 2, não a Fase 0.** Um zip não se fatia sem
descomprimir em sequência: `unzip -p Estabelecimentos0.zip | split` é serial por
natureza. Os 6,5 GB do zip 0 continuam sendo o **piso do tempo da Fase 0**, mesmo
com os outros 14 blocos prontos e ociosos. Quem ganha com os 6 pedaços é o
`INSERT` da Fase 2, que passa a ter tarefas parelhas para distribuir. Se esse
piso incomodar, a saída é começar a Fase 2 dos blocos já prontos enquanto o zip 0
ainda descomprime — o que transforma as fases 0 e 2 num *pipeline*, não em duas
etapas, e muda o desenho do `load.sh`.

**Fatiar por linha é seguro — medido.** `split -l` só vale se nenhum campo tiver
`\n` embutido; um corte no meio de um registro corromperia os dois blocos
vizinhos. Verificado em 16/09/2026 sobre os 2 primeiros milhões de linhas de
`Estabelecimentos1`, `Empresas1` e `Socios1`: **zero** linhas com número ímpar de
aspas, ou seja, nenhum campo multi-linha (ver 2.5). O teste é barato e vale
repetir se a RFB mudar o formato:

```bash
unzip -p Estabelecimentos1.zip | head -n 2000000 \
  | awk -F'"' '{if((NF-1)%2==1) c++} END{print c+0}'   # espera-se 0
```

**A higienização do stream tem de vir junto.** Hoje todo COPY passa por
`tr -d '\000'` (`load.sh`, `copy_zips`): os bytes NUL que a Receita emite em
campos como `complemento` quebram o COPY com *"unterminated CSV quoted field"*.
Com o Postgres lendo o arquivo direto, ninguém remove esses bytes — a Fase 0
**não é `unzip -d`**, é `unzip -p <zip> | tr -d '\000' > bloco.csv`. O mesmo vale
para o `tr -d '\r'` e o `grep -vi '^ano,cnpj,...'` que o `copy_regime` aplica.
Consequência para a estimativa: a descompressão continua sendo um pipe com
filtro, não uma extração pura, e a Fase 0 escreve ~30 GB que o Postgres depois
relê — ~60 GB de I/O que hoje não existem (seção 4).

### Fase 1 — Schema sem unicidade

As tabelas nascem sem PK e sem constraint única, estendendo à unicidade a lógica
que o `04_indexes.sql` já aplica aos índices ("criar índices no fim é ordens de
magnitude mais rápido"). Cada bloco vira uma foreign table.

### Fase 2 — Escrita única

O `03_transform.sql` **continua o mesmo SQL**: `parse_date`, `nullif`,
`coalesce(uf,'??')`, o `unnest` do `cnae_secundario`. Muda apenas o `FROM`, de
`staging.x` para `staging.x_fdw_NN`. Nenhuma regra de transformação sai do SQL —
é isso que torna este desenho mais seguro que a alternativa com DuckDB.

**São cinco destinos, não quatro.** `estabelecimento_cnae_secundario` (121,7 mi
de linhas, 30,3 min — empatada com `estabelecimento` como a mais cara do
transform) sai do **mesmo CSV** de estabelecimentos, via o `unnest` da linha 104
do `03_transform.sql`. Ou seja: cada bloco de estabelecimentos é lido **duas
vezes**. As opções são ler duas vezes em sequência (dobra a leitura de ~30 GB) ou
rodar os dois `INSERT` em paralelo sobre o mesmo arquivo — e aí a leitura deixa
de ser sequencial, que é justamente o argumento da seção 5 a favor do CSV. No
servidor, com `shared_buffers` de 128 MB, duas varreduras concorrentes do mesmo
arquivo de 6,5 GB não se ajudam no cache. **Isto precisa ser medido junto com o
paralelismo** (seção 6, item 1): é a maior incerteza restante do desenho.

**Duplicatas (ver 2.6).** O `ON CONFLICT DO NOTHING` não pode simplesmente
sair: há duplicata real em três tabelas. E os blocos paralelos descartam um
mecanismo inteiro — **`DISTINCT ON` só deduplica dentro do bloco**; dois blocos
com a mesma chave passam os dois. Sobra dedupe local à linha, ou algo global. O
tratamento é por tabela:

| tabela | onde eliminar | por quê |
|---|---|---|
| `estabelecimento_cnae_secundario` | na origem: distinct dentro do array, antes do `unnest` | duplicata é local a uma linha; não cruza bloco e não exige sort global |
| `empresa` | mantém a PK viva + `ON CONFLICT`, com desempate explícito | único mecanismo que deduplica **entre blocos** sem sort global de 68 mi de linhas |
| `estabelecimento`, `simples` | nada durante o INSERT; `CREATE UNIQUE INDEX` da Fase 3 como detector | zero duplicatas medidas; são as tabelas grandes, onde índice vivo custa caro |
| `socio` | `DELETE` de linhas idênticas depois do INSERT | não há chave natural para arbitrar; a varredura inteira custa 26s |

Manter a PK de `empresa` viva responde à pergunta que a seção 8 deixa aberta. É
um btree simples, o índice mais barato de manter durante o INSERT (14,3s contra
2,8s por 1,5 mi, seção 2.4), e é a única tabela que precisa dele. Trocar o
`DO NOTHING` por uma regra que prefira a linha com `razao_social` preenchida
corrige, de quebra, o desempate por sorte que existe hoje.

> **Nota sobre o `ON CONFLICT DO UPDATE` de `empresa` com blocos paralelos.** O
> `DO NOTHING` de hoje não bloqueia; o `DO UPDATE` proposto pega *row lock* na
> linha em conflito. Dois blocos que toquem as mesmas duas chaves em ordens
> diferentes podem, em teoria, *deadlock*ar — o Postgres mata um dos dois, e com
> `set -e` isso derruba a carga na hora 3. A probabilidade é baixa (as duplicatas
> são unidades por mês), mas o custo é alto e a defesa é trivial: retry do bloco.

Paralelismo em dois eixos (entre tabelas e entre blocos), governado por
`LOAD_JOBS`. O valor ótimo aqui (16 vCPU ociosas, 23 GB) e no servidor não é o
mesmo — `LOAD_JOBS=1` reproduz o comportamento serial de hoje. **Medido em 2.8 e
orçado em 7.3: `LOAD_JOBS = 3` no servidor**, porque 4 jobs rendem 2,5× (não 4×)
e a quarta vCPU do orçamento é da descompressão da Fase 0.

### Fase 3 — Unicidade e índices

`CREATE UNIQUE INDEX` reconstrói as ex-PKs junto com os índices do
`04_indexes.sql` — **incluindo os três GIN trgm, que permanecem**: eles não são
usados pelas 7 rotas da API, mas há consumidores que acessam o banco direto.

Para `estabelecimento` e `simples` o `CREATE UNIQUE INDEX` acumula um segundo
papel: é o **detector** de duplicata. Se um mês trouxer uma, ele falha aqui.

> ⚠️ **A retomada NÃO resolve esse caso** — e uma versão anterior deste documento
> dizia que sim. Se o índice único falhou, é porque há duplicata **nos dados**:
> recriá-lo falha de novo, quantas vezes for. O que falta não é retentativa, é
> uma regra de tratamento — decidida na **seção 7.1**: índice não-único mais
> quarentena, a carga termina degradada em vez de abortar.

### Fase 4 — IBGE, regime, matviews

Sem mudança de conteúdo, mas **com mudança de ordem**: hoje o `load.sh` roda o
de-para IBGE **entre** o transform e os índices; no desenho ele cai depois. Sem
impacto funcional aparente (o `ibge_transform.sql` só toca `dim_municipio`, que
já está preenchida pelo transform), e provavelmente melhor — as matviews fazem
`GROUP BY` sobre `estabelecimento` e se beneficiam dos índices prontos. Fica
registrado para não passar como mudança silenciosa.

### Escopo — o que este desenho NÃO cobre

O `load.sh` tem quatro caminhos; o redesenho trata de **um**.

- **`SAMPLE` não sobrevive ao `file_fdw`.** A amostra coerente depende de
  `unzip -p | head -n` e de `rg -f <padrões>` **no meio do pipe**
  (`copy_zips_match`). Sem pipe não há onde filtrar: seria preciso materializar
  CSVs já filtrados em disco antes de criar as foreign tables — um caminho novo,
  que este desenho não prevê. Alternativa barata: **manter o modo amostra como
  está hoje** (`\copy` + staging), já que ele carrega ~20 k linhas e não tem
  problema de desempenho nenhum. O preço é conviver com dois caminhos de carga.
- **`REGIME_ONLY` e `IBGE_ONLY` continuam com `\copy` + staging.** São
  incrementais, pequenos e têm cadência própria.
- **Logo, o schema `staging` não desaparece.** Ele continua abrigando `tabmun`,
  `ibge_raw`, `ibge_municipios` e `regime_tributario` — e, na Fase 1, as próprias
  foreign tables. O que some são as 5 tabelas grandes (os 27 GB). A linha da
  seção 4 sobre perder o `KEEP_STAGING` como ferramenta de debug vale só para
  elas.

### Acoplamento novo: `file_fdw` é server-side

Hoje o `\copy` é **client-side**, e é por isso que o `load.sh` funciona nos dois
modos que ele suporta: `docker compose exec` a partir do host e `psql -h $PGHOST`
a partir do container do watcher. Funcionaria até contra um Postgres remoto.

Foreign table de `file_fdw` é **server-side**: o CSV precisa estar visível ao
processo do Postgres. Isso implica dois passos que a seção 4 não contabiliza:

1. Montar `CNPJ_HOST_DATA_DIR` (read-only) no serviço `postgres-cnpj-rfb`, que
   hoje monta apenas `.../postgres:/var/lib/postgresql`.
2. **Recriar o container do postgres no deploy** para a montagem valer. O
   `tuning-carga.md` já avisa que restart mata carga em andamento — aqui vira um
   passo de deploy com downtime da API, a ser coordenado com a janela das 22h do
   watcher.

### Retomada

Tabela de controle registrando cada bloco concluído; reexecutar pula o que já
entrou. Hoje, uma falha na hora 3 custa as 3 horas anteriores. Dois cuidados que
a retomada tem de resolver, e que não são detalhe de implementação:

- **O `TRUNCATE` do `03_transform.sql` não pode rodar na retomada** — ele apaga
  exatamente o que a tabela de controle diz que já entrou.
- **Falhar sem índice é pior que falhar com índice** — ver abaixo.

### Ciclo de vida dos CSVs — dois requisitos em tensão

O pico de "~30 GB temporários" da Fase 0 só se sustenta se os blocos forem
apagados **durante** a carga, conforme consumidos. Mas:

- **A retomada precisa dos blocos ainda não confirmados.** Apagar cedo demais
  transforma a retomada em recarga — que é exatamente o que ela existe para
  evitar.
- **Um bloco de estabelecimentos é consumido duas vezes**: por
  `estabelecimento` e por `estabelecimento_cnae_secundario`. "Consumido" não é um
  evento único.

Nenhum dos dois requisitos manda no outro sozinho; é decisão de spec. O meio
termo natural é apagar um bloco só quando **todos** os destinos que o leem o
tiverem confirmado na tabela de controle — o que mantém o pico abaixo dos 30 GB
sem quebrar a retomada, ao custo de a Fase 0 não poder simplesmente extrair tudo
de uma vez e esquecer. Depende do disco livre do servidor (seção 6, item 5): se
sobrar folga, a opção mais simples é **manter os 30 GB até o fim da carga** e
apagar tudo junto, como o `DROP SCHEMA staging` faz hoje.

### O risco novo: falhar entre a Fase 2 e a Fase 3

Este é o único ponto em que o desenho **piora** o estado atual, e ele é caro
porque `cnpj_full` é o banco que a **API serve**, o mesmo que é recarregado.

- **Hoje:** uma falha depois do `TRUNCATE` deixa a base parcial, **mas
  indexada** — as consultas continuam respondendo.
- **No desenho:** a Fase 3 é a última. Uma falha na Fase 2 — ou o
  `CREATE UNIQUE INDEX` reprovando por duplicata nova, que é o papel de detector
  que lhe foi dado — deixa 72 mi de linhas **sem PK e sem índice nenhum**. Seq
  scan nesse volume com `shared_buffers` de 128 MB é a API fora do ar na
  prática.
- **E a janela é longa:** o watcher não reagenda em falha; ele só retenta na
  verificação seguinte, `CHECK_INTERVAL_H=24`. Até 24 h sem índice.

Mitigação obrigatória, não opcional: o `trap EXIT` do `load.sh` (que hoje só
imprime o resumo de tempos) precisa de uma **fase de recuperação** — em qualquer
saída anormal após o drop dos índices, recriá-los antes de devolver o erro. Vale
igualmente para a ação isolada da seção 8, que tem o mesmo risco em escala menor.

E ela é a **única** coisa entre a falha e a API degradada: como a carga roda de
madrugada e não há intervenção manual de madrugada (seção 7.1), nada corrige o
estado até o expediente seguinte. O `trap` não é rede de segurança — é o
procedimento de recuperação em si.

---

## 4. Trade-offs

| o que se ganha | o que se paga |
|---|---|
| Elimina a releitura de 27 GB (o gargalo do servidor) | ~~~30 GB de CSV temporário em disco~~ **deixou de ser custo** — 267 GB livres (2.7) |
| −27 GB de pico no banco | Descompressão deixa de ser sobreposta ao COPY — e agora **disputa** o orçamento de 4 vCPU (7.3) |
| Blocos retomáveis, e paralelos valendo **2,5× no teto e ~2× dentro do orçamento** (2.8), não 4× | Complexidade nova no `load.sh` |
| Toda a lógica permanece em SQL | Dedupe deixa de ser uma cláusula só e passa a ser 4 tratamentos (2.6) |
| Sem dependência nova (`file_fdw` é nativo) | Os zips precisam ser montados no container do Postgres — e o container **recriado** no deploy |
| — | Perde o staging como ferramenta de debug (`KEEP_STAGING=1`) nas 5 tabelas grandes |
| — | Falha no meio custa mais: hoje COPY e transform são etapas separadas |
| — | A carga deixa de ser client-side: some o modo "psql contra um Postgres remoto" |
| — | +~60 GB de I/O na Fase 0 (escrever 30 GB e relê-los) que hoje não existem |
| — | `SAMPLE` não cabe no `file_fdw` — ou ganha um caminho novo, ou fica no `\copy` |
| — | Falha entre as Fases 2 e 3 deixa a API sem índice por até 24 h |

**Duas alavancas que este desenho não usa** (independentes dele, e talvez mais
baratas por unidade de ganho):

- **`wal_level = minimal`** (com `max_wal_senders = 0`). Como a tabela é
  truncada e recarregada por inteiro, o Postgres pode **pular o WAL do `INSERT`**
  quando a tabela é criada ou truncada na mesma transação. É a mesma ideia do
  `staging UNLOGGED` da seção 8, mas aplicada onde o dado **fica**, não no
  descartável. Custo: exige restart e elimina replicação/PITR durante a janela —
  no servidor, provavelmente aceitável.
  > ⚠️ **Mas ela briga com os blocos paralelos.** A otimização exige que o
  > `TRUNCATE` e a escrita estejam na **mesma transação**, e uma transação é de
  > uma sessão só: N blocos paralelos na mesma tabela são N transações, e
  > nenhuma delas se qualifica. `wal_level = minimal` e paralelismo por bloco
  > são, na prática, **alternativas** — o que também significa que a alavanca
  > está disponível hoje, sem redesenho nenhum, já que a carga atual é serial.
  > Precisa ser medida antes de escolher entre as duas.
  >
  > **E a 2.8 mudou o lado em que o peso cai.** O paralelismo entrega 2,5× no
  > teto da máquina de dev e ~2× dentro do orçamento de 4 vCPU da 7.3, gastando
  > CPU que é o recurso escasso no servidor compartilhado. `wal_level = minimal`
  > não *adiciona* concorrência, *remove trabalho* — não custa vCPU nenhuma e
  > cabe no orçamento por definição. Num host onde a restrição é CPU e memória,
  > e não disco, a alavanca que remove trabalho tende a ganhar da que adiciona
  > paralelismo.
- **Carregar em banco/schema paralelo e trocar no fim** (`RENAME`). Resolve de
  uma vez os três problemas de disponibilidade: a janela em que a API serve base
  parcial, a falha do `CREATE UNIQUE INDEX` e a retomada — o banco antigo segue
  servindo até a troca. Custo: uma segunda cópia do banco, que no servidor são
  **89 GB**, não os ~58 GB que este documento supunha.
  > ✅ **Deixou de ser hipótese.** A 2.7 mediu **267 GB livres** no servidor: ela
  > **cabe**. E é a alavanca que mais se alinha às duas decisões da seção 7 —
  > entrega sozinha o que a 7.1 teve de resolver com quarentena (a base servida
  > nunca fica sem índice, porque a que serve é a antiga até o `RENAME`) e não
  > custa **nada** do orçamento de CPU e memória da 7.3, só disco, que é o único
  > recurso sobrando. Merece ser comparada de frente com o redesenho por blocos,
  > não listada como alternativa secundária.

**Variante com `program`:** `file_fdw` pode ler da saída de um comando
(`unzip -p ...`), dispensando os 30 GB temporários e a Fase 0 inteira. Mas
`program` roda dentro do container do Postgres, que **não tem `unzip`** — exigiria
abandonar a imagem oficial e manter um Dockerfile próprio. É a escolha entre 30 GB
de disco e uma imagem a manter.

> Duas correções a esta variante, à luz da Fase 0: (a) ela **não** elimina a
> descompressão serial — só a move para dentro do backend do Postgres, onde ela
> passa a ocupar o processo que deveria estar inserindo; (b) ela **perde o
> balanceamento por completo**, porque não há como fatiar `Estabelecimentos0.zip`
> num `unzip -p`: sem arquivo intermediário não há onde aplicar o `split`, e o
> zip 0 volta a ser uma tarefa monolítica de 6,5 GB. O ganho de disco é real; o
> preço é o desenho de blocos.

---

## 5. Ganho esperado

| | hoje (dev) | novo (dev) | hoje (servidor) |
|---|---|---|---|
| COPY + transform | 2h15 | ~1h05 – 1h30 | ~3h50 |
| índices | 45 min | 45 min | ? |
| **total** | **3h05** | **~2h – 2h20** | **20h+** |

A tabela acima considera apenas o redesenho do caminho do dado. **A correção dos
índices (seção 2.4) é independente e acumulativa**: sozinha vale −37% na fase de
transform, sem mexer em arquitetura nenhuma.

A faixa era larga porque o paralelismo não estava medido. **Agora está (2.8), e
a borda otimista caiu.** O desenho contava com blocos escalando perto de 4×; o
medido é **2,5× no teto da máquina de dev** e, dentro do orçamento de 4 vCPU da
7.3 (`LOAD_JOBS = 3`, um core para a descompressão), **~2×**. A borda de ~2h
pressupunha o teto; o realista é a metade superior da faixa.

Das três incógnitas que sustentavam a borda otimista, **uma foi respondida e
duas seguem em medição**:

- ~~o paralelismo escalar apesar das 28 partições~~ — **respondido**: escala
  2,5×, e a partição não era o gargalo (2.8);
- a dupla leitura do CSV de estabelecimentos não custar caro (item 6);
- o piso serial do zip 0 na Fase 0 não dominar — e ele ficou **mais** pesado,
  porque a 7.3 tira a descompressão do "de graça" e a põe a disputar as mesmas
  4 vCPU.

Com o paralelismo valendo ~2× em vez de 4×, a comparação com a ação isolada da
seção 8 (dropar e recriar índices, **−37% medidos**, poucas linhas de SQL) fica
desconfortável para o redesenho: ela entrega uma fatia grande do ganho sem
`file_fdw`, sem blocos, sem CSV temporário e sem gastar CPU.

> 🛑 **E a 2.9 tirou o chão que restava.** O redesenho ainda se justificava pela
> escrita única — "o mesmo SQL, só muda o `FROM`". No volume completo esse `FROM`
> custa **86% a mais**: o caminho novo soma ~44m56s contra ~39m42s do atual. A
> tabela de ganho acima **está superada**; medida contra o que se sabe hoje, a
> troca do caminho do dado não se paga. O que continua de pé, e com os números
> mais fortes do documento, é a seção 8 (−37% em poucas linhas) e as duas
> alavancas da seção 4 que não custam CPU: `wal_level = minimal` e carregar em
> banco paralelo com `RENAME`.

No servidor o ganho relativo tende a ser **maior**, porque lá o transform pesa
muito mais — e a leitura de CSV é sequencial, enquanto a releitura do staging com
`shared_buffers` de 128 MB é aleatória e vai quase toda ao disco.

---

## 6. Antes de implementar

1. ~~**Medir o paralelismo.**~~ **Feito** (seção 2.8). 4 jobs rendem **2,5×**,
   não 4×, e a vazão por job cai um terço já na terceira conexão. A hipótese de
   que o teto era o *relation extension lock* das 28 partições **foi refutada**:
   4 jobs em 4 tabelas independentes ficaram mais lentos que na particionada, e
   fatiar por UF ficou mais lento ainda (SP é 28,7% do volume e vira o caminho
   crítico). O teto é global — WAL/I/O/CPU. A Fase 0 continua fatiando **por
   posição no arquivo**. Escolha sustentada: **`LOAD_JOBS = 3`**, dentro do
   orçamento de 4 vCPU da 7.3, com ~1 core sobrando para a descompressão.
   Continua aberto o ponto de memória: **`work_mem` é por operação**, não por
   conexão. Com `TUNE_RAM_GB=6` ele sai em ~92 MB, e `LOAD_JOBS=3` multiplica o
   pico num servidor com 5,8 GB disponíveis e **sem swap**, onde estourar não é
   lentidão, é OOM kill. `LOAD_JOBS` e `work_mem` têm de sair do orçamento
   (`TUNE_RAM_GB`), não de configuração solta.
2. ~~**Checar duplicatas nas outras tabelas.**~~ **Feito** (seção 2.6): há
   duplicata em `empresa`, `cnae_secundario` e `socio`; `simples`,
   `estabelecimento` e as dimensões estão limpos. O tratamento por tabela está
   na Fase 2. Falta decidir a regra de desempate de `empresa` — e ela não é só
   de `empresa`: é a mesma regra que a seção 7.1 precisa para poder deduplicar
   `estabelecimento`/`simples` automaticamente em vez de terminar degradado.
   A 7.2 subiu o item de desejável a **pré-requisito do paralelismo**: sem regra
   de desempate, "a primeira linha do arquivo vence" deixa de ser determinístico
   quando os blocos correm em paralelo — a menos que `empresa` fique num bloco
   sequencial só dela.
3. ~~**`file_fdw` na escala completa.**~~ **Feito** (seção 2.9), e o resultado
   **inverteu o sinal**: no volume completo o `INSERT` a partir de `file_fdw` é
   **86% mais lento** que a partir do heap da staging (32m20s contra 17m23s). O
   −34% medido com 1 zip não sobreviveu à escala. Não é falta de paralelismo (os
   dois planos são seriais); é o custo de parsear CSV. Consequência: a escrita
   única paga mais na leitura do que economiza na escrita.
4. **Validar no servidor, não só aqui.** Os perfis são opostos: lá o COPY é 6×
   mais rápido e o INSERT 2,3× mais lento. Uma otimização que ajuda numa máquina
   pode ser irrelevante na outra.
5. ~~**Coletar dois números do servidor que ainda faltam.**~~ **Feito**
   (16/09/2026, leitura por SSH — números na seção 2.7). Os dois vieram, e um
   terceiro apareceu junto que muda mais coisa que os dois:
   - **8 vCPU** (Xeon Gold 6542Y, 4 núcleos físicos com HT) — metade da máquina de
     desenvolvimento. É o teto do paralelismo da Fase 2 **somado** à descompressão
     da Fase 0, que deixa de ficar sobreposta ao COPY e passa a disputar CPU.
   - **267 GB livres** em `/` (451 GB, 39% usados). Disco **não** é restrição: os
     ~30 GB de CSV temporário cabem com folga — e a variante "carregar em banco
     paralelo e trocar" (seção 4) também, que precisa de ~89 GB.
   - **O terceiro número: o servidor é compartilhado.** 30 containers de pé
     (Airflow com 4 workers, Kong, Traefik, Memgraph, Vaultwarden, dois Postgres
     de controladoria). `load average` de **2,0–2,4**, e 10 dos 16 GB de RAM já
     alocados. **Correção (2.11):** havia carga do CNPJ rodando durante a coleta,
     então esse número **não** é a linha de base dos outros serviços — ela é mais
     baixa, e falta medir com o host ocioso.
6. ~~**Medir a dupla leitura do CSV de estabelecimentos**~~ **Feito** (seção
   2.9): em paralelo é **22% mais barato** que em sequência (320 s contra 409 s)
   — o segundo leitor acha no cache o que o primeiro trouxe. A Fase 2 dispara os
   dois consumidores juntos, e eles ocupam 2 das 4 vCPU do orçamento da 7.3.
   Continua não medido **no servidor**, onde o cache é de 128 MB.

---

## 7. Decisões pendentes — o que bloqueia a spec

A seção 6 lista o que falta **medir**. Esta lista o que falta **decidir**: nenhum
dos dois itens abaixo se resolve com mais medição, e sem eles não dá para escrever
a spec nem os testes.

### 7.1 O que fazer quando o `CREATE UNIQUE INDEX` reprovar

O desenho deu ao índice único o papel de detector de duplicata em
`estabelecimento` e `simples` (Fase 3), e a seção 2.6 diz que o evento é
esperado — "é um mês; pode ser 0 ou 50 na próxima carga". Falta a resposta: o
índice falha às 3h da manhã, depois de horas de INSERT, com a API já sem índice
nenhum.

**Restrição que elimina metade das opções:** a carga roda de madrugada e
**não há ninguém para intervir de madrugada**. Qualquer caminho cujo passo
seguinte seja "alguém investiga" deixa a API sem índice até o expediente
seguinte — na prática, fora do ar por horas, sobre 72 mi de linhas com
`shared_buffers` de 128 MB. Isso descarta "falhar e alertar" como caminho
primário, e com ele a recomendação anterior deste documento.

O que sobra tem de valer como regra: **a carga termina sozinha, sempre, com a
base indexada e servindo.** O que ela não consegue decidir sozinha vira
_relatório_, não _parada_.

| caminho | custo | risco |
|---|---|---|
| **Falhar e alertar** — o `trap` recria os índices não-únicos, a carga aborta | exige intervenção manual de madrugada — **inviável** | a API fica degradada até alguém agir, e nada do mês anterior sobrou para servir (o `TRUNCATE` já passou) |
| **Índice não-único + quarentena** — detecta, registra as chaves, cria o índice **sem** `UNIQUE`, termina a carga e alerta | uma varredura extra por tabela (2–3 min aqui; mais no servidor) e um índice que não garante unicidade naquele mês | a base fica **sem a restrição de unicidade**, com a duplicata dentro, até alguém agir — mas **indexada e respondendo** |
| **Deduplicar e repetir** — `DELETE` por `ctid` mantendo a primeira ocorrência, depois `CREATE UNIQUE INDEX` de novo | uma varredura extra por tabela | apaga dado em silêncio; sem regra de desempate, pode apagar a linha boa (é o problema que 2.6 descreve em `empresa`) |
| **Promover as duas ao tratamento de `empresa`** — PK viva + `ON CONFLICT` durante o INSERT | o índice vivo nas **tabelas grandes**, que 2.4 mostra ser o custo mais caro do desenho | contradiz a razão de ser do redesenho |

**Recomendação: índice não-único + quarentena.** Concretamente, a Fase 3 para
`estabelecimento` e `simples` deixa de ser um `CREATE UNIQUE INDEX` seco e passa
a ser:

1. varredura da chave (`GROUP BY … HAVING count(*) > 1`), gravando chave,
   `ctid`s e a competência da carga numa tabela de quarentena
   (`analytics.duplicata_carga`);
2. **sem duplicata** — `CREATE UNIQUE INDEX`, caminho feliz, custo = a varredura;
3. **com duplicata** — `CREATE INDEX` (mesmas colunas, sem `UNIQUE`), a carga
   **termina com sucesso degradado**: a API serve com o mesmo plano de acesso, o
   alerta sai, e a quarentena diz exatamente quais chaves olhar no expediente
   seguinte.

Três consequências que a spec precisa carregar:

- **O código de saída deixa de ser binário.** Precisa de um terceiro estado
  ("carga concluída, unicidade pendente") que alerta sem fazer o watcher tratar
  como falha e reagendar uma recarga de 3 h por causa de 1 linha duplicada.
- **O nome do índice não pode depender da unicidade.** A carga seguinte tem de
  conseguir dropar e recriar o índice sem saber em que modo o mês anterior
  terminou; se o mês novo estiver limpo, ele volta a ser `UNIQUE` sozinho.
- **A quarentena é acumulativa.** Uma chave que aparece duplicada em dois meses
  seguidos é a evidência que a seção 6, item 2 pede para escrever a regra de
  desempate — e é o que permite promover o caminho 3 (dedupe automático) depois.

O `DELETE` por `ctid` continua documentado como ferramenta **manual**, usada de
dia sobre o que a quarentena registrou. O caminho 3 automatizado só é aceitável
depois que existir a regra de desempate de `empresa` (seção 6, item 2) — a mesma
regra serve às três tabelas, e sem ela "manter a primeira" é a sorte que 2.6 já
critica.

Rejeitado: detectar a duplicata **antes** do `TRUNCATE`, direto nos CSVs via
`file_fdw`. Resolveria melhor (a base do mês anterior continuaria servindo
intacta), mas custa uma varredura completa dos ~27 GB antes de a carga começar,
no servidor onde a releitura de 27 GB é justamente o gargalo que o redesenho
existe para eliminar.

### 7.2 Critério de aceite — como se prova que a base nova é igual à antiga

O documento inteiro fala de **tempo** e não define **correção**. Sem um contrato
de equivalência não há como escrever teste antes da implementação, e o redesenho
troca o caminho do dado inteiro — é exatamente onde um erro silencioso cabe.

**A definição.** A carga nova é aceita quando, para a mesma entrada, produz o
mesmo **conteúdo lógico** que a carga atual em todas as tabelas de `analytics`.
"Conteúdo lógico" é o dump de cada tabela **ordenado pela chave natural**, com as
colunas voláteis removidas. Três exclusões, e elas fazem parte do contrato:

- **`socio.id`** é `generated`/serial — o valor depende da ordem de inserção, que
  o paralelismo por blocos muda de propósito. Compara-se `socio` **sem o `id`**,
  ordenado pelas 11 colunas restantes.
- **A ordem física (heap)** não é contrato. Nenhuma comparação pode depender de
  `ctid` ou da ordem natural de um `SELECT *` sem `ORDER BY`.
- **Divergência intencional é exceção nomeada**, declarada no teste antes de
  rodar — não descoberta na comparação. Concretamente: se a dedupe de
  `analytics.socio` (seção 8) entrar junto, a base nova legitimamente tem 22
  linhas a menos, e isso é um item da lista de exceções, não uma falha.

#### Nível 1 — fixture sintética (segundos; é este que se escreve antes do código)

Um conjunto de CSVs fabricados, versionado no repo, com poucas dezenas de linhas
escolhidas para acertar **cada** transformação do `03_transform.sql`:

| caso | o que pega |
|---|---|
| `08314885` com as duas linhas da seção 2.6, na ordem do arquivo | o desempate de `empresa` |
| `cnae_secundaria` com o mesmo código repetido 6× | o `unnest` + descarte |
| estabelecimento com `uf` vazia e com `uf` só de espaços | `coalesce(nullif(btrim(uf),''),'??')` e a partição `DEFAULT` |
| capital `1.234,56`, `0`, vazio | `replace(',', '.')` + `nullif` |
| datas `00000000`, `0`, `20200231` (inexistente), válida | `analytics.parse_date` |
| `cnpj_basico` com 7 dígitos e com letra | o filtro `~ '^\d{8}$'` (que no dump medido não reprova nenhuma linha real — ver a correção na 2.5) |
| `ddd_1` vazio com `telefone_1` preenchido, e o inverso | o `btrim(ddd_1 \|\| telefone_1)` |
| dimensão com `codigo` não numérico | o filtro `~ '^\d+$'` das seis dimensões |
| sócio estrangeiro duplicado idêntico (padrão da 2.6) | o caso sem chave natural |

O teste roda **os dois caminhos** sobre os mesmos CSVs, no mesmo container, e
compara tabela a tabela via hash do dump ordenado:

```sql
SELECT md5(string_agg(t::text, E'\n' ORDER BY t::text)) FROM analytics.empresa t;
```

Igualdade é **exata**. Este é o teste do TDD: roda em segundos, não precisa de
dado da Receita e quebra em qualquer divergência de cast, regex ou filtro.

#### Nível 2 — amostra coerente (minutos)

`SAMPLE=200000` nos dois caminhos, mesmo zip, mesmo mês, dois bancos na mesma
máquina (`cnpj_old` / `cnpj_new`). Mesma comparação por hash do nível 1, agora
sobre dado real — pega o que a fixture não teve imaginação de inventar. Duas
ressalvas do modo amostra, que a spec precisa saber: ele não carrega **regime**
(staging vazia) e o recorte é `head -N` de **um** zip, então casos raros
(partição `??`, duplicata de `empresa`) podem não aparecer — por isso o nível 1
existe e não é substituível por este.

#### Nível 3 — volume total, uma vez, antes de trocar

Carga completa pelos dois caminhos, do mesmo mês. O hash do nível 1 continua
valendo e é o critério principal; as agregações abaixo ficam como **diagnóstico**
— quando o hash diverge, são elas que dizem onde:

- contagem de linhas por tabela, idêntica nas cinco;
- contagem por `uf` em `estabelecimento` (roteamento de partição e o `'??'`);
- `sum(capital_social)` e `count(*) FILTER (WHERE capital_social IS NULL)`;
- `min`/`max`/`count` das colunas `date`;
- contagem de `estabelecimento_cnae_secundario` por `cnae_cod`.

Mais o teste de ponta a ponta, que é o que o usuário enxerga: as **7 rotas** da
API (`/healthz`, `/stats/capital-por-natureza`, `/stats/empresas`,
`/stats/regime`, `/empresas/{cnpj}`, `/filial/{cnpj}`, `/socios`) respondendo
**byte a byte** o mesmo, apontadas ora para `cnpj_old` ora para `cnpj_new`. O
conjunto de entrada é fixo e versionado — CNPJs e querystrings num arquivo, não
escolhidos na hora — e inclui obrigatoriamente um com CNAE secundário repetido na
origem, um sem UF (partição `??`) e o `08314885` da seção 2.6. Comparação em
bytes crus: se a ordem de um array de CNAEs mudar, é divergência, não detalhe de
formatação.

#### O achado que este contrato expõe: `ON CONFLICT DO NOTHING` + blocos paralelos

Escrever o contrato revelou um problema que o desenho ainda não tratava. Hoje o
vencedor de um `ON CONFLICT DO NOTHING` é "a primeira linha **do arquivo**" — a
2.6 mostra que em `empresa` isso é a linha boa, por sorte. Com **blocos
carregados em paralelo**, "primeira" deixa de ser definido: quem chega primeiro
depende do escalonamento, e duas execuções sobre a mesma entrada podem produzir
bases diferentes. Consequências:

- o nível 1 do contrato passaria a ser **flaky**, e um teste que falha
  aleatoriamente é descartado pela equipe em duas semanas;
- pior que o teste: o **dado** vira sorteio mensal entre a linha boa e o lixo.

Duas saídas, e a spec tem de escolher: ou **`empresa` é carregada por um único
bloco sequencial** (é a menor das cinco, e a 2.1 mostra que ela não é o gargalo),
ou a regra de desempate da **seção 6, item 2** deixa de ser desejável e passa a
ser **pré-requisito** do paralelismo. As duas resolvem; a segunda é a certa a
longo prazo, a primeira destrava a implementação agora.

#### Quando o redesenho é aceito

Níveis 1, 2 e 3 verdes, quarentena (7.1) vazia na carga de comparação, e a lista
de exceções nomeadas revisada item a item. Enquanto o nível 1 não existir e
passar **contra a carga atual** — provando que o contrato descreve o
comportamento de hoje, e não o que se imagina dele —, o redesenho não deveria
começar a ser implementado.

---

### 7.3 Orçamento de recursos — a carga não pode lotar o servidor

A seção 2.7 mostrou que o host é **compartilhado**: 30 containers, `load average`
de 2,0–2,4 em repouso, 10 dos 16 GB de RAM já alocados e **sem swap**. Decidido
em 16/09/2026: **a carga não pode ocupar toda a CPU nem toda a memória** — nem no
pico. Ela é um trabalho de madrugada num servidor que continua servindo Airflow,
Kong, Traefik e duas APIs de controladoria, e a vizinhança não pode perceber a
carga rodando.

Isso reposiciona o paralelismo. Ele deixa de ser "quanto escala" e vira **"quanto
cabe no orçamento"** — a medição do item 1 da seção 6 continua valendo, mas para
escolher um ponto dentro do teto, não para achar o teto.

**O orçamento, em números, derivado da 2.7:**

| recurso | total | já comprometido | orçamento da carga | folga deixada |
|---|---|---|---|---|
| vCPU | 8 | ~2,2 (load medido — **com carga rodando**, ver 2.11) | **≤ 4** | ≥ 1,8 |
| RAM | 16 GB (5,8 disponíveis) | 10,2 GB | **≤ 3 GB** | ~2,8 GB |
| disco | 267 GB livres | — | ~30 GB de CSV (+89 GB se houver banco paralelo) | > 140 GB |

Consequências que a spec tem de carregar:

- **`LOAD_JOBS` + descompressão ≤ 4 vCPU, somados.** Na Fase 0 o `unzip` deixa de
  estar sobreposto ao COPY e passa a **concorrer** com o INSERT: um stream de
  descompressão come perto de um core. Logo `LOAD_JOBS = 3` com um `unzip`, ou
  `2 + 2`. Nunca "um job por core".
- **`TUNE_RAM_GB` é teto, não alvo.** O orçamento inteiro da carga tem de caber
  nele: `shared_buffers` + `work_mem × conexões ativas` (e `work_mem` é por
  **operação**, não por conexão) + `maintenance_work_mem × workers de índice`.
  Com o `shared_buffers` indo a 1 GB (seção 8), sobram ~2 GB para o resto — o que
  empurra `work_mem` para baixo e `max_parallel_maintenance_workers` para 1.
- **Degradar, nunca abortar.** Se no início da carga o host já estiver acima do
  orçamento, o caminho é **reduzir `LOAD_JOBS`** — até 1, sequencial — e seguir.
  Abortar contradiz a 7.1: não há ninguém de madrugada para reagendar.
- **A Fase 0 anda de `nice`/`ionice`.** Descompressão e escrita dos CSVs são o
  trabalho mais fácil de ceder: são I/O e CPU sem transação aberta, e atrasá-los
  não segura lock nenhum.

**O que NÃO fazer, e a razão:** pôr `mem_limit` no container do Postgres. O
limite de cgroup transforma estouro em OOM kill **dentro** do container — mata o
Postgres que a API serve, que é exatamente o desfecho que a 7.1 existe para
evitar. O teto de memória tem de vir da **configuração do Postgres** (que degrada
para disco quando não cabe), não do cgroup. Para CPU o raciocínio se inverte:
`cpus:` só atrasa, não mata, e é a forma honesta de garantir o teto de 4 vCPU.

Fica em aberto uma verificação, não uma decisão: medir o pico real de RSS do
Postgres durante uma carga com `LOAD_JOBS` no orçamento. Sem swap, "achamos que
cabe" é a diferença entre lentidão e OOM.

---

## 8. Fazer primeiro — independente deste redesenho

**1. Dropar os índices antes do transform e recriá-los depois** (seções 2.4 e
**2.11**). Deixou de ser "a ação de maior retorno por esforço" e passou a ser **a
única que ataca o gargalo real**: a 2.11 mediu **15h59m** no `INSERT` de
`estabelecimento` no servidor, esperando em `DataFileRead`, com **212 índices e
14 GB** mantidos vivos através de um `shared_buffers` de **128 MB**. São 16 horas
de uma carga de ~20 — o resto do documento disputa as outras 4. Resolvida com
poucas linhas no `03_transform.sql`/`load.sh` — sem `file_fdw`, sem blocos, sem
tocar em `ON CONFLICT`. Os **−37% medidos** em `analytics.empresa` (3 índices)
são o piso; em `estabelecimento`, com 212, a ordem de grandeza é outra. Faz toda carga se comportar como a primeira, que é o que o
`04_indexes.sql` já pretende. Os −37% são o piso: foram medidos em
`analytics.empresa`, que tem **3** índices; `estabelecimento` tem **6** mais as
28 partições, e lá o ganho tende a ser maior.

**Ponto a resolver na spec, e ele não é opcional:** o que fazer se a carga falhar
depois do drop e antes da recriação — a base fica sem índices **servindo a API**,
e o watcher só retenta 24 h depois (`CHECK_INTERVAL_H=24`). A resposta é a mesma
do desenho grande: o `trap EXIT` do `load.sh`, que hoje só imprime o resumo de
tempos, precisa recriar os índices em qualquer saída anormal antes de propagar o
erro. Esta ação e essa mitigação entram **juntas**, não em sequência.

Quanto a manter os índices
da PK durante a carga, a seção 2.6 responde: **sim para `empresa`** (é a única
tabela com duplicata recorrente, e o `ON CONFLICT` precisa de um índice para
arbitrar), **não para as demais**.

Depois dela, quatro ações menores, também sustentadas pelos dados:

- **Deduplicar `analytics.socio`.** 22 linhas 100% idênticas estão no banco hoje
  (2.6), porque a tabela só tem PK sintética e nenhum `ON CONFLICT`. Um `DELETE`
  ao fim do `03_transform.sql` resolve, e a varredura completa custa 26s. É o
  único caso em que o banco **já** contém duplicata.
- **`shared_buffers` do servidor: 128 MB → 1 GB.** Nunca foi configurado (o
  `load.sh` não mexe nele porque exige restart). É o parâmetro que o
  `tuning-carga.md` chama de "o grande knob" e o mais provável responsável pela
  lentidão do transform lá.
- **`TUNE_RAM_GB`: 6 → 4** no servidor. Com 5,7 GB livres e **sem swap**, um pico
  de 3,6 GB somado ao `shared_buffers` não deixa margem — e sem swap, a falta de
  memória é OOM kill, não lentidão.
- **`staging` `UNLOGGED`**, enquanto o staging existir: corta o WAL de 27 GB de
  dados descartáveis. Uma palavra por tabela no `02_staging.sql`.
