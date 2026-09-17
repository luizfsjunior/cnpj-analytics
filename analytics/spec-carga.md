# Spec da carga — v2

Especificação para reescrever o fluxo de carga (`load.sh` + `03_transform.sql`).
Escrita em 16/09/2026, a partir das medições das seções 2.7 a 2.11 do
[`redesenho-carga.md`](redesenho-carga.md) e das decisões da seção 7 de lá.

Documento de **contrato**, não de implementação: diz o que tem de ser verdade ao
final, o que é proibido, e como se prova cada ponto. Nenhuma linha de código deve
ser escrita antes de os testes da seção 6 existirem e **passarem contra a carga
atual** — é o que garante que o contrato descreve o comportamento de hoje, e não
o que se imagina dele.

---

## 1. Os três requisitos, como invariantes

### R1 — A estrutura final do banco não muda

Ao fim de uma carga bem-sucedida, o schema `analytics` tem de ser **idêntico** ao
que o par `01_schema.sql` + `04_indexes.sql` produz hoje: mesmas tabelas, colunas,
tipos, `NOT NULL`, partições, índices (nome, tipo, colunas, `fillfactor`),
constraints, funções e matviews.

Não é "equivalente" nem "compatível": é **idêntico**, porque a API e os
consumidores que acessam o banco direto não podem perceber a troca.

> **Como se prova:** `pg_dump --schema-only --schema=analytics` antes e depois,
> normalizado (ordenação estável, sem comentários de versão) e comparado por
> hash. Diferença de uma linha reprova. É o teste T1 da seção 6.

Corolário que restringe o desenho: **nada de tabela de controle, coluna de
rastreio ou índice auxiliar dentro de `analytics`**. O que a carga precisa
guardar sobre si mesma mora no schema `carga` (R2), que a API não enxerga e que
não entra na comparação do T1.

### R2 — Sanitização explícita, com rejeito contabilizado

Hoje a sanitização existe, mas é **implícita e silenciosa**: `nullif`, `btrim`,
`parse_date`, filtros `~ '^\d{8}$'` e `ON CONFLICT DO NOTHING` espalhados pelo
`03_transform.sql`. O que quer que ela descarte, ninguém fica sabendo — e o
problema não é o tamanho do descarte, é não haver como responder à pergunta.

> ⚠️ **As "26.442 linhas descartadas" não existem — erro de medição, corrigido
> em 16/09/2026.** Esta seção afirmava que a carga de 2026-09 havia descartado
> 26.442 linhas em silêncio, com base na 2.5 do redesenho ("o staging tem
> 71.900.890 linhas, 71.874.448 passam no filtro"). A carga completa de ensaio
> mostrou que o primeiro número está errado, e a verificação é direta:
>
> | medida | valor |
> |---|---|
> | linhas físicas nos 10 `Estabelecimentos*.zip` | 71.874.455 |
> | `staging.estabelecimentos` no `cnpj_bench` (a base da 2.5) | **71.874.448** |
> | `staging.estabelecimentos` no ensaio da v2 | **71.874.448** |
> | das quais passam em `^\d{8}$` | **71.874.448 — todas** |
>
> As quatro stagings do ensaio batem **exatamente** com as do `cnpj_bench`, e em
> todas as quatro o filtro descarta **zero** linhas. Não houve perda no COPY: os
> dois caminhos leem o mesmo. O 71.900.890 não corresponde a nada mensurável na
> máquina.
>
> **O que isso muda, e o que não muda.** Não muda a R2: contabilizar o descarte
> continua certo, e é o que permitiu *provar* que ele é zero em vez de supor.
> Muda o critério de aceite da seção 7, que pedia rejeito "na ordem das 26.442" e
> tratava zero como sinal de sanitização quebrada — era o contrário.

A v2 mantém **exatamente as mesmas regras** — R1 e o contrato de equivalência
proíbem mudar o resultado — mas torna cada descarte **nomeado e contado**:

| # | regra | onde se aplica | hoje | rejeito vai para |
|---|---|---|---|---|
| S1 | remover bytes `NUL` | todos os CSVs | `tr -d '\000'` | — (correção, não descarte) |
| S2 | decodificar `LATIN9` | todos os CSVs | `ENCODING` do COPY | — |
| S3 | `btrim` + vazio → `NULL` | todo campo texto | implícito | — |
| S4 | `cnpj_basico` casa `^\d{8}$` | empresa, estabelecimento, simples, socio | filtro silencioso | `carga.rejeito` |
| S5 | código de dimensão casa `^\d+$` | as 6 dimensões | filtro silencioso | `carga.rejeito` |
| S6 | data sentinela (`0`, `00000000`) **ou impossível** → `NULL` | colunas `date` | sentinelas só; impossível **mata a carga** | `carga.rejeito` |
| S6b | data fora de `^\d{8}$` → `NULL` **(desvio nomeado, ver abaixo)** | colunas `date` | `to_date` leniente devolvia uma data | `carga.rejeito` |
| S7 | `capital_social`: vírgula → ponto, vazio → `NULL` | empresa | implícito | contador |
| S8 | `uf` vazia ou desconhecida → `'??'` (partição DEFAULT) | estabelecimento | `coalesce` | contador |
| S9 | CNAE vindo do `unnest` casa `^\d+$` | cnae_secundario | filtro silencioso | `carga.rejeito` |
| S10 | duplicata de chave natural | ver seção 3 | `ON CONFLICT DO NOTHING` | `carga.duplicata` |
| S11 | **todo cast é total**: valor que não converte vira `NULL` | todo `::smallint`, `::integer`, `::numeric`, `::date` | **mata a carga** | `carga.rejeito` |
| S12 | valor mais longo que a coluna vira `NULL` | `char(8)`, `char(14)`, `char(2)`, `varchar(14)` | **mata a carga** | `carga.rejeito` |

**O contrato do rejeito:** toda linha descartada por S4, S5 ou S9 é gravada em
`carga.rejeito(competencia, tabela, regra, linha_bruta, detectado_em)`, e toda
regra produz um contador por carga em `carga.resumo`. Uma carga que rejeite mais
que um limiar configurável (default: **0,1%** das linhas de uma tabela) termina
com **aviso** — nunca aborta (7.1).

> **Por que isso não viola R1:** `carga` é um schema separado, não referenciado
> pela API nem pelo `analytics`. Se alguém o descartar, a carga continua correta;
> perde-se só a auditoria.

#### A exceção nomeada da S6b — decidida em 16/09/2026

A pré-validação exigida pela R2.1 precisa de um formato fixo: para saber se
`31/02` existe, é preciso primeiro saber onde estão o mês e o dia. O `to_date`
antigo não tinha essa restrição — ele era **leniente** e devolvia uma data para
entradas como `'2020-01-01'` ou `'202001'`, sem estourar.

**Na v2, fora de `^\d{8}$` e das sentinelas, a saída é `NULL` mais um rejeito S6
contado.** É o único desvio de conteúdo que a v2 introduz em relação a uma carga
que hoje termina com sucesso, e ele está aqui declarado em vez de escondido.

Por que é aceitável: nenhuma linha da fixture cai neste caso, o golden do T2 não
muda, e o layout publicado pela Receita é AAAAMMDD em todas as colunas de data.
Se um dia ele mudar, o rejeito contado avisa — que é exatamente o
comportamento que a R2 existe para produzir, no lugar da adivinhação silenciosa
de antes. O teste `test_s6_data_fora_do_formato_aaaammdd_vira_null` fixa a
decisão.

#### O custo da auditoria, medido em varreduras

Capturar `linha_bruta` custa **uma varredura sequencial a mais da staging por
tabela** — em `estabelecimento` são os ~27 GB lidos de novo. A varredura é uma
só e cobre S4, S6, S11 e S12 juntas, com filtro barato e, no mês normal, zero
linhas de saída.

Foi uma escolha, não um descuido: a alternativa era gravar só contadores no
caminho crítico, e aí a maior tabela continuaria sem resposta para "o que eram
essas linhas" — que é a pergunta que a R2 existe para responder. O custo é de
minutos contra as 16 horas que a Fase 2 ataca.

#### R2.1 — Nenhuma linha pode derrubar a carga (S11 e S12)

Esta é a regra que dá nome ao requisito de robustez, e ela nasceu de uma
descoberta: **hoje, uma única célula malformada em 73 milhões de linhas mata uma
carga de 20 horas** — às 3 da manhã, sem ninguém para reagir (7.1). Medido em
16/09/2026, direto no Postgres 18 do projeto:

| entrada | o que acontece hoje | onde |
|---|---|---|
| `'20200231'` (data impossível) | `ERROR: date/time field value out of range` | `parse_date`, todas as colunas `date` |
| `'1.234,56'` (capital com separador de milhar) | `ERROR: invalid input syntax for type numeric: "1.234.56"` | `empresa.capital_social` |
| `'99999'` em porte/natureza/qualificação | `ERROR: value "99999" is out of range for type smallint` | 11 colunas `::smallint` |
| `'99999999999'` em cnae/município | `ERROR: value ... is out of range for type integer` | 4 colunas `::integer` |
| `cnpj_ordem` com 5 dígitos | `ERROR: value too long for type character(14)` | a concatenação do CNPJ |

Nenhuma dessas é hipotética no sentido que importa: todas dependem do que a
Receita publicar no mês que vem, e o layout dela já mudou antes.

**A regra:** todo cast e toda atribuição a coluna de tamanho fixo tem de ser
**total** — para qualquer entrada existe saída, e a saída ruim é `NULL` mais um
rejeito contado, nunca uma exceção.

> ⚠️ **Como NÃO implementar isso:** função PL/pgSQL com bloco `EXCEPTION`. Cada
> bloco `EXCEPTION` abre uma **subtransação**, e uma subtransação por linha em 73
> milhões de linhas é ordem de magnitude pior que o problema que se quer
> resolver. A implementação tem de ser **pré-validação em SQL puro** — regex e
> comparação de faixa antes do cast — mantendo as funções `IMMUTABLE` e
> `PARALLEL SAFE`, que é o que permite os blocos da Fase 3.
>
> Para `parse_date` isso significa validar ano, mês e dia (incluindo dias do mês
> e ano bissexto) **antes** de chamar `to_date`, porque o `to_date` estoura
> sozinho e não há como perguntar a ele se a data existe.

**Por que isso é seguro pelo contrato de equivalência (7.2):** nenhuma dessas
mudanças altera o resultado de uma carga que hoje **termina com sucesso**. Se um
mês tivesse um desses valores, não haveria carga bem-sucedida para comparar — ela
teria morrido inteira. O que era falha catastrófica passa a ser rejeito
contabilizado, e nada mais muda.

#### R2.2 — Falhar cedo quando não dá para sanitizar

Nem tudo é sanitizável. Se a Receita mudar o número de colunas de um arquivo, não
há regra que salve a linha — e o `COPY` vai falhar de qualquer forma. O que a v2
controla é **quando**: conferir a contagem de colunas do CSV na Fase 1, antes do
COPY grande, em vez de descobrir na hora 16. Falhar em 2 minutos é recuperável;
falhar na hora 16 queima a janela inteira.

### R3 — Carga eficiente dentro dos recursos do servidor

Orçamento da seção 7.3, com a correção da 2.11 (o `load average` de 2,0–2,4 foi
medido **com carga rodando**; a linha de base real é menor):

| recurso | teto da carga | folga mínima deixada ao host |
|---|---|---|
| vCPU | **4** de 8 | 4 |
| RAM | **3 GB** de 16 (5,8 GB disponíveis, **sem swap**) | ~2,8 GB |
| disco | sem restrição (267 GB livres) | — |

E a prioridade vem da 2.11, não do desenho antigo: **16 das ~20 horas da carga
estão na manutenção de índice de uma tabela só.** A v2 ataca isso primeiro, e o
resto depois.

---

## 2. O fluxo da v2

```
Fase 0   pré-voo: recursos, orçamento, baseline
Fase 1   COPY  zips -> staging (UNLOGGED, client-side)
Fase 2   DROP dos índices de analytics (DDL salva antes)
Fase 3   transform staging -> analytics, com sanitização explícita
Fase 4   CREATE dos índices + ANALYZE
Fase 5   IBGE, regime, matviews
sempre   trap: saída anormal depois da Fase 2 recria os índices antes de morrer
```

### Fase 0 — pré-voo

1. Ler `nproc`, memória disponível, `load average` e disco livre.
2. Derivar `LOAD_JOBS` do orçamento: `min(3, vCPU_livres − 1)`, nunca acima de 3
   (2.8: 4 jobs rendem 2,5×, e o quarto core é da descompressão).
3. Derivar `work_mem` e `maintenance_work_mem` do teto de 3 GB, lembrando que
   **`work_mem` é por operação, não por conexão**.
4. Se o host já estiver acima do orçamento: **degradar** `LOAD_JOBS` até 1 e
   seguir. Nunca abortar (7.1) — não há ninguém de madrugada.
5. Registrar o baseline em `carga.resumo`: início, versão do código, parâmetros
   derivados, recursos observados.

### Fase 1 — COPY para a staging

Sem mudança de mecanismo: `unzip -p | tr -d '\000' | \copy`, **client-side**.

> **Decisão registrada: `file_fdw` está fora.** A 2.9 mediu o `INSERT` a partir de
> foreign table **86% mais lento** que a partir do heap no volume completo
> (32m20s contra 17m23s), e a 2.10 mostrou que no servidor a troca é líquido
> negativo — economiza ~7 min de COPY e paga ≥15 min de parse. O caminho do dado
> **não muda**.

Única mudança: as tabelas de staging viram **`UNLOGGED`**. Corta o WAL de 27 GB
de dados descartáveis, e o risco é nulo — se o servidor cair no meio, a carga
recomeça de qualquer forma.

### Fase 2 — dropar os índices (a fase que existe por causa da 2.11)

Antes do transform, para cada tabela de `analytics`:

1. Salvar o DDL de **todos** os índices em `carga.indice_salvo`, via
   `pg_get_indexdef`, incluindo os das 28 partições.
2. **Dropar** os índices secundários.
3. **Manter** as PKs que o transform usa para arbitrar `ON CONFLICT` — hoje, só
   a de `empresa` (2.6 e seção 3 abaixo).

> É esta fase que ataca as 16 horas: são 212 índices e 14 GB mantidos vivos
> através de um `shared_buffers` de 128 MB, com o `INSERT` esperando em
> `DataFileRead` (2.11). Os −37% medidos na 2.4 foram em `analytics.empresa`, que
> tem 3 índices; aqui são 212.

### Fase 3 — transform com sanitização, em blocos paralelos

> **Otimização já medida e que entra aqui (item 6 da seção 6 do redesenho):**
> `estabelecimento` e `estabelecimento_cnae_secundario` saem **da mesma fonte**.
> Lidos em sequência, são duas varreduras; lidos **ao mesmo tempo**, a segunda
> acha em cache o que a primeira trouxe — medido **22% mais barato** (320 s
> contra 409 s). Os dois consumidores disparam juntos, e ocupam 2 dos
> `LOAD_JOBS`.

Mesmo SQL de hoje **em resultado**, reorganizado para que cada regra da tabela S
tenha um lugar nomeado e para que o rejeito seja capturado em vez de sumir.

O `TRUNCATE` de cada tabela continua no início do seu próprio transform, como
hoje — e ele roda **uma vez, antes dos blocos**, nunca dentro de um bloco.

**Paralelismo em dois eixos**, ambos limitados por `LOAD_JOBS`:

1. **entre tabelas** — `socio`, `simples` e as dimensões não disputam nada entre
   si;
2. **entre blocos da mesma tabela** — o que ataca o caminho crítico, que é
   `estabelecimento`.

#### Como os blocos são feitos

Faixa de `ctid` da tabela de staging. Não há CSV, não há `split`, não há arquivo
intermediário: o bloco é um predicado no `FROM`.

```sql
WHERE ctid >= '(0,0)'::tid AND ctid < '(50500,0)'::tid
```

É **exatamente** o recorte medido na seção 2.8, que deu 2,5× com 4 jobs — logo o
número não é extrapolação de outro desenho, é deste. O número de páginas por
bloco sai de `relpages / LOAD_JOBS` na Fase 0.

#### A armadilha que os blocos trazem, e como ela é resolvida

Com blocos concorrentes, **"a primeira linha do arquivo vence" deixa de existir**:
quem chega primeiro passa a depender do escalonamento. Isso afeta todo
`ON CONFLICT DO NOTHING` — e é o que faria o dado do mês virar sorteio e o T2
ficar intermitente.

O alcance real do problema é menor do que parece, porque a Fase 2 já dropou os
índices:

| tabela | tem índice vivo na Fase 3? | então | blocos? |
|---|---|---|---|
| `empresa` | **sim** — PK viva, o `ON CONFLICT` precisa dela para arbitrar | a ordem decide quem vence | **NÃO — bloco único, sequencial** |
| `estabelecimento` | não | sem `ON CONFLICT`; as linhas são distintas (2.6) | **sim** |
| `cnae_secundario` | não | `DISTINCT` resolve dentro do bloco, e duplicata entre blocos exigiria estabelecimento repetido, que a 2.6 mediu como zero | **sim** |
| `simples` | não | detecção movida para a Fase 4 (7.1) | **sim** |
| `socio` | não | sem chave natural, nada a arbitrar | **sim** |

Ou seja: **só `empresa` fica sequencial**, e ela é a menor das cinco — 14,6 min
contra 30,4 min de `estabelecimento` na 2.1. O caminho crítico continua
paralelizado.

> Isto **não dispensa** a regra de desempate de `empresa` (decisão 2): ela segue
> sendo necessária para que "manter a primeira" deixe de ser sorte. O bloco único
> é o que permite implementar os blocos **agora**, sem esperar por ela.

#### Bloco que falha

Retry do bloco, não da carga. Um bloco é um `INSERT` sobre uma faixa de `ctid`
com `TRUNCATE` fora dele: reexecutar um bloco que falhou no meio **duplicaria**
linhas, então o retry só é seguro se o bloco inteiro estiver dentro de uma
transação — que é o comportamento padrão de um `INSERT` único. Um bloco é uma
declaração, logo é atômico: ou entrou inteiro, ou não entrou.

### Fase 4 — recriar os índices

Recria a partir de `carga.indice_salvo` — e é isso que garante R1: não existe uma
segunda cópia da definição para divergir do `04_indexes.sql`. `ANALYZE` ao fim.

**Depois da Fase 2, esta vira a fase mais cara da carga** — são 14 GB de índice
para construir. Logo ela também é paralela, e o paralelismo aqui é o mais barato
de todos: 212 índices independentes, nenhum `ON CONFLICT`, nenhuma ordem a
preservar, nada de determinismo em jogo. `IDX_JOBS` índices ao mesmo tempo,
saindo do mesmo orçamento da Fase 0.

Três regras, e a terceira não é óbvia:

1. **Construir pelo pai**, não por partição (restrição do T1, acima). O Postgres
   propaga para as 28 partições e regenera os nomes.
2. **Um índice do pai por vez, vários pais em paralelo** — `estabelecimento`,
   `empresa`, `socio` e `simples` não disputam entre si.
3. **`max_parallel_maintenance_workers` conta contra o orçamento duas vezes.**
   Cada worker paralelo de um `CREATE INDEX` usa a sua fatia de
   `maintenance_work_mem`. Três builds simultâneos com 2 workers cada não são 3
   alocações, são 9. Num host sem swap, é assim que se chega ao OOM achando que
   se está dentro do teto.

> **Restrição descoberta pelo T1, e ela é obrigatória:** os índices das 28
> partições **não têm nome escolhido por nós**. O `04_indexes.sql` cria o índice
> no pai particionado e o Postgres propaga gerando o nome
> (`estabelecimento_ac_cnpj_basico_idx`), inclusive truncando em 63 caracteres.
> Logo, a Fase 4 tem de dropar e recriar **pelo pai**, deixando o Postgres
> regenerar os mesmos nomes. Recriar partição a partição produz nomes diferentes
> e viola o R1. O teste `test_recriar_indices_pelo_pai_preserva_os_nomes` já
> passa hoje: a Fase 4 é viável exatamente assim, e só assim.

`maintenance_work_mem` e `max_parallel_maintenance_workers` saem do orçamento da
Fase 0, não de constante no arquivo.

### O orçamento, em números fechados

As Fases 3 e 4 **não se sobrepõem**, então cada uma pode usar o teto inteiro —
desde que ninguém confunda "teto por fase" com "teto somado".

| | Fase 3 (transform em blocos) | Fase 4 (índices) |
|---|---|---|
| teto total | 3 GB | 3 GB |
| `shared_buffers` (fixo, ver decisão 3) | 1 GB | 1 GB |
| sobra para a fase | 2 GB | 2 GB |
| jobs simultâneos | `LOAD_JOBS` = 3 | `IDX_JOBS` = 3 |
| parâmetro por job | `work_mem` ≤ **256 MB** | `maintenance_work_mem` ≤ **512 MB** |
| multiplicador escondido | `work_mem` é por **operação**, não por conexão (2 a 3 por `INSERT` complexo) | cada worker paralelo de índice pega sua fatia |
| `max_parallel_*` | `_workers_per_gather` = 0 na carga | `_maintenance_workers` = **1** |

Os dois "multiplicadores escondidos" são o motivo de os números parecerem
conservadores para um host de 16 GB: 3 jobs × 3 operações × 256 MB já é 2,3 GB de
pico teórico. O T12 mede o pico real; até ele existir, o número fica conservador
de propósito.

### Fase 5 e recuperação

IBGE, regime e matviews, sem mudança. E o `trap EXIT` — que hoje só imprime o
resumo de tempos — ganha uma **fase de recuperação obrigatória**: em qualquer
saída anormal após a Fase 2, recriar os índices antes de propagar o erro. Sem
isso, uma falha deixa a API sem índice até alguém agir de manhã (7.1), e o
watcher só retenta em 24 h.

---

## 3. Duplicatas — o que cada tabela faz

Da seção 2.6, e **sem mudar o resultado** (R1 + contrato de equivalência):

| tabela | chave | hoje | v2 |
|---|---|---|---|
| `empresa` | `cnpj_basico` | PK viva + `ON CONFLICT DO NOTHING` | **igual** — a PK fica viva na Fase 2 |
| `estabelecimento` | `(cnpj, uf)` | `ON CONFLICT` | detecção na Fase 4 (7.1) |
| `cnae_secundario` | `(cnpj, cnae)` | `ON CONFLICT` | `DISTINCT` no `unnest` + contador |
| `simples` | `cnpj_basico` | `ON CONFLICT` | detecção na Fase 4 (7.1) |
| `socio` | linha inteira | nada — 22 duplicatas estão no banco | **igual: espelha a fonte, sem tratamento** (5.2) |

Para `estabelecimento` e `simples`, a Fase 4 aplica o que a **7.1** decidiu:
varredura da chave → se limpo, `CREATE UNIQUE INDEX`; se sujo, índice
**não-único** mais quarentena em `carga.duplicata`, e a carga termina em
**sucesso degradado**, com um terceiro estado de saída que não faz o watcher
reagendar uma recarga de horas por causa de uma linha.

> ⚠️ **Isto tensiona o R1.** Num mês com duplicata o índice fica não-único, e o
> schema **difere** do `01_schema.sql`. É desvio conhecido, registrado e
> temporário: o T1 tem de tratá-lo como **exceção nomeada**, não como falha — e a
> carga seguinte volta ao índice único sozinha se o mês vier limpo.

---

## 4. Fora de escopo, e a medição que descartou cada um

| descartado | por quê |
|---|---|
| `file_fdw` / escrita única | 2.9: 86% mais lento no volume completo; 2.10: líquido negativo no servidor |
| CSV temporário na Fase 0 | consequência do acima — os blocos saem de faixa de `ctid`, não de arquivo |
| fatiar blocos por UF | 2.8: mais lento que fatiar por posição — SP é 28,7% do volume e vira o caminho crítico |
| carregar em banco paralelo + `RENAME` | **não descartado** — ver seção 5, item 5 |

**Blocos paralelos ENTRAM** (decisão de 16/09/2026). A versão anterior desta spec
os listava como fora de escopo, com o argumento de que 2,5× era pouco para a
complexidade. O argumento não se sustenta: 2,5× é ganho real, o recorte por
`ctid` medido na 2.8 é barato de implementar, e o único ponto sensível — o
desempate do `ON CONFLICT` — fica contido em `empresa`, resolvido com um bloco
sequencial (Fase 3).

---

## 5. Decisões — todas fechadas em 16/09/2026

1. ~~**`socio`**: deduplicar as 22 linhas idênticas?~~ **Decidido: não.** Ver 5.2.
2. **Regra de desempate de `empresa`**: sem ela, "a primeira linha do arquivo
   vence" continua sendo sorte — e é a mesma regra que destravaria o dedupe
   automático de `estabelecimento`/`simples` (7.1).
3. ~~**`shared_buffers` 128 MB → 1 GB**~~ — **entra** (16/09/2026). É a
   configuração que a 2.11 aponta como causa direta das 16 h: 14 GB de índice
   mantidos através de 128 MB de cache, com o `INSERT` esperando em
   `DataFileRead`. Ela ajuda **as duas** fases caras — o transform e a construção
   dos índices. Custo: um restart do container, que só pode acontecer com a carga
   parada. **Falta decidir apenas o quando**, não o se.
4. ~~**`wal_level = minimal`**~~ — **fica de fora, e por incompatibilidade, não
   por preguiça.** A otimização exige que o `TRUNCATE` e a escrita estejam na
   **mesma transação**, e uma transação é de uma sessão só. Blocos paralelos são
   N sessões, logo N transações: nenhuma se qualifica. Escolhido o paralelismo
   (2,5× medido), esta alavanca deixa de estar disponível. Se um dia os blocos
   forem revertidos, ela volta à mesa — e aí exige restart e perde PITR na
   janela.
5. ~~**Banco paralelo + `RENAME`**~~ — **adiado** (16/09/2026): não entra na v2.
   Não foi descartado por inviabilidade (cabe: precisa de ~89 GB, há 267 GB
   livres) e sim por prioridade — ele ataca **disponibilidade**, não tempo, e a
   v2 existe para atacar as 16 horas. Misturar as duas mudanças no mesmo mês
   também destrói o diagnóstico: se a carga seguinte der errado, não se sabe qual
   das duas causou.

   O que já foi levantado, para quem retomar (nada disto precisa ser
   redescoberto):

   - **A API já está pronta.** Os 28 `analytics.` do código Go são o
     comportamento desejado: ela fala com *o schema chamado `analytics`*, então
     passa a ler o novo sem alteração, sem reconectar, sem deploy.
   - **O trabalho real não é o `RENAME`, é tornar os SQL agnósticos de schema.**
     Hoje `01_schema`, `03_transform`, `04_indexes` e `05_materialized_views`
     escrevem `analytics.` na mão (11 ocorrências só nas matviews). Para o mesmo
     arquivo construir `analytics` ou `analytics_novo`, tem de passar a depender
     de `search_path` — e o T1 tem de normalizar o nome do schema antes de
     comparar os dumps.
   - **Armadilha do OID, e é silenciosa.** O Postgres guarda definição de view
     por **OID**, não por nome. Uma matview criada em `analytics_novo` que
     referencie `analytics.empresa` fica amarrada ao objeto **antigo**: depois do
     `RENAME` continua lendo o mês passado sem erro nenhum, e só aparece quando
     alguém tentar dropar o schema velho.
   - **O `RENAME` precisa de `ACCESS EXCLUSIVE`.** Uma consulta longa da API
     segura a troca, e todas as consultas novas enfileiram atrás dela. Exige
     `lock_timeout` curto com repetição.

   Quando entrar, a spec **encolhe**: o banco paralelo torna desnecessárias a
   quarentena da 7.1 e a recuperação do `trap`, e transforma o contrato da 7.2 de
   autópsia em **portão** — dá para comparar o schema novo com o antigo antes de
   trocar, e não trocar se divergir.
6. ~~**`parse_date` total**~~ — **decidido: entra** (16/09/2026), e generalizado
   para **todos** os casts e comprimentos (S11 e S12, seção R2.1). Ver 5.1.

### 5.1 A decisão 6 — decidida, e generalizada

Escrever o T3 revelou que **uma única data impossível derruba a carga inteira**:

```
analytics.parse_date('20200231')
  ERROR:  date/time field value out of range: "20200231"
```

`parse_date` trata as sentinelas (`''`, `'0'`, `'00000000'`) e delega o resto ao
`to_date`, que no PostgreSQL 18 **estoura** em data inexistente — não rola para o
mês seguinte, como se poderia supor. Um `20200231` ou um `20201332` vindos da
Receita matam uma carga de 20 horas na fase de transform, de madrugada, sem
ninguém para reagir (7.1).

Proposta: tornar `parse_date` **total** — data impossível vira `NULL` e um
rejeito S6 contado, em vez de exceção.

O ponto que torna a decisão barata: isso **não muda o resultado de nenhuma carga
que hoje termina com sucesso**. Se um mês tivesse uma data impossível, a carga
teria falhado por inteiro e não haveria conteúdo para comparar. Ou seja, a
mudança é segura pelo contrato de equivalência — não precisa de exceção nomeada.
Só o que era falha catastrófica passa a ser rejeito contabilizado.

O teste `test_s6_data_impossivel_derruba_a_carga_hoje` fixa o comportamento atual
e **deve ficar vermelho** quando esta decisão for implementada.

**Decisão tomada em 16/09/2026: entra — e vale para todos os casts, não só datas.**
Procurar outros do mesmo tipo rendeu mais quatro, todos igualmente fatais e todos
listados na R2.1. O objetivo declarado é robustez: a carga tem de ser capaz de
digerir qualquer coisa que a Receita publicar, registrando o que não entendeu, em
vez de morrer na hora 16 e deixar a base pela metade até alguém chegar de manhã.

---

### 5.2 A decisão 1 — `socio` espelha a fonte, e não se toca

**Decidido em 16/09/2026: nenhum tratamento.** As 22 linhas duplicadas continuam
no banco, exatamente como a Receita as publica.

O que se investigou antes de decidir:

- **A duplicata está na fonte, não no nosso `COPY`.** No staging, as duas cópias
  ficam na **mesma página**, em slots vizinhos (`(59002,14)` e `(59002,15)`) —
  logo, vieram do mesmo arquivo, a poucas linhas de distância. Zip carregado duas
  vezes produziria milhões de linhas em faixas distantes; sobreposição entre dois
  `Socios*.zip` produziria páginas distantes. Nenhum dos dois é o caso.
- **Uma implementação independente reproduz.** A BrasilAPI, que carrega os mesmos
  arquivos da RFB em outra infraestrutura, mostra `ROSEWOOK PARTNERS, INC.` duas
  vezes no CNPJ `00643187000141`, e `EMANOR` + `HAMBECK` duas vezes cada no
  `03373780000103` — o mesmo caso duplo que medimos aqui.
- **É um conjunto congelado.** Todas as entradas são de **27/03/1979 a
  08/08/2000**, nenhuma posterior; 12 das 18 empresas estão **baixadas**, 3
  inaptas, 3 ativas. O perfil é de artefato de migração antiga da própria
  Receita, não de erro recorrente de cadastro.

**Por que não deduplicar, mesmo com a duplicata confirmada:** os dois erros não
são simétricos. Apagar dado da fonte é irreversível e silencioso, e deixa no
código a licença para a carga remover linhas — licença que sobrevive ao contexto
que a justificou. Manter custa uma contagem de sócios levemente inflada em 18
empresas entre 27,8 milhões de linhas. Se um dia incomodar, a deduplicação
acontece **na consulta**, onde é reversível e testável.

Efeito colateral bom: como o conteúdo não muda, **não há exceção nomeada** a
carregar no contrato de equivalência (7.2).

> O teste `test_s10_socio_duplicado_entra_duas_vezes` **já trava esta decisão**:
> ele está verde hoje e fica vermelho se alguém "consertar" isso no futuro.

Fica valendo, por ser de custo zero: `carga.resumo` guarda a **contagem** de
duplicatas de `socio` por carga. Em dois ou três meses isso confirma (ou derruba)
a hipótese de conjunto congelado, sem ninguém precisar lembrar de olhar. É
contador, não tratamento — nada é alterado ou removido.

---

## 6. Testes — escrever antes, e passar contra a carga ATUAL

| # | teste | prova |
|---|---|---|
| **T1** | `pg_dump --schema-only` de `analytics`, antes × depois, por hash | **R1** |
| **T2** | fixture sintética: hash do dump ordenado de cada tabela, caminho atual × v2 | equivalência (7.2, nível 1) |
| **T3** | cada regra S1–S10 com um caso que a dispara e um que não | **R2** |
| **T4** | linha rejeitada aparece em `carga.rejeito` com a regra certa | **R2** |
| **T5** | matar a carga entre as Fases 2 e 4 → os índices voltam pelo `trap` | recuperação (7.1) |
| **T6** | host acima do orçamento → `LOAD_JOBS` degrada e a carga **não** aborta | **R3** / 7.3 |
| **T7** | `LOAD_JOBS` e `work_mem` derivam do orçamento, não de constante no arquivo | **R3** |
| **T8** | mês com duplicata → índice não-único, quarentena e saída degradada | 7.1 |
| **T9** | amostra coerente (`SAMPLE`): hash igual entre os dois caminhos | 7.2, nível 2 |
| **T10** | carga em blocos × carga sequencial: mesmo hash, e **repetido 3×** | determinismo dos blocos (Fase 3) |
| **T11** | `empresa` é carregada por um bloco só | a armadilha do `ON CONFLICT` (Fase 3) |
| **T12** | pico de memória com `LOAD_JOBS` jobs cabe no teto de 3 GB | **R3**, e o host não tem swap |
| **T13** | cada cast recebe lixo (`'20200231'`, `'1.234,56'`, `'99999'`, `'99999999999'`, ordem com 5 dígitos) e a carga **termina**, com rejeito contado | **S11/S12** — o requisito de robustez |
| **T14** | CSV com número de colunas diferente do layout falha na Fase 1, não na 16ª hora | **R2.2** |

T2 e T3 guiam a implementação. **T1 e T5 são os que impedem o desastre
silencioso** — o primeiro pega a estrutura mudando sem ninguém notar, o segundo
pega a API ficando sem índice de madrugada.

### Estado em 16/09/2026 — a v2 está implementada

Os 10 `xfail(strict=True)` que guardavam T4 a T14 caíram: cada um deles passou a
passar, o `strict` acusou, e os marcadores foram removidos. Eles deixaram de ser
"o que falta fazer" e viraram "o que não pode regredir".

- O golden do T2 **não mudou**. É o resultado que mais importa deste trabalho: a
  v2 sanitiza, conta, fatia em blocos e reconstrói 212 índices sem mexer numa
  vírgula do conteúdo.
- O T1 continua verde, inclusive no ciclo completo Fase 2 → transform → Fase 4:
  232 índices viram 12 e voltam a 232, com o `pg_dump` idêntico byte a byte.
- O **T9 passou** sobre os zips reais de 2026-06, com `SAMPLE=200000`: duas cargas
  completas pelo `load.sh` (`CARGA_TRANSFORM=sequencial` e `blocos`), hash a
  hash, todas as tabelas iguais. Ele continua pulando onde não há zips.

#### Os três bugs que só o T9 pegou

Vale registrar, porque os três passavam por 83 testes verdes e **nenhum deles
apareceria sem dado real**:

1. **`psql` em background comia o stdin do `while read`.** `executar_paralelo`
   alimenta os blocos por um pipe; o `psql` disparado com `&` herdava o mesmo
   pipe e consumia as linhas seguintes. O primeiro comando rodava, os demais
   sumiam — e a carga terminava **com sucesso e quatro tabelas vazias**. É o pior
   modo de falha possível: silencioso e verde. Corrigido com `< /dev/null`
   explícito no `psql`.
2. **`SIGPIPE` na conferência de layout.** `unzip -p | head -1` mata o `unzip`
   com SIGPIPE, e sob `set -o pipefail` o pipeline reporta falha mesmo tendo
   lido a linha. O `|| echo 0` do chamador emendava um `0` depois do número
   certo, e `[ "7\n0" -ne 7 ]` reprovava com "integer expected". Com os zips
   stubados o problema não existe: o stub termina antes do SIGPIPE.
3. **O transform sequencial não sobrevivia à Fase 2.** `03_transform.sql` usava
   `ON CONFLICT (cnpj, uf)` e `ON CONFLICT (cnpj_basico)`, que precisam de um
   índice único para arbitrar — e a Fase 2 dropa exatamente esses índices. O
   caminho em blocos já usava `ON CONFLICT DO NOTHING` sem inferência; o
   sequencial passou a usar também. Só `empresa` infere a chave, porque a PK
   dela é a única que a Fase 2 preserva.

O bug 1 é o argumento mais forte a favor de o T9 ser **obrigatório antes de ir
para o servidor**, e não um teste opcional: a suíte inteira estava verde enquanto
a carga real produzia base vazia.

#### E mais três na Fase 0, achados ao rodar a carga completa

A Fase 0 é o trecho que menos se testa e o que mais decide — todos os números da
carga saem dela. Rodar uma carga de verdade expôs três erros:

4. **`work_mem` era derivado do teto inteiro, ignorando o `shared_buffers`.** Com
   3 GB de teto e 1 GB fixo de `shared_buffers`, `3 jobs × 3 operações × 256 MB`
   davam 2,3 GB que, somados ao próprio `shared_buffers`, passavam dos 3 GB. A
   tabela da 7.3 sempre disse que a sobra **por fase** é 2 GB; o código é que
   lia 3. Agora `SHARED_BUFFERS_MB` é descontado antes de qualquer derivação, e
   é o **T12 que pega** — foi ele quem reprovou a conta errada.
5. **A conta de vCPU livres misturava duas grandezas.** Era `cota − load do
   host`, quando o certo é `nproc − load`, e só então limitar pela cota. A forma
   errada subestima sempre: num servidor de 8 vCPU com load 2 e cota 4, ela dá 2
   jobs onde cabem 3 — e numa máquina de 16 vCPU com load 8 degradava para 1 job
   sem motivo.
6. **`MemAvailable` nem sempre existe.** O `/proc/meminfo` do MSYS só expõe
   `MemTotal` e `MemFree`, então `livre_mb` ficava 0 e a degradação por memória
   **nunca disparava** — silêncio justamente no host onde o OOM é o risco. Agora
   há fallback para `MemFree`.

> **A recuperação do trap funcionou em condição real, sem ensaio.** Quando o bug
> 3 derrubou a carga depois da Fase 2, o `trap EXIT` disparou sozinho e recriou
> os cinco índices antes de propagar o erro. É exatamente o cenário da 7.1 — e
> foi a primeira vez que ele aconteceu de verdade, por acidente.

Três correções de **encanamento** foram feitas nos testes, e nenhuma tocou no que
eles afirmam — são registradas aqui porque mexer em teste é exatamente o que não
se deve fazer em silêncio:

1. **T5** chamava `bash recuperar_indices.sh` sem ambiente: o script não sabia
   qual banco recuperar (cairia no `cnpj` de verdade, não no descartável do
   teste) e `bash` puro no Windows é o launcher do WSL, que não herda o
   ambiente. Passou a receber `DB` e o `bash_exe` do conftest.
2. **T12** consultava `carga.resumo` num banco vazio e exigia `max(pico_rss_mb)`,
   que só existe depois de uma carga real. Virou dois testes: o pico **teórico**
   derivado da Fase 0 (`LOAD_JOBS × ~3 × work_mem + shared_buffers ≤ 3 GB`), que
   é conferível sem uma carga de horas, e a guarda estrutural de que o load.sh
   grava `pico_rss_mb`. O pico REAL do servidor não é observável do processo de
   carga quando o banco está noutro container — fica para a primeira carga
   completa no servidor, e é por isso que o número segue conservador.
3. **T4-limiar, T6 e T14** rodavam `load.sh` em modo amostra sem o stub de `rg`,
   e mediam a ausência do ripgrep em vez do que queriam medir. Ganharam
   `com_rg=1`, como os demais testes de `load.sh` já faziam.

E o stub de `unzip` do conftest passou a devolver **o layout de cada zip** (7
colunas para `Empresas`, 30 para `Estabelecimentos`, …) em vez da mesma linha
para todos. Enquanto ninguém olhava o formato, o stub uniforme bastava; a
conferência de layout da R2.2 olha.

O golden do T2 está em `analytics/tests/golden_carga_atual.json` e **tem de ser
versionado**: é o registro do conteúdo que a carga produz. Regravá-lo sem
registrar uma exceção nomeada na spec é como mudar o contrato em silêncio.

A suíte leva ~5 min porque cada teste cria um banco descartável e roda
`01_schema` + `02_staging` + `03_transform`. Se incomodar, o caminho é uma
fixture de escopo de sessão — não cortar casos.

---

## 6.1 Tudo que entra, e de onde veio o número

Resumo do que a v2 acumula, para não se perder na soma:

| # | otimização | ganho | origem |
|---|---|---|---|
| 1 | **dropar índices antes do transform** | é o alvo das **16 h** (2.11); −37% medido na menor tabela (2.4) | Fase 2 |
| 2 | **`shared_buffers` 128 MB → 1 GB** | ataca o `DataFileRead` que causa as 16 h; ajuda transform **e** índices | decisão 3 |
| 3 | **blocos paralelos por `ctid`** | **2,5×** medido, ~2× dentro do orçamento | Fase 3 / 2.8 |
| 4 | **os dois consumidores do mesmo staging em paralelo** | **−22%** medido | Fase 3 / 2.9 |
| 5 | **construção de índices em paralelo** | 212 índices independentes; a fase mais cara depois da nº 1 | Fase 4 |
| 6 | **staging `UNLOGGED`** | corta o WAL de 27 GB de dado descartável | Fase 1 |
| 7 | **`synchronous_commit = off`** | já existe hoje no `load.sh` | — |

O que **não** entra, e o motivo em uma linha: `file_fdw` (86% mais lento, 2.9),
CSV temporário (consequência), fatiar por UF (mais lento que por posição, 2.8) e
`wal_level = minimal` (incompatível com blocos, decisão 4).

Duas coisas que a soma **não** permite: multiplicar os ganhos entre si, e supor
que eles sobrevivem ao servidor no tamanho medido aqui. Os números de 2.8 e 2.9
são de uma máquina com 16 vCPU ociosas; o servidor tem 8 compartilhadas. A conta
final se faz lá, não aqui.

---

## 6.2 A carga completa de ensaio — 16/09/2026

Carga completa da v2 sobre o dump de 2026-06, na máquina de desenvolvimento, com
o **orçamento do servidor** (`ORCAMENTO_RAM_MB=3072`, `ORCAMENTO_VCPU=4`) para
que os números sejam comparáveis. Derivados pela Fase 0: `LOAD_JOBS=3`,
`work_mem=204MB`, `maintenance_work_mem=341MB`.

| fase | tempo |
|---|---|
| 0 pré-voo | 4 s |
| 1 schema + staging (DDL) | 4 s |
| **COPY** (lookups, empresas, estabelecimentos, sócios, simples, regime) | **45m06s** |
| **2 drop de índices** | **< 1 s** |
| **3 transform (3 blocos)** | **2h36m58s** |
| de-para IBGE | 8 s |
| 4a detecção de duplicata | 51m19s |
| 4b reconstrução paralela dos índices | 23m19s |
| 4c `04_indexes.sql` (os 212 índices, ver ressalva) | 44m14s |
| 5 matviews + regime | 4m39s |
| **TOTAL** | **5h26m26s** |

### A equivalência, provada no volume completo

O `cnpj_bench` — 87 GB, carregado pelo caminho **antigo**, sem o schema `carga` —
estava na mesma máquina. Comparar os dois é o teste que nem o T2 (fixture) nem o
T9 (amostra de 200 mil) conseguem fazer: **339 milhões de linhas, hash a hash**.

| tabela | linhas | `sum(hashtext(linha))` — antigo × v2 |
|---|---|---|
| `estabelecimento` | 71.874.448 | idêntico |
| `estabelecimento_cnae_secundario` | 121.703.224 | idêntico |
| `empresa` | 68.629.147 | idêntico |
| `simples` | 49.034.553 | idêntico |
| `socio` | 27.838.448 | idêntico |
| `dim_cnae`, `dim_municipio` | 6.931 | idêntico |

O hash é `sum(hashtext(t::text))`, que é **ordem-independente** — de propósito:
ordenar 71 milhões de linhas para comparar seria caro e desnecessário, e a soma
detecta qualquer diferença de conteúdo sem depender da ordem de inserção, que os
blocos mudam por construção.

E o **R1 no volume completo**: `pg_dump --schema-only` dos dois bancos, 1.456
linhas de DDL cada, **idênticas** — mesmas tabelas, 28 partições e 192 índices.
As três únicas diferenças são todas explicadas e nenhuma vem da carga: o corpo
novo de `parse_date` (decisão 6), e `norm_municipio` mais
`ux_dim_municipio_ibge`, que vêm do `ibge_transform.sql` — este rodou no ensaio e
não no `cnpj_bench`.

### Três coisas que o ensaio ensinou, e uma delas é ruim

1. **A Fase 2 custa menos de 1 segundo.** Salvar a DDL e dropar os índices é
   barato; era a hipótese e se confirmou.
2. **A detecção de duplicata da Fase 4 custava 51 minutos** — quase 1/6 da carga
   para encontrar 23 linhas. **Corrigido em 16/09/2026**; ver 6.3 abaixo.
3. **Este ensaio mediu o PIOR caso da Fase 4**, porque o banco era novo. Medido
   depois no cenário mensal — ver 6.4.

## 6.3 A detecção de duplicata deixou de custar 51 minutos

O desenho original da Fase 4 dizia "varredura da chave → se limpo,
`CREATE UNIQUE INDEX`; se sujo, índice não-único mais quarentena". A carga de
ensaio mostrou o preço dessa vírgula: **51 minutos, toda carga, para encontrar
23 linhas** — três `GROUP BY ... HAVING count(*) > 1` sobre 71,9 milhões de
estabelecimentos, 49 milhões de simples e 27,8 milhões de sócios, todos
derramando em disco porque não cabem em `work_mem=204MB`.

**A correção: o `CREATE UNIQUE INDEX` já é a varredura.** Ele percorre a tabela
inteira para construir o índice e verifica a unicidade de graça, no mesmo passe.
Então a Fase 4 **tenta** o índice único; se ele falhar com `unique_violation`, aí
sim `carga.quarentenar()` varre a fonte e o índice é recriado não-único.

| | antes | agora |
|---|---|---|
| mês limpo (o normal) | 51 min de varredura preventiva | **zero** |
| mês sujo | 51 min + o build | build perdido + varredura, e aí ela se paga |

O contrato da 7.1 é o mesmo — índice não-único, `carga.duplicata`, sucesso
degradado. Mudou só **como se descobre**, e o T8 continua sendo quem cobra o
desfecho.

> **Sobre o bloco `EXCEPTION`, que a R2.1 proíbe.** A proibição é sobre
> `EXCEPTION` **por linha**: cada bloco abre uma subtransação, e 73 milhões delas
> custam mais que o problema que resolveriam. Aqui é uma por índice — no máximo
> 212 numa carga inteira. São coisas diferentes, e confundir as duas seria perder
> a otimização por causa de uma regra lida fora de contexto.

### A decisão 5.2 e o custo que ela não tinha

A contagem de duplicatas idênticas em `socio` não tem índice de onde pegar
carona: ela é uma varredura dedicada, `GROUP BY` de 11 colunas sobre 27,8 milhões
de linhas, e era o pedaço mais caro dos 51 minutos. A decisão 5.2 a chamou de
"custo zero" — a medição mostrou que não é.

Ela **não alimenta nenhuma decisão da carga**: serve só para confirmar, ao longo
de alguns meses, se as 22 duplicatas são o conjunto congelado de 1979–2000 que a
5.2 supõe. Por isso passou a ser **opcional e desligada por padrão**
(`CONTAR_DUP_SOCIO=1`). A hipótese continua verificável; o que mudou é que ela
deixou de ser cobrada de toda carga mensal.

---

## 6.4 O ciclo de índices no cenário MENSAL — 32 minutos

O ensaio da 6.2 mediu o pior caso: banco novo, sem índice para a Fase 2 salvar,
e os 212 secundários nascendo **sequencialmente** no `04_indexes.sql`. A carga
mensal é outra coisa — o banco já tem os índices do mês anterior.

Medido sobre uma cópia do `cnpj_bench` (87 GB, base completa com os índices
prontos), com o mesmo orçamento do servidor:

| passo | tempo |
|---|---|
| **Fase 2** — salvar a DDL e dropar **220** índices (238 → 18) | **2 s** |
| **Fase 4** — reconstruir os 17 índices-pai (~28 GB), 3 em paralelo | **31m52s** |
| fechamento + `ANALYZE` | 52 s |
| **ciclo completo** | **32 min** |

E `pg_dump` antes × depois: **idêntico** — 238 índices voltaram exatamente como
estavam, com os nomes que o Postgres gera para as 28 partições.

> Os 17 índices não são um erro de contagem: a Fase 2 salva e dropa apenas os
> índices do **pai**, e o Postgres propaga para as 28 partições. Foi essa a
> restrição descoberta pelo T1 — recriar partição a partição geraria nomes
> diferentes e violaria o R1.

### O que isso faz com a conta da carga mensal

| fase | ensaio (banco novo) | mensal (medido) |
|---|---|---|
| COPY | 45m06s | 45m06s |
| Fase 2 | < 1 s | 2 s |
| **Fase 3 — transform** | **2h36m58s** | **2h36m58s** |
| Fase 4 + `04_indexes.sql` | 1h58m52s | **32m** |
| resto (IBGE, regime, matviews) | 4m47s | 4m47s |
| **total** | **5h26m** | **≈ 3h59m** |

**A meta da seção 7 — carga completa abaixo de 6 h — está cumprida na máquina de
desenvolvimento.** E a conta inverteu: o gargalo deixou de ser índice e passou a
ser o **transform**, que agora é 2/3 da carga. O diagnóstico original da 2.11
apontava 16 das 20 horas em manutenção de índice; depois da Fase 2, da
reconstrução paralela e do fim da varredura preventiva, o ciclo inteiro de
índices cabe em 32 minutos.

Quem for otimizar a próxima rodada deve olhar para a Fase 3, não para os índices
— e o alvo mais provável é o que a 2.9 já mediu: `estabelecimento` e
`estabelecimento_cnae_secundario` saem da mesma staging e hoje disparam juntos,
mas o ganho de 22% foi medido em outro contexto e não foi reconferido aqui.

---

> **O que este ensaio NÃO prova.** Que a carga do servidor vai a 5h26. A máquina
> de desenvolvimento tem 16 vCPU e o servidor tem 8 compartilhadas; o orçamento
> foi igualado, o hardware não. O que ele prova é o que importa antes de ir para
> lá: o conteúdo é idêntico, a estrutura é idêntica, e a carga termina.

---

## 7. Como se sabe que deu certo

- T1 a T8 verdes, e T9 verde antes de ir para o servidor;
- o `INSERT` de `estabelecimento` no servidor sai das **15h59m** da 2.11 para a
  ordem de **1 h** — é a hipótese central desta spec e a única métrica que
  realmente importa. Dessa queda, a parte grande vem da Fase 2 (índices) e a
  parte menor dos blocos da Fase 3; se a Fase 2 sozinha já entregar, os blocos
  ficam como margem, não como necessidade;
- o T10 verde **três vezes seguidas** — um teste de determinismo que passa uma
  vez não provou nada;
- carga completa abaixo de **6 h** no servidor (hoje: 20 h+);
- `carga.rejeito` **consistente com a aritmética da própria carga**, e não com um
  número esperado de antemão: para cada tabela, `linhas_lidas` tem de fechar com
  `linhas_inseridas` mais o que o rejeito e as duplicatas explicam. Zero rejeito
  é um resultado **válido** — foi o que a carga de ensaio mediu sobre o dump de
  2026-06, e a conferência contra o `cnpj_bench` confirmou que é o valor certo.
  O que continua sendo alarme é rejeito **inexplicado**: linhas que somem sem
  aparecer em nenhum dos dois lados da conta.
