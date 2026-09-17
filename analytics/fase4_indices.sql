-- ============================================================================
-- fase4_indices.sql — Fase 4 da spec: duplicata, quarentena e reconstrução
--
-- Depois que a Fase 2 tirou os 212 índices do caminho, ESTA vira a fase mais
-- cara da carga: são 14 GB de índice para construir. Ela faz três coisas, nesta
-- ordem:
--
--   1. procura duplicata de chave natural na staging;
--   2. recria os índices a partir de `carga.indice_salvo` — único, ou NÃO-ÚNICO
--      quando o mês veio sujo;
--   3. ANALYZE.
--
-- Por que recriar a partir do `carga.indice_salvo` e não do 04_indexes.sql: é o
-- que garante o R1. Não existe uma segunda cópia da definição em lugar nenhum
-- que possa divergir do que o banco realmente tinha antes da carga.
--
-- É IDEMPOTENTE de propósito: o `recuperar_indices.sh`, chamado pelo trap numa
-- saída anormal, executa exatamente este arquivo.
--
-- Paralelismo: aqui ele é o mais barato de todos — 212 índices independentes,
-- nenhum ON CONFLICT, nenhuma ordem a preservar. Quem paraleliza é o load.sh,
-- que chama `carga.recriar_indice(...)` em IDX_JOBS sessões. Este arquivo, em
-- sessão única, faz tudo em sequência.
--
-- ⚠️ `max_parallel_maintenance_workers` conta contra o orçamento DUAS vezes:
-- cada worker paralelo de um CREATE INDEX usa a sua fatia de
-- `maintenance_work_mem`. Três builds simultâneos com 2 workers cada não são 3
-- alocações, são 9 — e num host sem swap é assim que se chega ao OOM achando
-- que se está dentro do teto. A Fase 0 do load.sh deriva os dois juntos.
-- ============================================================================

\set ON_ERROR_STOP on

-- As duas etapas podem ser executadas separadamente, e o load.sh usa isso para
-- paralelizar: primeiro `-v fazer_indices=0` (só a detecção, que tem de vir
-- antes — é ela que decide único × não-único), depois N sessões chamando
-- `carga.recriar_indice`, e por fim `-v fazer_duplicatas=0` para o ANALYZE.
-- Sem variável nenhuma, faz tudo em sequência: é assim que a recuperação do
-- trap o executa.
\if :{?fazer_duplicatas}
\else
  \set fazer_duplicatas 1
\endif
\if :{?fazer_indices}
\else
  \set fazer_indices 1
\endif

DO $$
BEGIN
    IF to_regclass('carga.indice_salvo') IS NULL THEN
        RAISE EXCEPTION 'schema `carga` ausente — aplique analytics/00_carga.sql antes da Fase 4';
    END IF;
END $$;

-- ----------------------------------------------------------------------------
-- 1. Duplicata de chave natural (decisão 7.1) — SEM varredura preventiva
--
-- Este bloco já varreu a fonte antes de reconstruir, para decidir se cada índice
-- sairia único ou não-único. Na carga completa de 16/09/2026 isso custou **51
-- minutos** — `GROUP BY` sobre 71,9 milhões de estabelecimentos, 49 milhões de
-- simples e 27,8 milhões de sócios, todos derramando em disco — para encontrar
-- 23 linhas. Quase 1/6 da carga gasto em auditoria preventiva.
--
-- A descoberta agora é de graça: quem acha a duplicata é o próprio
-- `CREATE UNIQUE INDEX` da etapa 2, que varre a tabela de qualquer jeito para
-- construir o índice. Falhou, `carga.quarentenar()` entra e a varredura
-- acontece — mas só no mês que a justifica. Mês limpo custa zero.
--
-- O desfecho contratado pela 7.1 não mudou: índice não-único, chaves em
-- `carga.duplicata`, carga em SUCESSO DEGRADADO. Mudou só como se descobre.
-- ----------------------------------------------------------------------------
\if :fazer_duplicatas

-- `socio` não tem índice único nenhum a criar, então não há trabalho de onde
-- pegar carona: esta contagem é uma varredura dedicada, e a mais cara das três
-- (GROUP BY de 11 colunas sobre 27,8 milhões de linhas). Ela existe só para
-- alimentar o contador da decisão 5.2 — confirmar, em dois ou três meses, se as
-- 22 duplicatas são mesmo um conjunto congelado de 1979–2000.
--
-- A decisão 5.2 a chamou de "custo zero". A medição mostrou que não é, então ela
-- passou a ser OPCIONAL: `-v contar_dup_socio=1` (ou `CONTAR_DUP_SOCIO=1` no
-- load.sh). Rode quando quiser conferir a hipótese; não a pague todo mês.
\if :{?contar_dup_socio}
\else
  \set contar_dup_socio 0
\endif
\if :contar_dup_socio
SELECT carga.contar('socio', 'S10_duplicata_na_fonte', coalesce(sum(repetidas), 0)::bigint)
FROM (
    SELECT count(*) - 1 AS repetidas
    FROM staging.socios
    WHERE cnpj_basico ~ '^\d{8}$'
    GROUP BY cnpj_basico, identificador_socio, nome_socio, cnpj_cpf_socio,
             qualificacao_socio, data_entrada_sociedade, pais, cpf_representante,
             nome_representante, qualificacao_repr, faixa_etaria
    HAVING count(*) > 1
) d;
\endif

\endif
\if :fazer_indices

-- ----------------------------------------------------------------------------
-- 2. Reconstrução
--
-- O trabalho de verdade está em `carga.recriar_indice`, definida no
-- 00_carga.sql. Ela é função — e não um bloco aqui dentro — porque o load.sh
-- precisa chamá-la em IDX_JOBS sessões ao mesmo tempo: o paralelismo desta fase
-- mora lá. Este arquivo faz tudo em sequência, que é o que a recuperação do
-- trap precisa.
-- ----------------------------------------------------------------------------
DO $$
DECLARE
    r record;
BEGIN
    -- Mês sujo com a constraint AINDA viva (a Fase 2 não rodou, ou a PK de
    -- `empresa` que ela preserva): a constraint única tem de sair antes, senão
    -- o índice não-único não tem como existir.
    FOR r IN
        SELECT s.tabela, s.indice FROM carga.indice_salvo s
         WHERE s.schema_nome = 'analytics' AND s.e_constraint
           AND carga.tabela_suja(s.tabela)
           AND to_regclass(format('analytics.%I', s.indice)) IS NOT NULL
    LOOP
        EXECUTE format('ALTER TABLE analytics.%I DROP CONSTRAINT %I', r.tabela, r.indice);
    END LOOP;

    FOR r IN SELECT indice FROM carga.indice_salvo
              WHERE schema_nome = 'analytics' ORDER BY tabela, e_constraint DESC, indice
    LOOP
        RAISE NOTICE '%', carga.recriar_indice(r.indice);
    END LOOP;
END $$;

-- Rede para o caso em que a Fase 2 NÃO rodou: a PK que veio do 01_schema.sql
-- continua viva e única, então o `CREATE UNIQUE INDEX` da etapa 2 nunca chega a
-- ser tentado e a duplicata passaria batida. Só age se JÁ houver quarentena
-- registrada — nunca varre por conta própria.
DO $$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT c.relname AS tabela, con.conname AS indice, pg_get_constraintdef(con.oid) AS def
          FROM pg_constraint con
          JOIN pg_class     c ON c.oid = con.conrelid
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = 'analytics' AND con.contype = 'p'
           AND carga.tabela_suja(c.relname)
    LOOP
        EXECUTE format('ALTER TABLE analytics.%I DROP CONSTRAINT %I', r.tabela, r.indice);
        EXECUTE format('CREATE INDEX %I ON analytics.%I (%s)',
                       r.indice, r.tabela, substring(r.def from '\((.*)\)$'));
        RAISE NOTICE '% virou índice NÃO-ÚNICO: % tem duplicata neste mês', r.indice, r.tabela;
    END LOOP;
END $$;

-- ----------------------------------------------------------------------------
-- 3. ANALYZE
-- ----------------------------------------------------------------------------
\endif
ANALYZE analytics.empresa;
ANALYZE analytics.estabelecimento;
ANALYZE analytics.estabelecimento_cnae_secundario;
ANALYZE analytics.socio;
ANALYZE analytics.simples;

-- O desfecho da carga sai daqui: mês com duplicata termina 'degradado', não
-- 'falha'. A diferença importa para o watcher, que não deve reagendar uma
-- recarga de 6 horas por causa de uma linha repetida.
SELECT CASE WHEN EXISTS (SELECT 1 FROM carga.duplicata
                          WHERE competencia = carga.competencia())
            THEN 'degradado' ELSE 'sucesso' END AS desfecho_indices,
       (SELECT count(*) FROM carga.duplicata WHERE competencia = carga.competencia())
            AS chaves_em_quarentena;
