-- ============================================================================
-- fase2_drop_indices.sql — Fase 2 da spec: salvar a DDL e dropar os índices
--
-- Esta fase existe por causa de UMA medição (2.11): das ~20 horas da carga, 16
-- estão na manutenção de índice de uma tabela só. São 212 índices e 14 GB
-- mantidos vivos através de um shared_buffers de 128 MB, com o INSERT parado em
-- `DataFileRead`. Construir no fim é ordens de magnitude mais barato que manter
-- durante o INSERT — os −37% da 2.4 foram medidos em `empresa`, que tem 3
-- índices; aqui são 212.
--
-- Rodar ENTRE o COPY e o transform. A Fase 4 (fase4_indices.sql) recria.
--
-- ⚠️ A partir daqui e até a Fase 4, a base está SEM ÍNDICE. Se a carga morrer
-- no meio, a API responde com seq scan em 73 milhões de linhas até alguém agir.
-- É por isso que o `trap EXIT` do load.sh chama `recuperar_indices.sh` — e é o
-- T5 que impede que alguém tire essa rede sem perceber.
--
-- Duas regras que não são óbvias:
--
-- 1. **Dropar pelo PAI, nunca por partição.** Os índices das 28 partições não
--    têm nome escolhido por nós: o 04_indexes.sql cria no pai particionado e o
--    Postgres propaga gerando o nome (`estabelecimento_ac_cnpj_basico_idx`),
--    inclusive truncando em 63 caracteres. Dropar (e recriar) partição a
--    partição produz nomes diferentes e viola o R1. Dropar o índice do pai já
--    leva os 28 filhos junto.
--
-- 2. **A PK de `empresa` FICA.** É o único índice que o transform ainda usa: o
--    `ON CONFLICT (cnpj_basico) DO NOTHING` precisa dela para arbitrar entre
--    duplicatas. Sem ela, "a primeira linha do arquivo vence" deixaria de valer.
--    As demais PKs caem — a detecção de duplicata delas foi movida para a Fase 4.
-- ============================================================================

\set ON_ERROR_STOP on

DO $$
DECLARE
    r record;
    v_alvos text[] := ARRAY['empresa', 'estabelecimento',
                            'estabelecimento_cnae_secundario', 'socio', 'simples'];
    v_dropados integer := 0;
BEGIN
    IF to_regclass('carga.indice_salvo') IS NULL THEN
        RAISE EXCEPTION 'schema `carga` ausente — aplique analytics/00_carga.sql antes da Fase 2';
    END IF;

    -- Salvar TUDO antes de dropar qualquer coisa. Se o processo morrer entre o
    -- salvamento e o DROP, a Fase 4 apenas recria o que já existe (é
    -- idempotente); na ordem inversa, a definição se perderia para sempre.
    FOR r IN
        SELECT n.nspname                            AS schema_nome,
               c.relname                            AS tabela,
               ic.relname                           AS indice,
               con.conname IS NOT NULL              AS e_constraint,
               CASE WHEN con.conname IS NOT NULL
                    THEN format('ALTER TABLE %I.%I ADD CONSTRAINT %I %s',
                                n.nspname, c.relname, con.conname, pg_get_constraintdef(con.oid))
                    -- `ON ONLY` tem de sair, e esta é a linha mais importante do
                    -- arquivo. Para uma tabela particionada, `pg_get_indexdef`
                    -- devolve `CREATE INDEX ... ON ONLY analytics.estabelecimento
                    -- ...` — e essa forma cria um índice INVÁLIDO só no pai, sem
                    -- propagar para as 28 partições. Recriar assim deixaria a
                    -- tabela com 6 índices em vez de 174, todos marcados
                    -- `indisvalid = false`, e o R1 iria junto.
                    -- Com `ON`, o Postgres constrói em cada partição e regenera
                    -- os nomes que ele mesmo gerou da primeira vez
                    -- (`estabelecimento_ac_cnpj_basico_idx`), que é a única forma
                    -- de o dump voltar idêntico.
                    ELSE replace(pg_get_indexdef(i.indexrelid), ' ON ONLY ', ' ON ')
               END                                  AS definicao
          FROM pg_index i
          JOIN pg_class      ic  ON ic.oid = i.indexrelid
          JOIN pg_class      c   ON c.oid  = i.indrelid
          JOIN pg_namespace  n   ON n.oid  = c.relnamespace
          LEFT JOIN pg_constraint con ON con.conindid = i.indexrelid
                                     AND con.contype IN ('p', 'u')
         WHERE n.nspname = 'analytics'
           AND c.relname = ANY (v_alvos)
           AND NOT c.relispartition          -- só o pai: os filhos vêm junto
    LOOP
        INSERT INTO carga.indice_salvo (schema_nome, tabela, indice, definicao, e_constraint)
        VALUES (r.schema_nome, r.tabela, r.indice, r.definicao, r.e_constraint)
        ON CONFLICT (schema_nome, indice)
        DO UPDATE SET definicao = excluded.definicao,
                      e_constraint = excluded.e_constraint,
                      tabela = excluded.tabela,
                      dropado = false,
                      salvo_em = now();
    END LOOP;

    -- Agora dropar. A PK de `empresa` é a única exceção (ver cabeçalho).
    FOR r IN
        SELECT * FROM carga.indice_salvo
         WHERE schema_nome = 'analytics'
           AND NOT (tabela = 'empresa' AND e_constraint)
         ORDER BY tabela, indice
    LOOP
        IF r.e_constraint THEN
            EXECUTE format('ALTER TABLE %I.%I DROP CONSTRAINT IF EXISTS %I',
                           r.schema_nome, r.tabela, r.indice);
        ELSE
            EXECUTE format('DROP INDEX IF EXISTS %I.%I', r.schema_nome, r.indice);
        END IF;
        UPDATE carga.indice_salvo SET dropado = true
         WHERE schema_nome = r.schema_nome AND indice = r.indice;
        v_dropados := v_dropados + 1;
    END LOOP;

    RAISE NOTICE 'Fase 2: % definições salvas, % índices dropados (a PK de empresa fica)',
                 (SELECT count(*) FROM carga.indice_salvo WHERE schema_nome = 'analytics'),
                 v_dropados;
END $$;

SELECT tabela, indice, e_constraint, dropado
  FROM carga.indice_salvo
 WHERE schema_nome = 'analytics'
 ORDER BY tabela, indice;
