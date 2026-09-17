-- ============================================================================
-- 00_carga.sql — o schema `carga`: o que a carga guarda SOBRE SI MESMA
--
-- Por que um schema separado (spec-carga.md, R1): o `analytics` tem de ficar
-- byte a byte igual ao que `01_schema.sql` + `04_indexes.sql` produzem — é o
-- teste T1. Logo, nenhuma tabela de controle, coluna de rastreio ou índice
-- auxiliar pode morar lá. Tudo que a carga precisa registrar (rejeito,
-- duplicata, contador, DDL de índice salvo, resumo da execução) mora aqui.
-- A API não enxerga este schema: se alguém o dropar, a carga continua correta e
-- só se perde a auditoria.
--
-- Aplicar ANTES de 03_transform.sql. O load.sh faz isso; nos testes, quem faz é
-- `preparar_banco()` em analytics/tests/fixture_carga.py.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS carga;

-- ----------------------------------------------------------------------------
-- Competência da carga corrente
--
-- Vem do GUC `carga.competencia`, setado pelo load.sh no início da execução
-- (`SET carga.competencia = '2026-09'`). Sem ele — rodando um .sql na mão, ou
-- num teste — cai no mês corrente, para nunca gravar rejeito órfão.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION carga.competencia() RETURNS text
    LANGUAGE sql STABLE PARALLEL SAFE AS $$
    SELECT coalesce(nullif(current_setting('carga.competencia', true), ''),
                    to_char(current_date, 'YYYY-MM'));
$$;

-- ----------------------------------------------------------------------------
-- Casts TOTAIS (S11 e S12 da spec)
--
-- "Total" = para qualquer entrada existe saída. Valor que não converte vira
-- NULL; NUNCA exceção. É o requisito R2.1: hoje uma única célula malformada em
-- 73 milhões de linhas mata uma carga de 20 horas, às 3 da manhã.
--
-- ⚠️ Duas restrições de implementação, e as duas são de desempenho:
--
-- 1. NADA de bloco EXCEPTION. Cada bloco EXCEPTION abre uma subtransação, e uma
--    subtransação por linha em 73 milhões de linhas é ordem de magnitude pior
--    que o problema que se quer resolver. A validação é por regex e faixa,
--    ANTES do cast.
-- 2. O corpo é um único SELECT, sem CTE e sem subconsulta. Isso não é estilo: o
--    inliner do Postgres (`inline_function`) recusa corpos com `WITH` ou
--    sublink, e uma função NÃO inlinada vira uma chamada por linha — 73 milhões
--    delas, por coluna. Por isso as expressões se repetem em vez de serem
--    fatoradas num CTE.
--
-- O CASE é o que protege o cast: os ramos seguintes só são avaliados quando a
-- guarda de regex já falhou. (A ressalva do manual sobre dobra de constantes
-- não se aplica aqui — o argumento é sempre uma coluna da staging.)
-- ----------------------------------------------------------------------------

-- `^[+-]?[0-9]{1,5}$` garante no máximo 99999, que sempre cabe em integer; daí
-- a faixa do smallint é conferida por comparação, não por tentativa.
CREATE OR REPLACE FUNCTION carga.num_smallint(s text) RETURNS smallint
    LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT CASE
        WHEN s IS NULL OR btrim(s) = ''                       THEN NULL
        WHEN btrim(s) !~ '^[+-]?[0-9]{1,5}$'                  THEN NULL
        WHEN btrim(s)::integer NOT BETWEEN -32768 AND 32767   THEN NULL
        ELSE btrim(s)::integer::smallint
    END;
$$;

-- Mesma ideia: 10 dígitos sempre cabem em bigint, e a faixa do integer é
-- conferida depois. É o que trata o '99999999999' em cnae/município.
CREATE OR REPLACE FUNCTION carga.num_integer(s text) RETURNS integer
    LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT CASE
        WHEN s IS NULL OR btrim(s) = ''                                     THEN NULL
        WHEN btrim(s) !~ '^[+-]?[0-9]{1,10}$'                               THEN NULL
        WHEN btrim(s)::bigint NOT BETWEEN -2147483648 AND 2147483647        THEN NULL
        ELSE btrim(s)::bigint::integer
    END;
$$;

-- capital_social (S7 + S11): vírgula decimal vira ponto, vazio vira NULL.
-- O teto de 16 dígitos inteiros é o que numeric(18,2) comporta depois do
-- round(_, 2) — acima disso o INSERT estouraria por overflow numérico.
-- `'1.234,56'` (separador de milhar) vira `'1.234.56'`, não casa, e é rejeitado.
CREATE OR REPLACE FUNCTION carga.num_capital(s text) RETURNS numeric
    LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT CASE
        WHEN s IS NULL OR btrim(s) = ''                                         THEN NULL
        WHEN replace(btrim(s), ',', '.') !~ '^[+-]?[0-9]{1,16}(\.[0-9]+)?$'     THEN NULL
        ELSE round(replace(btrim(s), ',', '.')::numeric, 2)
    END;
$$;

-- S12: valor mais longo que a coluna vira NULL em vez de
-- "value too long for type character(n)".
CREATE OR REPLACE FUNCTION carga.cabe(s text, n integer) RETURNS text
    LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT CASE WHEN s IS NULL OR length(s) <= n THEN s END;
$$;

-- Predicado da varredura de rejeito: "havia conteúdo e ele se perdeu".
-- Distingue o NULL legítimo (campo vazio na origem, S3) do NULL que veio de um
-- cast recusado (S11/S12) — só o segundo é rejeito.
CREATE OR REPLACE FUNCTION carga.perdeu(bruto text, virou_nulo boolean) RETURNS boolean
    LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT bruto IS NOT NULL AND btrim(bruto) <> '' AND virou_nulo;
$$;

-- Idem para data: as sentinelas ('0', '00000000') viram NULL por regra (S6) e
-- NÃO são rejeito; o que sobra — data impossível, formato fora de AAAAMMDD — é.
--
-- Recebe o resultado da conversão em vez de chamar `analytics.parse_date`: este
-- arquivo é aplicado ANTES do 01_schema.sql, então o `analytics` ainda não
-- existe aqui. Inverter a ordem resolveria, mas amarraria o schema da carga ao
-- schema da API — e é justamente o contrário que a R1 pede.
CREATE OR REPLACE FUNCTION carga.perdeu_data(bruto text, virou_nulo boolean) RETURNS boolean
    LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT bruto IS NOT NULL
       AND btrim(bruto) NOT IN ('', '0', '00000000')
       AND virou_nulo;
$$;

-- ----------------------------------------------------------------------------
-- Auditoria
-- ----------------------------------------------------------------------------

-- Toda linha descartada por S4, S5, S9 ou S12, e toda célula zerada por S6/S11.
-- Sem índice: é tabela de escrita em massa e leitura eventual, e um índice aqui
-- só encareceria a carga que ela existe para observar.
CREATE TABLE IF NOT EXISTS carga.rejeito (
    competencia  text        NOT NULL,
    tabela       text        NOT NULL,
    regra        text        NOT NULL,   -- S4, S5, S6, S9, S11, S12
    coluna       text,                   -- NULL quando a linha inteira caiu
    linha_bruta  text        NOT NULL,   -- a linha da staging, como veio
    detectado_em timestamptz NOT NULL DEFAULT now()
);

-- Um contador por regra e por tabela, em toda carga — inclusive as regras que
-- NÃO descartam nada (S7, S8, S10). É o que responde "a sanitização continua
-- pegando a mesma ordem de grandeza de sempre?" sem ninguém abrir o rejeito.
CREATE TABLE IF NOT EXISTS carga.contador (
    competencia  text        NOT NULL,
    tabela       text        NOT NULL,
    regra        text        NOT NULL,
    quantidade   bigint      NOT NULL,
    registrado_em timestamptz NOT NULL DEFAULT now()
);

-- Quarentena da 7.1: chave natural repetida num mês. A carga NÃO para por
-- causa disso — o índice sai não-único e a carga termina em sucesso degradado.
CREATE TABLE IF NOT EXISTS carga.duplicata (
    competencia  text        NOT NULL,
    tabela       text        NOT NULL,
    chave        text        NOT NULL,
    ocorrencias  bigint      NOT NULL,
    detectado_em timestamptz NOT NULL DEFAULT now()
);

-- DDL dos índices de `analytics`, salva pela Fase 2 antes do DROP e relida pela
-- Fase 4 e pela recuperação do trap. É a ÚNICA cópia: não existe uma segunda
-- definição em lugar nenhum que possa divergir do 04_indexes.sql — e é assim
-- que a Fase 4 preserva o R1.
CREATE TABLE IF NOT EXISTS carga.indice_salvo (
    schema_nome     text        NOT NULL,
    tabela          text        NOT NULL,
    indice          text        NOT NULL,
    definicao       text        NOT NULL,   -- pg_get_indexdef / ADD CONSTRAINT
    e_constraint    boolean     NOT NULL DEFAULT false,
    dropado         boolean     NOT NULL DEFAULT false,
    salvo_em        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (schema_nome, indice)
);

-- Uma linha por execução da carga: o baseline da Fase 0 e o desfecho.
-- `desfecho` tem TRÊS estados, e o terceiro existe por causa da 7.1: um mês com
-- duplicata termina 'degradado', não 'falha' — assim o watcher não reagenda uma
-- recarga de horas por causa de uma linha.
CREATE TABLE IF NOT EXISTS carga.resumo (
    id             bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    competencia    text        NOT NULL,
    inicio         timestamptz NOT NULL DEFAULT now(),
    fim            timestamptz,
    versao_codigo  text,                   -- git rev do repo que rodou
    parametros     jsonb,                  -- LOAD_JOBS, work_mem, ... derivados na Fase 0
    recursos       jsonb,                  -- nproc, memória livre, load average, disco
    pico_rss_mb    integer,                -- T12: o host não tem swap
    desfecho       text        CHECK (desfecho IN ('sucesso', 'degradado', 'falha')),
    observacoes    text
);

-- Abre (ou reaproveita) o resumo da carga corrente e devolve o id.
-- Reaproveitar importa: o load.sh abre o resumo na Fase 0, e o 03_transform
-- roda depois, numa sessão sua — os dois têm de escrever na MESMA linha.
CREATE OR REPLACE FUNCTION carga.resumo_atual() RETURNS bigint
    LANGUAGE plpgsql AS $$
DECLARE
    v_id bigint;
BEGIN
    SELECT id INTO v_id FROM carga.resumo
     WHERE competencia = carga.competencia() AND fim IS NULL
     ORDER BY inicio DESC LIMIT 1;
    IF v_id IS NULL THEN
        INSERT INTO carga.resumo (competencia) VALUES (carga.competencia())
        RETURNING id INTO v_id;
    END IF;
    RETURN v_id;
END;
$$;

-- Registra um contador, somando se a mesma regra já foi contada nesta carga
-- (a Fase 3 em blocos chama isto uma vez por bloco).
CREATE OR REPLACE FUNCTION carga.contar(p_tabela text, p_regra text, p_qtd bigint)
    RETURNS void LANGUAGE sql AS $$
    INSERT INTO carga.contador (competencia, tabela, regra, quantidade)
    VALUES (carga.competencia(), p_tabela, p_regra, coalesce(p_qtd, 0));
$$;

-- Apaga a auditoria da competência corrente. Chamado no início do transform:
-- sem isto, reexecutar a carga do mesmo mês empilharia rejeito sobre rejeito e
-- o contador deixaria de significar "o que este mês descartou".
CREATE OR REPLACE FUNCTION carga.limpar_competencia() RETURNS void
    LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM carga.rejeito   WHERE competencia = carga.competencia();
    DELETE FROM carga.contador  WHERE competencia = carga.competencia();
    DELETE FROM carga.duplicata WHERE competencia = carga.competencia();
END;
$$;

-- ----------------------------------------------------------------------------
-- Reconstrução de índice (Fase 4)
--
-- Mora aqui, e não no fase4_indices.sql, por um motivo prático: o load.sh chama
-- `carga.recriar_indice` em IDX_JOBS sessões simultâneas, e para isso a função
-- tem de existir antes de a Fase 4 começar. São 212 índices independentes —
-- nenhum ON CONFLICT, nenhuma ordem a preservar —, o paralelismo mais barato da
-- carga inteira.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION carga.tabela_suja(p_tabela text) RETURNS boolean
    LANGUAGE sql STABLE AS $fn$
    SELECT EXISTS (SELECT 1 FROM carga.duplicata
                    WHERE competencia = carga.competencia() AND tabela = p_tabela);
$fn$;

-- Varre a fonte e põe as chaves repetidas em quarentena.
--
-- ⚠️ É CARA — e é por isso que só roda quando já se sabe que o mês veio sujo.
-- Na carga completa de 16/09/2026 estas varreduras preventivas custaram 51
-- minutos (`GROUP BY` sobre 71,9 milhões de estabelecimentos e 49 milhões de
-- linhas de simples, derramando em disco) para encontrar UMA duplicata. Hoje
-- quem descobre a duplicata é o próprio `CREATE UNIQUE INDEX`, de graça; esta
-- função só é chamada depois que ele falha.
CREATE OR REPLACE FUNCTION carga.quarentenar(p_tabela text) RETURNS bigint
    LANGUAGE plpgsql AS $fn$
DECLARE
    v_qtd bigint := 0;
BEGIN
    DELETE FROM carga.duplicata
     WHERE competencia = carga.competencia() AND tabela = p_tabela;

    IF p_tabela = 'estabelecimento' THEN
        INSERT INTO carga.duplicata (competencia, tabela, chave, ocorrencias)
        SELECT carga.competencia(), 'estabelecimento',
               (cnpj_basico || cnpj_ordem || cnpj_dv) || '|'
                   || coalesce(nullif(btrim(uf), ''), '??'),
               count(*)
        FROM staging.estabelecimentos
        WHERE cnpj_basico ~ '^\d{8}$'
          AND length(cnpj_basico || cnpj_ordem || cnpj_dv) <= 14
        GROUP BY 2, 3 HAVING count(*) > 1;
    ELSIF p_tabela = 'simples' THEN
        INSERT INTO carga.duplicata (competencia, tabela, chave, ocorrencias)
        SELECT carga.competencia(), 'simples', cnpj_basico, count(*)
        FROM staging.simples
        WHERE cnpj_basico ~ '^\d{8}$'
        GROUP BY 2, 3 HAVING count(*) > 1;
    ELSIF p_tabela = 'empresa' THEN
        INSERT INTO carga.duplicata (competencia, tabela, chave, ocorrencias)
        SELECT carga.competencia(), 'empresa', cnpj_basico, count(*)
        FROM staging.empresas
        WHERE cnpj_basico ~ '^\d{8}$'
        GROUP BY 2, 3 HAVING count(*) > 1;
    ELSIF p_tabela = 'estabelecimento_cnae_secundario' THEN
        -- A chave aqui nasce de um unnest, então a "fonte" é a própria tabela.
        INSERT INTO carga.duplicata (competencia, tabela, chave, ocorrencias)
        SELECT carga.competencia(), p_tabela, cnpj || '|' || cnae_cod, count(*)
        FROM analytics.estabelecimento_cnae_secundario
        GROUP BY 2, 3 HAVING count(*) > 1;
    END IF;

    GET DIAGNOSTICS v_qtd = ROW_COUNT;
    RETURN v_qtd;
END;
$fn$;

CREATE OR REPLACE FUNCTION carga.recriar_indice(p_indice text) RETURNS text
    LANGUAGE plpgsql AS $fn$
DECLARE
    r      carga.indice_salvo%ROWTYPE;
    v_dups bigint;
BEGIN
    SELECT * INTO r FROM carga.indice_salvo
     WHERE schema_nome = 'analytics' AND indice = p_indice;
    IF NOT FOUND THEN
        RETURN format('%s: sem definição salva — nada a fazer', p_indice);
    END IF;

    -- Já existe? Então a Fase 2 não chegou a dropá-lo, ou outra sessão já o
    -- recriou. Sair sem fazer nada é o que torna esta função reexecutável — e é
    -- do que a recuperação do trap depende.
    IF to_regclass(format('analytics.%I', r.indice)) IS NOT NULL THEN
        UPDATE carga.indice_salvo SET dropado = false
         WHERE schema_nome = 'analytics' AND indice = r.indice;
        RETURN format('%s: já existe', r.indice);
    END IF;

    -- Índice comum: nada a arbitrar.
    IF NOT r.e_constraint THEN
        EXECUTE r.definicao;
        UPDATE carga.indice_salvo SET dropado = false
         WHERE schema_nome = 'analytics' AND indice = r.indice;
        RETURN format('%s: recriado', r.indice);
    END IF;

    -- Constraint única: TENTA criar. É aqui que a duplicata do mês aparece, e
    -- de graça — o `CREATE UNIQUE INDEX` varre a tabela inteira de qualquer
    -- forma para construir o índice, então a verificação de unicidade não custa
    -- nada além do que já seria pago. Era isto ou varrer a fonte antes para
    -- decidir, que foi o desenho anterior e custou 51 minutos por carga.
    --
    -- ⚠️ Sobre o bloco EXCEPTION, que a spec proíbe em OUTRO contexto: a
    -- proibição da R2.1 é sobre EXCEPTION por LINHA — 73 milhões de
    -- subtransações. Aqui é uma por índice, no máximo 212 numa carga inteira.
    -- São coisas diferentes e a distinção importa.
    BEGIN
        EXECUTE r.definicao;
        UPDATE carga.indice_salvo SET dropado = false
         WHERE schema_nome = 'analytics' AND indice = r.indice;
        RETURN format('%s: recriado', r.indice);
    EXCEPTION WHEN unique_violation THEN
        -- Mês sujo. AGORA vale varrer a fonte: a varredura deixou de ser um
        -- custo fixo de toda carga e virou o preço de um mês excepcional.
        v_dups := carga.quarentenar(r.tabela);
        -- A constraint única é impossível; vira índice NÃO-ÚNICO com o MESMO
        -- nome, para a API continuar com o mesmo plano de acesso. Isto tensiona
        -- o R1 e é desvio conhecido, registrado e temporário: a carga seguinte
        -- volta ao índice único sozinha se o mês vier limpo.
        EXECUTE format('CREATE INDEX %I ON analytics.%I (%s)',
                       r.indice, r.tabela,
                       substring(r.definicao from '\((.*)\)$'));
        UPDATE carga.indice_salvo SET dropado = false
         WHERE schema_nome = 'analytics' AND indice = r.indice;
        RETURN format('%s: criado NÃO-ÚNICO — %s chave(s) repetida(s) em %s',
                      r.indice, v_dups, r.tabela);
    END;
END;
$fn$;

-- Visão de leitura humana: o que esta carga descartou, por regra.
CREATE OR REPLACE VIEW carga.resumo_regras AS
SELECT competencia, tabela, regra, sum(quantidade) AS quantidade
  FROM carga.contador
 GROUP BY competencia, tabela, regra;
