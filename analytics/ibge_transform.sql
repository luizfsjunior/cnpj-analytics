-- ============================================================================
-- ibge_transform.sql — preenche analytics.dim_municipio.codigo_ibge / uf
--
-- O Municipios.csv da Receita traz apenas (codigo SIAFI 4 díg., nome) — sem UF e
-- sem código IBGE. A ponte é feita por duas fontes baixadas pelo load.sh:
--
--   staging.tabmun            TABMUN do Tesouro Nacional (CKAN): SIAFI -> IBGE + UF.
--                             Fonte PRIMÁRIA: de-para oficial, cobre 5.570 municípios.
--   staging.ibge_municipios   API de localidades do IBGE: (codigo_ibge, nome).
--                             Fonte de FALLBACK por nome, para municípios novos que o
--                             TABMUN ainda não publicou (ex.: Boa Esperança do Norte/MT,
--                             SIAFI 1182 -> IBGE 5101837, ausente do TABMUN em 2026-09).
--
-- Rodar DEPOIS do 03_transform.sql (que TRUNCATE-a a dim_municipio).
-- Idempotente: só faz UPDATE em cima da dimensão já carregada.
-- ============================================================================

\set ON_ERROR_STOP on
-- Este arquivo tem literais acentuados (a tabela de translate abaixo): garante a
-- leitura correta mesmo se o client_encoding da sessão não for UTF8.
SET client_encoding = 'UTF8';

-- Normalização p/ casar nomes entre fontes: maiúsculas, sem acento e só [A-Z0-9].
-- ("Alta Floresta D'Oeste" e "ALTA FLORESTA D OESTE" -> ALTAFLORESTADOESTE)
CREATE OR REPLACE FUNCTION analytics.norm_municipio(txt text)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
    SELECT regexp_replace(
               translate(upper(txt),
                         'ÁÀÂÃÄÅÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇÑ',
                         'AAAAAAEEEEIIIIOOOOOUUUUCN'),
               '[^A-Z0-9]', '', 'g')
$$;

-- ----------------------------------------------------------------------------
-- 1) fonte primária: TABMUN (SIAFI -> IBGE + UF)
--    Descarta as 19 linhas "DEMAIS MUNICIPIOS", que vêm com codigo_ibge 0000000.
-- ----------------------------------------------------------------------------
UPDATE analytics.dim_municipio m
SET codigo_ibge = t.codigo_ibge,
    uf          = t.uf
FROM (
    SELECT trim(codigo_siafi)::integer AS codigo_siafi,
           trim(codigo_ibge)::integer  AS codigo_ibge,
           trim(uf)                    AS uf
    FROM staging.tabmun
    WHERE trim(codigo_siafi) ~ '^\d+$'
      AND trim(codigo_ibge)  ~ '^\d{7}$'
      AND trim(codigo_ibge) <> '0000000'
) t
WHERE m.codigo = t.codigo_siafi;

-- ----------------------------------------------------------------------------
-- 2) fallback: casa por NOME normalizado contra a lista do IBGE, só para o que
--    sobrou sem código. Exige match ÚNICO no país — homônimo fica NULL de
--    propósito (melhor um furo visível que um município errado).
-- ----------------------------------------------------------------------------
WITH ibge_unico AS (
    SELECT analytics.norm_municipio(nome) AS chave,
           min(codigo_ibge)               AS codigo_ibge
    FROM staging.ibge_municipios
    GROUP BY 1
    HAVING count(*) = 1
)
UPDATE analytics.dim_municipio m
SET codigo_ibge = i.codigo_ibge
FROM ibge_unico i
WHERE m.codigo_ibge IS NULL
  AND analytics.norm_municipio(m.nome) = i.chave;

-- ----------------------------------------------------------------------------
-- 3) UF de quem veio pelo fallback: os 2 primeiros dígitos do código IBGE são a
--    UF (11=RO ... 53=DF), então não precisa de outra fonte.
-- ----------------------------------------------------------------------------
UPDATE analytics.dim_municipio m
SET uf = u.sigla
FROM (VALUES
    (11,'RO'),(12,'AC'),(13,'AM'),(14,'RR'),(15,'PA'),(16,'AP'),(17,'TO'),
    (21,'MA'),(22,'PI'),(23,'CE'),(24,'RN'),(25,'PB'),(26,'PE'),(27,'AL'),(28,'SE'),(29,'BA'),
    (31,'MG'),(32,'ES'),(33,'RJ'),(35,'SP'),
    (41,'PR'),(42,'SC'),(43,'RS'),
    (50,'MS'),(51,'MT'),(52,'GO'),(53,'DF')
) AS u(prefixo, sigla)
WHERE m.uf IS NULL
  AND m.codigo_ibge IS NOT NULL
  AND m.codigo_ibge / 100000 = u.prefixo;

-- ----------------------------------------------------------------------------
-- 4) "EXTERIOR" (SIAFI 9707) não é município: não tem IBGE. Marca a UF como 'EX',
--    a mesma sigla usada na partição analytics.estabelecimento_ex.
-- ----------------------------------------------------------------------------
UPDATE analytics.dim_municipio
SET uf = 'EX'
WHERE uf IS NULL
  AND codigo_ibge IS NULL
  AND analytics.norm_municipio(nome) = 'EXTERIOR';

ANALYZE analytics.dim_municipio;

-- ----------------------------------------------------------------------------
-- Validações — falham a carga em vez de deixar a dimensão silenciosamente furada.
-- ----------------------------------------------------------------------------
DO $$
DECLARE
    v_total       bigint;
    v_sem_ibge    bigint;
    v_sem_uf      bigint;
    v_ibge_dup    bigint;
    v_sp          integer;
    v_rj          integer;
    v_faltantes   text;
BEGIN
    SELECT count(*) INTO v_total FROM analytics.dim_municipio;
    IF v_total < 5570 THEN
        RAISE EXCEPTION 'dim_municipio tem % linhas; esperado >= 5570 (Municipios.zip carregado?)', v_total;
    END IF;

    -- Só 'EXTERIOR' pode ficar sem código IBGE.
    SELECT count(*), string_agg(codigo || '=' || nome, ', ' ORDER BY codigo)
      INTO v_sem_ibge, v_faltantes
      FROM analytics.dim_municipio
     WHERE codigo_ibge IS NULL
       AND analytics.norm_municipio(nome) <> 'EXTERIOR';
    IF v_sem_ibge > 0 THEN
        RAISE EXCEPTION 'municípios sem codigo_ibge (%): %', v_sem_ibge, v_faltantes;
    END IF;

    SELECT count(*) INTO v_sem_uf FROM analytics.dim_municipio WHERE uf IS NULL;
    IF v_sem_uf > 0 THEN
        RAISE EXCEPTION '% municípios sem UF', v_sem_uf;
    END IF;

    -- Dois códigos da Receita apontando para o mesmo IBGE = de-para furado.
    SELECT count(*) INTO v_ibge_dup FROM (
        SELECT codigo_ibge FROM analytics.dim_municipio
        WHERE codigo_ibge IS NOT NULL GROUP BY 1 HAVING count(*) > 1
    ) d;
    IF v_ibge_dup > 0 THEN
        RAISE EXCEPTION '% códigos IBGE repetidos em dim_municipio', v_ibge_dup;
    END IF;

    -- Âncoras conhecidas.
    SELECT codigo_ibge INTO v_sp FROM analytics.dim_municipio WHERE codigo = 7107;
    IF v_sp IS DISTINCT FROM 3550308 THEN
        RAISE EXCEPTION 'São Paulo (SIAFI 7107) mapeou para % (esperado 3550308)', v_sp;
    END IF;
    SELECT codigo_ibge INTO v_rj FROM analytics.dim_municipio WHERE codigo = 6001;
    IF v_rj IS DISTINCT FROM 3304557 THEN
        RAISE EXCEPTION 'Rio de Janeiro (SIAFI 6001) mapeou para % (esperado 3304557)', v_rj;
    END IF;

    RAISE NOTICE 'dim_municipio: % linhas, % com codigo_ibge',
        v_total, (SELECT count(codigo_ibge) FROM analytics.dim_municipio);
END $$;

-- Só depois das validações: o índice ratifica a unicidade do de-para e serve os
-- lookups por código IBGE (ex.: /stats/empresas?municipio_ibge=3550308).
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_municipio_ibge
    ON analytics.dim_municipio (codigo_ibge) WHERE codigo_ibge IS NOT NULL;
