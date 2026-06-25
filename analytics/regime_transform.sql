-- ============================================================================
-- regime_transform.sql — regime tributário (entidades-*.zip): tabela + transform
--                        + índices, num único arquivo idempotente.
--
-- Fonte SEPARADA da Receita (share Nextcloud token MPPfFit7g7zdA8C). Usado tanto
-- pela carga completa (load.sh) quanto pela carga incremental (REGIME_ONLY=1).
-- Pré-requisito: staging.regime_tributario já populada (COPY feito pelo load.sh).
--
-- Grão = (cnpj completo, ano, forma). A ordem do CNPJ varia (não é só matriz) e
-- há SCP, então NÃO dá p/ chavear por cnpj_basico — usa-se PK surrogate (id).
-- DISTINCT remove duplicatas exatas que aparecem entre os arquivos.
-- ============================================================================

\set ON_ERROR_STOP on

CREATE TABLE IF NOT EXISTS analytics.regime_tributario (
    id                   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    cnpj                 char(14) NOT NULL,        -- CNPJ completo
    cnpj_basico          char(8)  NOT NULL,        -- join com empresa
    ano                  smallint NOT NULL,
    forma_de_tributacao  text     NOT NULL,        -- LUCRO REAL/PRESUMIDO/ARBITRADO, ISENTO/IMUNE...
    qtd_escrituracoes    integer,
    cnpj_da_scp          char(14)                  -- '0' na origem -> NULL
) WITH (fillfactor = 100);

TRUNCATE analytics.regime_tributario RESTART IDENTITY;
INSERT INTO analytics.regime_tributario
    (cnpj, cnpj_basico, ano, forma_de_tributacao, qtd_escrituracoes, cnpj_da_scp)
SELECT DISTINCT
    cnpj14::char(14),
    substr(cnpj14, 1, 8)::char(8),
    nullif(ano, '')::smallint,
    forma_de_tributacao,
    nullif(quantidade_de_escrituracoes, '')::integer,
    nullif(nullif(cnpj_da_scp, ''), '0')::char(14)
FROM (
    SELECT regexp_replace(cnpj, '\D', '', 'g') AS cnpj14, *
    FROM staging.regime_tributario
) s
WHERE length(cnpj14) = 14
  AND nullif(ano, '') IS NOT NULL
  AND nullif(forma_de_tributacao, '') IS NOT NULL;

-- índices: join por empresa/CNPJ e filtro por ano+forma
CREATE INDEX IF NOT EXISTS ix_regime_cnpj_basico
    ON analytics.regime_tributario (cnpj_basico);
CREATE INDEX IF NOT EXISTS ix_regime_cnpj
    ON analytics.regime_tributario (cnpj);
CREATE INDEX IF NOT EXISTS ix_regime_ano_forma
    ON analytics.regime_tributario (ano, forma_de_tributacao);

DROP TABLE IF EXISTS staging.regime_tributario;
VACUUM ANALYZE analytics.regime_tributario;
