-- ============================================================================
-- 04_indexes.sql — índices, estatísticas estendidas e ANALYZE
-- Rodar SÓ DEPOIS da carga (03_transform.sql): criar índices no fim é ordens
-- de magnitude mais rápido que mantê-los vivos durante o COPY/INSERT.
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- empresa: agregação de capital por natureza, com cobertura p/ evitar heap fetch
CREATE INDEX IF NOT EXISTS ix_empresa_natureza
    ON analytics.empresa (natureza_juridica_cod) INCLUDE (capital_social);
CREATE INDEX IF NOT EXISTS ix_empresa_porte
    ON analytics.empresa (porte_cod);
CREATE INDEX IF NOT EXISTS ix_empresa_razao_trgm
    ON analytics.empresa USING gin (razao_social gin_trgm_ops);

-- estabelecimento: criados no parent, propagam p/ todas as partições
CREATE INDEX IF NOT EXISTS ix_estab_cnpj_basico
    ON analytics.estabelecimento (cnpj_basico);                 -- join p/ empresa/sócio
CREATE INDEX IF NOT EXISTS ix_estab_cnae_situacao
    ON analytics.estabelecimento (cnae_fiscal_principal, situacao_cadastral);
CREATE INDEX IF NOT EXISTS ix_estab_municipio
    ON analytics.estabelecimento (municipio_cod);
-- "só ATIVAS" é o filtro dominante -> índice parcial (bem menor)
CREATE INDEX IF NOT EXISTS ix_estab_cnae_ativas
    ON analytics.estabelecimento (cnae_fiscal_principal)
    WHERE situacao_cadastral = 2;
-- séries de aberturas: BRIN minúsculo p/ data
CREATE INDEX IF NOT EXISTS ix_estab_inicio_brin
    ON analytics.estabelecimento USING brin (data_inicio_atividade);
CREATE INDEX IF NOT EXISTS ix_estab_fantasia_trgm
    ON analytics.estabelecimento USING gin (nome_fantasia gin_trgm_ops);

-- CNAE secundário: busca reversa (quem tem CNAE X)
CREATE INDEX IF NOT EXISTS ix_estab_cnae_sec_cnae
    ON analytics.estabelecimento_cnae_secundario (cnae_cod);

-- sócio: rede societária
CREATE INDEX IF NOT EXISTS ix_socio_doc
    ON analytics.socio (cnpj_cpf_socio);
CREATE INDEX IF NOT EXISTS ix_socio_cnpj_basico
    ON analytics.socio (cnpj_basico);
CREATE INDEX IF NOT EXISTS ix_socio_nome_trgm
    ON analytics.socio USING gin (nome_socio gin_trgm_ops);

-- regime tributário: join por empresa/CNPJ e filtro por ano+forma
CREATE INDEX IF NOT EXISTS ix_regime_cnpj_basico
    ON analytics.regime_tributario (cnpj_basico);
CREATE INDEX IF NOT EXISTS ix_regime_cnpj
    ON analytics.regime_tributario (cnpj);
CREATE INDEX IF NOT EXISTS ix_regime_ano_forma
    ON analytics.regime_tributario (ano, forma_de_tributacao);

-- Estatísticas estendidas para colunas correlacionadas (planner subestima sem isto)
CREATE STATISTICS IF NOT EXISTS st_estab_geo
    ON uf, municipio_cod FROM analytics.estabelecimento;
CREATE STATISTICS IF NOT EXISTS st_estab_cnae_sit
    ON cnae_fiscal_principal, situacao_cadastral FROM analytics.estabelecimento;

VACUUM ANALYZE analytics.empresa;
VACUUM ANALYZE analytics.estabelecimento;
VACUUM ANALYZE analytics.estabelecimento_cnae_secundario;
VACUUM ANALYZE analytics.socio;
VACUUM ANALYZE analytics.simples;
VACUUM ANALYZE analytics.regime_tributario;
