-- ============================================================================
-- 03_transform.sql — converte staging (text) -> analytics (tipado)
-- Rodar APÓS o COPY bruto (load.sh). Dimensões primeiro, depois os fatos.
-- Idempotente: TRUNCATE nos destinos antes de inserir.
-- ============================================================================

\set ON_ERROR_STOP on

-- ----------------------------------------------------------------------------
-- Dimensões
-- ----------------------------------------------------------------------------
TRUNCATE analytics.dim_cnae, analytics.dim_natureza_juridica,
         analytics.dim_qualificacao, analytics.dim_pais,
         analytics.dim_motivo_situacao, analytics.dim_municipio;

INSERT INTO analytics.dim_cnae (codigo, descricao)
SELECT codigo::integer, descricao FROM staging.cnaes
WHERE codigo ~ '^\d+$' ON CONFLICT DO NOTHING;

INSERT INTO analytics.dim_natureza_juridica (codigo, descricao)
SELECT codigo::smallint, descricao FROM staging.naturezas
WHERE codigo ~ '^\d+$' ON CONFLICT DO NOTHING;

INSERT INTO analytics.dim_qualificacao (codigo, descricao)
SELECT codigo::smallint, descricao FROM staging.qualificacoes
WHERE codigo ~ '^\d+$' ON CONFLICT DO NOTHING;

INSERT INTO analytics.dim_pais (codigo, nome)
SELECT codigo::smallint, descricao FROM staging.paises
WHERE codigo ~ '^\d+$' ON CONFLICT DO NOTHING;

INSERT INTO analytics.dim_motivo_situacao (codigo, descricao)
SELECT codigo::smallint, descricao FROM staging.motivos
WHERE codigo ~ '^\d+$' ON CONFLICT DO NOTHING;

INSERT INTO analytics.dim_municipio (codigo, nome)
SELECT codigo::integer, descricao FROM staging.municipios
WHERE codigo ~ '^\d+$' ON CONFLICT DO NOTHING;

-- ----------------------------------------------------------------------------
-- empresa
-- ----------------------------------------------------------------------------
TRUNCATE analytics.empresa;
INSERT INTO analytics.empresa
    (cnpj_basico, razao_social, natureza_juridica_cod, qualificacao_resp_cod,
     capital_social, porte_cod, ente_federativo)
SELECT
    cnpj_basico,
    nullif(razao_social, ''),
    nullif(natureza_juridica, '')::smallint,
    nullif(qualificacao_resp, '')::smallint,
    nullif(replace(capital_social, ',', '.'), '')::numeric(18,2),
    nullif(porte, '')::smallint,
    nullif(ente_federativo, '')
FROM staging.empresas
WHERE cnpj_basico ~ '^\d{8}$'
ON CONFLICT (cnpj_basico) DO NOTHING;

-- ----------------------------------------------------------------------------
-- estabelecimento
-- ----------------------------------------------------------------------------
TRUNCATE analytics.estabelecimento;
INSERT INTO analytics.estabelecimento (
    cnpj, cnpj_basico, matriz_filial, nome_fantasia, situacao_cadastral,
    data_situacao_cadastral, motivo_situacao_cod, nome_cidade_exterior, pais_cod,
    data_inicio_atividade, cnae_fiscal_principal, tipo_logradouro, logradouro,
    numero, complemento, bairro, cep, uf, municipio_cod, ddd_telefone_1,
    ddd_telefone_2, ddd_fax, email, situacao_especial, data_situacao_especial)
SELECT
    cnpj_basico || cnpj_ordem || cnpj_dv,
    cnpj_basico,
    nullif(identificador_matriz_filial, '')::smallint,
    nullif(nome_fantasia, ''),
    nullif(situacao_cadastral, '')::smallint,
    analytics.parse_date(data_situacao_cadastral),
    nullif(motivo_situacao, '')::smallint,
    nullif(nome_cidade_exterior, ''),
    nullif(pais, '')::smallint,
    analytics.parse_date(data_inicio_atividade),
    nullif(cnae_principal, '')::integer,
    nullif(tipo_logradouro, ''),
    nullif(logradouro, ''),
    nullif(numero, ''),
    nullif(complemento, ''),
    nullif(bairro, ''),
    nullif(cep, ''),
    coalesce(nullif(btrim(uf), ''), '??'),               -- DEFAULT partition pega '??'
    nullif(municipio, '')::integer,
    nullif(btrim(ddd_1 || telefone_1), ''),
    nullif(btrim(ddd_2 || telefone_2), ''),
    nullif(btrim(ddd_fax || fax), ''),
    nullif(correio_eletronico, ''),
    nullif(situacao_especial, ''),
    analytics.parse_date(data_situacao_especial)
FROM staging.estabelecimentos
WHERE cnpj_basico ~ '^\d{8}$'
ON CONFLICT (cnpj, uf) DO NOTHING;

-- ----------------------------------------------------------------------------
-- estabelecimento_cnae_secundario (explode a lista separada por vírgula)
-- ----------------------------------------------------------------------------
TRUNCATE analytics.estabelecimento_cnae_secundario;
INSERT INTO analytics.estabelecimento_cnae_secundario (cnpj, cnae_cod)
SELECT cnpj, cnae::integer
FROM (
    SELECT cnpj_basico || cnpj_ordem || cnpj_dv AS cnpj,
           btrim(unnest(string_to_array(nullif(cnae_secundaria, ''), ','))) AS cnae
    FROM staging.estabelecimentos
    WHERE cnpj_basico ~ '^\d{8}$' AND nullif(cnae_secundaria, '') IS NOT NULL
) s
WHERE cnae ~ '^\d+$'
ON CONFLICT DO NOTHING;

-- ----------------------------------------------------------------------------
-- socio
-- ----------------------------------------------------------------------------
TRUNCATE analytics.socio RESTART IDENTITY;
INSERT INTO analytics.socio (
    cnpj_basico, identificador_socio, nome_socio, cnpj_cpf_socio,
    qualificacao_socio_cod, data_entrada_sociedade, pais_cod, cpf_representante,
    nome_representante, qualificacao_repr_cod, faixa_etaria_cod)
SELECT
    cnpj_basico,
    nullif(identificador_socio, '')::smallint,
    nullif(nome_socio, ''),
    nullif(cnpj_cpf_socio, ''),
    nullif(qualificacao_socio, '')::smallint,
    analytics.parse_date(data_entrada_sociedade),
    nullif(pais, '')::smallint,
    nullif(cpf_representante, ''),
    nullif(nome_representante, ''),
    nullif(qualificacao_repr, '')::smallint,
    nullif(faixa_etaria, '')::smallint
FROM staging.socios
WHERE cnpj_basico ~ '^\d{8}$';

-- ----------------------------------------------------------------------------
-- simples
-- ----------------------------------------------------------------------------
TRUNCATE analytics.simples;
INSERT INTO analytics.simples (
    cnpj_basico, opcao_simples, data_opcao_simples, data_exclusao_simples,
    opcao_mei, data_opcao_mei, data_exclusao_mei)
SELECT
    cnpj_basico,
    CASE upper(nullif(opcao_simples, '')) WHEN 'S' THEN true WHEN 'N' THEN false END,
    analytics.parse_date(data_opcao_simples),
    analytics.parse_date(data_exclusao_simples),
    CASE upper(nullif(opcao_mei, '')) WHEN 'S' THEN true WHEN 'N' THEN false END,
    analytics.parse_date(data_opcao_mei),
    analytics.parse_date(data_exclusao_mei)
FROM staging.simples
WHERE cnpj_basico ~ '^\d{8}$'
ON CONFLICT (cnpj_basico) DO NOTHING;

-- regime_tributario: fonte/cadência próprias -> transform isolado em
-- analytics/regime_transform.sql (chamado pelo load.sh após os índices).
