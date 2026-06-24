-- ============================================================================
-- Minha Receita — modelo analítico (schema `analytics`)
-- 01_schema.sql — dimensões, fatos e partições (SEM índices; ver 04_indexes.sql)
--
-- Convive com a tabela `cnpj` (jsonb) da API; não a substitui.
-- Aplicar via:
--   docker compose exec -T postgres psql -U minhareceita -d minhareceita \
--     -v ON_ERROR_STOP=1 < analytics/01_schema.sql
--
-- Nota de integridade: NÃO há FOREIGN KEYs enforced contra as dimensões.
-- Os dados abertos da Receita contêm códigos ausentes nos lookups (ex.: país
-- 367, municípios de exterior), o que faria a carga falhar. As colunas *_cod
-- ficam indexadas (04_indexes.sql) para joins baratos, mas a integridade
-- referencial é declarada apenas por documentação.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS analytics;

-- Helper: datas no formato AAAAMMDD, com '0'/'00000000'/'' representando nulo.
CREATE OR REPLACE FUNCTION analytics.parse_date(s text) RETURNS date
    LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT CASE
        WHEN s IS NULL OR btrim(s) IN ('', '0', '00000000') THEN NULL
        ELSE to_date(s, 'YYYYMMDD')
    END;
$$;

-- ----------------------------------------------------------------------------
-- Dimensões carregadas dos CSVs de lookup (codigo;descricao)
-- ----------------------------------------------------------------------------
CREATE TABLE analytics.dim_natureza_juridica (
    codigo      smallint PRIMARY KEY,
    descricao   text NOT NULL
);

CREATE TABLE analytics.dim_cnae (
    codigo      integer PRIMARY KEY,           -- 7 dígitos, ex. 6201501
    descricao   text NOT NULL
);

CREATE TABLE analytics.dim_municipio (
    codigo       integer PRIMARY KEY,          -- código Receita (4 díg.)
    nome         text NOT NULL,
    codigo_ibge  integer,                      -- preenchido depois (tabmun IBGE)
    uf           char(2)                       -- idem (não vem no Municipios.csv)
);

CREATE TABLE analytics.dim_pais (
    codigo      smallint PRIMARY KEY,
    nome        text NOT NULL
);

CREATE TABLE analytics.dim_qualificacao (      -- sócio / responsável / representante
    codigo      smallint PRIMARY KEY,
    descricao   text NOT NULL
);

CREATE TABLE analytics.dim_motivo_situacao (
    codigo      smallint PRIMARY KEY,
    descricao   text NOT NULL
);

-- ----------------------------------------------------------------------------
-- Dimensões de domínio (valores fixos, semeados aqui para joins legíveis)
-- ----------------------------------------------------------------------------
CREATE TABLE analytics.dim_situacao_cadastral (codigo smallint PRIMARY KEY, descricao text NOT NULL);
INSERT INTO analytics.dim_situacao_cadastral VALUES
    (1,'Nula'),(2,'Ativa'),(3,'Suspensa'),(4,'Inapta'),(8,'Baixada');

CREATE TABLE analytics.dim_matriz_filial (codigo smallint PRIMARY KEY, descricao text NOT NULL);
INSERT INTO analytics.dim_matriz_filial VALUES (1,'Matriz'),(2,'Filial');

CREATE TABLE analytics.dim_porte (codigo smallint PRIMARY KEY, descricao text NOT NULL);
INSERT INTO analytics.dim_porte VALUES
    (0,'Não informado'),(1,'Micro empresa'),(3,'Empresa de pequeno porte'),(5,'Demais');

CREATE TABLE analytics.dim_identificador_socio (codigo smallint PRIMARY KEY, descricao text NOT NULL);
INSERT INTO analytics.dim_identificador_socio VALUES
    (1,'Pessoa jurídica'),(2,'Pessoa física'),(3,'Estrangeiro');

CREATE TABLE analytics.dim_faixa_etaria (codigo smallint PRIMARY KEY, descricao text NOT NULL);
INSERT INTO analytics.dim_faixa_etaria VALUES
    (0,'Não se aplica'),(1,'0 a 12 anos'),(2,'13 a 20 anos'),(3,'21 a 30 anos'),
    (4,'31 a 40 anos'),(5,'41 a 50 anos'),(6,'51 a 60 anos'),(7,'61 a 70 anos'),
    (8,'71 a 80 anos'),(9,'Maiores de 80 anos');

-- ----------------------------------------------------------------------------
-- Fato 1: empresa (grão = CNPJ básico, 8 díg.)
-- ----------------------------------------------------------------------------
CREATE TABLE analytics.empresa (
    cnpj_basico              char(8) PRIMARY KEY,
    razao_social             text,
    natureza_juridica_cod    smallint,
    qualificacao_resp_cod    smallint,
    capital_social           numeric(18,2),
    porte_cod                smallint,
    ente_federativo          text
) WITH (fillfactor = 100);

-- ----------------------------------------------------------------------------
-- Fato 2: estabelecimento (grão = CNPJ 14 díg.) — PARTICIONADA por UF
-- ----------------------------------------------------------------------------
CREATE TABLE analytics.estabelecimento (
    cnpj                     char(14) NOT NULL,
    cnpj_basico              char(8)  NOT NULL,
    matriz_filial            smallint,
    nome_fantasia            text,
    situacao_cadastral       smallint,
    data_situacao_cadastral  date,
    motivo_situacao_cod      smallint,
    nome_cidade_exterior     text,
    pais_cod                 smallint,
    data_inicio_atividade    date,
    cnae_fiscal_principal    integer,
    tipo_logradouro          text,
    logradouro               text,
    numero                   text,
    complemento              text,
    bairro                   text,
    cep                      char(8),
    uf                       char(2) NOT NULL,
    municipio_cod            integer,
    ddd_telefone_1           text,
    ddd_telefone_2           text,
    ddd_fax                  text,
    email                    text,
    situacao_especial        text,
    data_situacao_especial   date,
    PRIMARY KEY (cnpj, uf)
) PARTITION BY LIST (uf);

-- Uma partição por UF + EX (exterior) + DEFAULT (UF vazia/inesperada)
CREATE TABLE analytics.estabelecimento_ac PARTITION OF analytics.estabelecimento FOR VALUES IN ('AC');
CREATE TABLE analytics.estabelecimento_al PARTITION OF analytics.estabelecimento FOR VALUES IN ('AL');
CREATE TABLE analytics.estabelecimento_ap PARTITION OF analytics.estabelecimento FOR VALUES IN ('AP');
CREATE TABLE analytics.estabelecimento_am PARTITION OF analytics.estabelecimento FOR VALUES IN ('AM');
CREATE TABLE analytics.estabelecimento_ba PARTITION OF analytics.estabelecimento FOR VALUES IN ('BA');
CREATE TABLE analytics.estabelecimento_ce PARTITION OF analytics.estabelecimento FOR VALUES IN ('CE');
CREATE TABLE analytics.estabelecimento_df PARTITION OF analytics.estabelecimento FOR VALUES IN ('DF');
CREATE TABLE analytics.estabelecimento_es PARTITION OF analytics.estabelecimento FOR VALUES IN ('ES');
CREATE TABLE analytics.estabelecimento_go PARTITION OF analytics.estabelecimento FOR VALUES IN ('GO');
CREATE TABLE analytics.estabelecimento_ma PARTITION OF analytics.estabelecimento FOR VALUES IN ('MA');
CREATE TABLE analytics.estabelecimento_mt PARTITION OF analytics.estabelecimento FOR VALUES IN ('MT');
CREATE TABLE analytics.estabelecimento_ms PARTITION OF analytics.estabelecimento FOR VALUES IN ('MS');
CREATE TABLE analytics.estabelecimento_mg PARTITION OF analytics.estabelecimento FOR VALUES IN ('MG');
CREATE TABLE analytics.estabelecimento_pa PARTITION OF analytics.estabelecimento FOR VALUES IN ('PA');
CREATE TABLE analytics.estabelecimento_pb PARTITION OF analytics.estabelecimento FOR VALUES IN ('PB');
CREATE TABLE analytics.estabelecimento_pr PARTITION OF analytics.estabelecimento FOR VALUES IN ('PR');
CREATE TABLE analytics.estabelecimento_pe PARTITION OF analytics.estabelecimento FOR VALUES IN ('PE');
CREATE TABLE analytics.estabelecimento_pi PARTITION OF analytics.estabelecimento FOR VALUES IN ('PI');
CREATE TABLE analytics.estabelecimento_rj PARTITION OF analytics.estabelecimento FOR VALUES IN ('RJ');
CREATE TABLE analytics.estabelecimento_rn PARTITION OF analytics.estabelecimento FOR VALUES IN ('RN');
CREATE TABLE analytics.estabelecimento_rs PARTITION OF analytics.estabelecimento FOR VALUES IN ('RS');
CREATE TABLE analytics.estabelecimento_ro PARTITION OF analytics.estabelecimento FOR VALUES IN ('RO');
CREATE TABLE analytics.estabelecimento_rr PARTITION OF analytics.estabelecimento FOR VALUES IN ('RR');
CREATE TABLE analytics.estabelecimento_sc PARTITION OF analytics.estabelecimento FOR VALUES IN ('SC');
CREATE TABLE analytics.estabelecimento_sp PARTITION OF analytics.estabelecimento FOR VALUES IN ('SP');
CREATE TABLE analytics.estabelecimento_se PARTITION OF analytics.estabelecimento FOR VALUES IN ('SE');
CREATE TABLE analytics.estabelecimento_to PARTITION OF analytics.estabelecimento FOR VALUES IN ('TO');
CREATE TABLE analytics.estabelecimento_ex PARTITION OF analytics.estabelecimento FOR VALUES IN ('EX');
CREATE TABLE analytics.estabelecimento_default PARTITION OF analytics.estabelecimento DEFAULT;

-- ----------------------------------------------------------------------------
-- Fato 3: CNAEs secundários (M:N estabelecimento × CNAE)
-- ----------------------------------------------------------------------------
CREATE TABLE analytics.estabelecimento_cnae_secundario (
    cnpj      char(14) NOT NULL,
    cnae_cod  integer  NOT NULL,
    PRIMARY KEY (cnpj, cnae_cod)
) WITH (fillfactor = 100);

-- ----------------------------------------------------------------------------
-- Fato 4: sócio (QSA)
-- ----------------------------------------------------------------------------
CREATE TABLE analytics.socio (
    id                       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    cnpj_basico              char(8) NOT NULL,
    identificador_socio      smallint,
    nome_socio               text,
    cnpj_cpf_socio           varchar(14),       -- CPF mascarado por privacidade
    qualificacao_socio_cod   smallint,
    data_entrada_sociedade   date,
    pais_cod                 smallint,
    cpf_representante        varchar(14),
    nome_representante       text,
    qualificacao_repr_cod    smallint,
    faixa_etaria_cod         smallint
) WITH (fillfactor = 100);

-- ----------------------------------------------------------------------------
-- Fato 5: Simples / MEI (1:1 com empresa)
-- ----------------------------------------------------------------------------
CREATE TABLE analytics.simples (
    cnpj_basico              char(8) PRIMARY KEY,
    opcao_simples            boolean,
    data_opcao_simples       date,
    data_exclusao_simples    date,
    opcao_mei                boolean,
    data_opcao_mei           date,
    data_exclusao_mei        date
) WITH (fillfactor = 100);

-- ----------------------------------------------------------------------------
-- Fato 6: regime tributário (entidades-*.zip — share Nextcloud SEPARADO da
-- Receita, token MPPfFit7g7zdA8C; ver analytics/load_regime.sh e docs)
-- Grão = (cnpj completo, ano, forma). A ordem do CNPJ varia (não é só matriz) e
-- há SCP, então NÃO dá p/ chavear por cnpj_basico — usa-se PK surrogate.
-- ----------------------------------------------------------------------------
CREATE TABLE analytics.regime_tributario (
    id                   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    cnpj                 char(14) NOT NULL,        -- CNPJ completo
    cnpj_basico          char(8)  NOT NULL,        -- join com empresa
    ano                  smallint NOT NULL,
    forma_de_tributacao  text     NOT NULL,        -- LUCRO REAL/PRESUMIDO/ARBITRADO, ISENTO/IMUNE...
    qtd_escrituracoes    integer,
    cnpj_da_scp          char(14)                  -- '0' na origem -> NULL
) WITH (fillfactor = 100);
