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
--
-- A função é TOTAL (spec-carga.md, decisão 6 / R2.1): para qualquer entrada
-- existe saída, e a saída ruim é NULL. Antes ela delegava ao `to_date`, que no
-- PostgreSQL 18 ESTOURA em data inexistente — `20200231` não rola para 02/03,
-- dá `ERROR: date/time field value out of range`. Uma única dessas em 73
-- milhões de linhas matava uma carga de 20 horas na fase de transform, de
-- madrugada, sem ninguém para reagir.
--
-- Por que a validação é este CASE feio em vez de um bloco EXCEPTION: EXCEPTION
-- abre uma subtransação por linha, e 73 milhões de subtransações custam mais
-- que o problema. E por que as expressões se repetem em vez de saírem num CTE:
-- o inliner do Postgres recusa corpos com `WITH`, e uma função não inlinada
-- vira uma chamada por linha, por coluna de data.
--
-- Escopo, e é um desvio registrado na spec (S6): só `^\d{8}$` é aceito. Entrada
-- fora desse formato e das sentinelas — `'2020-01-01'`, `'202001'` — vira NULL
-- mais um rejeito contado, em vez de passar pela leniência do `to_date`.
CREATE OR REPLACE FUNCTION analytics.parse_date(s text) RETURNS date
    LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT CASE
        WHEN s IS NULL OR btrim(s) IN ('', '0', '00000000') THEN NULL
        WHEN s !~ '^[0-9]{8}$'                              THEN NULL
        WHEN substr(s, 1, 4) = '0000'                       THEN NULL   -- ano 0 não existe
        WHEN substr(s, 5, 2)::integer NOT BETWEEN 1 AND 12  THEN NULL
        WHEN substr(s, 7, 2)::integer < 1                   THEN NULL
        WHEN substr(s, 7, 2)::integer > CASE substr(s, 5, 2)::integer
                 WHEN 2 THEN CASE WHEN (substr(s, 1, 4)::integer % 4 = 0
                                        AND substr(s, 1, 4)::integer % 100 <> 0)
                                       OR substr(s, 1, 4)::integer % 400 = 0
                                  THEN 29 ELSE 28 END
                 WHEN 4 THEN 30 WHEN 6 THEN 30 WHEN 9 THEN 30 WHEN 11 THEN 30
                 ELSE 31 END                                THEN NULL
        ELSE to_date(s, 'YYYYMMDD')
    END;
$$;

-- ----------------------------------------------------------------------------
-- Dimensões carregadas dos CSVs de lookup (codigo;descricao)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.dim_natureza_juridica (
    codigo      smallint PRIMARY KEY,
    descricao   text NOT NULL
);

CREATE TABLE IF NOT EXISTS analytics.dim_cnae (
    codigo      integer PRIMARY KEY,           -- 7 dígitos, ex. 6201501
    descricao   text NOT NULL
);

CREATE TABLE IF NOT EXISTS analytics.dim_municipio (
    codigo       integer PRIMARY KEY,          -- código Receita (4 díg.)
    nome         text NOT NULL,
    codigo_ibge  integer,                      -- 7 díg.; preenchido por ibge_transform.sql
    uf           char(2)                       -- idem (não vem no Municipios.csv)
);

CREATE TABLE IF NOT EXISTS analytics.dim_pais (
    codigo      smallint PRIMARY KEY,
    nome        text NOT NULL
);

CREATE TABLE IF NOT EXISTS analytics.dim_qualificacao (      -- sócio / responsável / representante
    codigo      smallint PRIMARY KEY,
    descricao   text NOT NULL
);

CREATE TABLE IF NOT EXISTS analytics.dim_motivo_situacao (
    codigo      smallint PRIMARY KEY,
    descricao   text NOT NULL
);

-- ----------------------------------------------------------------------------
-- Dimensões de domínio (valores fixos, semeados aqui para joins legíveis)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.dim_situacao_cadastral (codigo smallint PRIMARY KEY, descricao text NOT NULL);
INSERT INTO analytics.dim_situacao_cadastral VALUES
    (1,'Nula'),(2,'Ativa'),(3,'Suspensa'),(4,'Inapta'),(8,'Baixada')
    ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS analytics.dim_matriz_filial (codigo smallint PRIMARY KEY, descricao text NOT NULL);
INSERT INTO analytics.dim_matriz_filial VALUES (1,'Matriz'),(2,'Filial') ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS analytics.dim_porte (codigo smallint PRIMARY KEY, descricao text NOT NULL);
INSERT INTO analytics.dim_porte VALUES
    (0,'Não informado'),(1,'Micro empresa'),(3,'Empresa de pequeno porte'),(5,'Demais')
    ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS analytics.dim_identificador_socio (codigo smallint PRIMARY KEY, descricao text NOT NULL);
INSERT INTO analytics.dim_identificador_socio VALUES
    (1,'Pessoa jurídica'),(2,'Pessoa física'),(3,'Estrangeiro')
    ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS analytics.dim_faixa_etaria (codigo smallint PRIMARY KEY, descricao text NOT NULL);
INSERT INTO analytics.dim_faixa_etaria VALUES
    (0,'Não se aplica'),(1,'0 a 12 anos'),(2,'13 a 20 anos'),(3,'21 a 30 anos'),
    (4,'31 a 40 anos'),(5,'41 a 50 anos'),(6,'51 a 60 anos'),(7,'61 a 70 anos'),
    (8,'71 a 80 anos'),(9,'Maiores de 80 anos')
    ON CONFLICT DO NOTHING;

-- ----------------------------------------------------------------------------
-- Fato 1: empresa (grão = CNPJ básico, 8 díg.)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.empresa (
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
CREATE TABLE IF NOT EXISTS analytics.estabelecimento (
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
CREATE TABLE IF NOT EXISTS analytics.estabelecimento_ac PARTITION OF analytics.estabelecimento FOR VALUES IN ('AC');
CREATE TABLE IF NOT EXISTS analytics.estabelecimento_al PARTITION OF analytics.estabelecimento FOR VALUES IN ('AL');
CREATE TABLE IF NOT EXISTS analytics.estabelecimento_ap PARTITION OF analytics.estabelecimento FOR VALUES IN ('AP');
CREATE TABLE IF NOT EXISTS analytics.estabelecimento_am PARTITION OF analytics.estabelecimento FOR VALUES IN ('AM');
CREATE TABLE IF NOT EXISTS analytics.estabelecimento_ba PARTITION OF analytics.estabelecimento FOR VALUES IN ('BA');
CREATE TABLE IF NOT EXISTS analytics.estabelecimento_ce PARTITION OF analytics.estabelecimento FOR VALUES IN ('CE');
CREATE TABLE IF NOT EXISTS analytics.estabelecimento_df PARTITION OF analytics.estabelecimento FOR VALUES IN ('DF');
CREATE TABLE IF NOT EXISTS analytics.estabelecimento_es PARTITION OF analytics.estabelecimento FOR VALUES IN ('ES');
CREATE TABLE IF NOT EXISTS analytics.estabelecimento_go PARTITION OF analytics.estabelecimento FOR VALUES IN ('GO');
CREATE TABLE IF NOT EXISTS analytics.estabelecimento_ma PARTITION OF analytics.estabelecimento FOR VALUES IN ('MA');
CREATE TABLE IF NOT EXISTS analytics.estabelecimento_mt PARTITION OF analytics.estabelecimento FOR VALUES IN ('MT');
CREATE TABLE IF NOT EXISTS analytics.estabelecimento_ms PARTITION OF analytics.estabelecimento FOR VALUES IN ('MS');
CREATE TABLE IF NOT EXISTS analytics.estabelecimento_mg PARTITION OF analytics.estabelecimento FOR VALUES IN ('MG');
CREATE TABLE IF NOT EXISTS analytics.estabelecimento_pa PARTITION OF analytics.estabelecimento FOR VALUES IN ('PA');
CREATE TABLE IF NOT EXISTS analytics.estabelecimento_pb PARTITION OF analytics.estabelecimento FOR VALUES IN ('PB');
CREATE TABLE IF NOT EXISTS analytics.estabelecimento_pr PARTITION OF analytics.estabelecimento FOR VALUES IN ('PR');
CREATE TABLE IF NOT EXISTS analytics.estabelecimento_pe PARTITION OF analytics.estabelecimento FOR VALUES IN ('PE');
CREATE TABLE IF NOT EXISTS analytics.estabelecimento_pi PARTITION OF analytics.estabelecimento FOR VALUES IN ('PI');
CREATE TABLE IF NOT EXISTS analytics.estabelecimento_rj PARTITION OF analytics.estabelecimento FOR VALUES IN ('RJ');
CREATE TABLE IF NOT EXISTS analytics.estabelecimento_rn PARTITION OF analytics.estabelecimento FOR VALUES IN ('RN');
CREATE TABLE IF NOT EXISTS analytics.estabelecimento_rs PARTITION OF analytics.estabelecimento FOR VALUES IN ('RS');
CREATE TABLE IF NOT EXISTS analytics.estabelecimento_ro PARTITION OF analytics.estabelecimento FOR VALUES IN ('RO');
CREATE TABLE IF NOT EXISTS analytics.estabelecimento_rr PARTITION OF analytics.estabelecimento FOR VALUES IN ('RR');
CREATE TABLE IF NOT EXISTS analytics.estabelecimento_sc PARTITION OF analytics.estabelecimento FOR VALUES IN ('SC');
CREATE TABLE IF NOT EXISTS analytics.estabelecimento_sp PARTITION OF analytics.estabelecimento FOR VALUES IN ('SP');
CREATE TABLE IF NOT EXISTS analytics.estabelecimento_se PARTITION OF analytics.estabelecimento FOR VALUES IN ('SE');
CREATE TABLE IF NOT EXISTS analytics.estabelecimento_to PARTITION OF analytics.estabelecimento FOR VALUES IN ('TO');
CREATE TABLE IF NOT EXISTS analytics.estabelecimento_ex PARTITION OF analytics.estabelecimento FOR VALUES IN ('EX');
CREATE TABLE IF NOT EXISTS analytics.estabelecimento_default PARTITION OF analytics.estabelecimento DEFAULT;

-- ----------------------------------------------------------------------------
-- Fato 3: CNAEs secundários (M:N estabelecimento × CNAE)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.estabelecimento_cnae_secundario (
    cnpj      char(14) NOT NULL,
    cnae_cod  integer  NOT NULL,
    PRIMARY KEY (cnpj, cnae_cod)
) WITH (fillfactor = 100);

-- ----------------------------------------------------------------------------
-- Fato 4: sócio (QSA)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.socio (
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
CREATE TABLE IF NOT EXISTS analytics.simples (
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
-- Receita, token MPPfFit7g7zdA8C). Por ter fonte e cadência próprias, toda a sua
-- DDL/transform/índices vivem juntos em analytics/regime_transform.sql (reusado
-- pela carga completa e pela incremental `REGIME_ONLY=1 bash analytics/load.sh`).
-- ----------------------------------------------------------------------------
