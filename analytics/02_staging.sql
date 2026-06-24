-- ============================================================================
-- 02_staging.sql — tabelas de staging (tudo TEXT, sem constraints)
-- Espelham 1:1 os layouts dos CSVs da Receita. Populadas por COPY bruto
-- (ver load.sh) e convertidas em 03_transform.sql.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS staging;

DROP TABLE IF EXISTS staging.empresas;
CREATE TABLE staging.empresas (           -- Empresas*.csv (7 colunas)
    cnpj_basico              text,
    razao_social             text,
    natureza_juridica        text,
    qualificacao_resp        text,
    capital_social           text,        -- vírgula decimal: "5000,00"
    porte                    text,
    ente_federativo          text
);

DROP TABLE IF EXISTS staging.estabelecimentos;
CREATE TABLE staging.estabelecimentos (   -- Estabelecimentos*.csv (30 colunas)
    cnpj_basico              text,
    cnpj_ordem               text,
    cnpj_dv                  text,
    identificador_matriz_filial text,
    nome_fantasia            text,
    situacao_cadastral       text,
    data_situacao_cadastral  text,
    motivo_situacao          text,
    nome_cidade_exterior     text,
    pais                     text,
    data_inicio_atividade    text,
    cnae_principal           text,
    cnae_secundaria          text,        -- lista separada por vírgula
    tipo_logradouro          text,
    logradouro               text,
    numero                   text,
    complemento              text,
    bairro                   text,
    cep                      text,
    uf                       text,
    municipio                text,
    ddd_1                    text,
    telefone_1               text,
    ddd_2                    text,
    telefone_2               text,
    ddd_fax                  text,
    fax                      text,
    correio_eletronico       text,
    situacao_especial        text,
    data_situacao_especial   text
);

DROP TABLE IF EXISTS staging.socios;
CREATE TABLE staging.socios (             -- Socios*.csv (11 colunas)
    cnpj_basico              text,
    identificador_socio      text,
    nome_socio               text,
    cnpj_cpf_socio           text,
    qualificacao_socio       text,
    data_entrada_sociedade   text,
    pais                     text,
    cpf_representante        text,
    nome_representante       text,
    qualificacao_repr        text,
    faixa_etaria             text
);

DROP TABLE IF EXISTS staging.simples;
CREATE TABLE staging.simples (            -- Simples.csv (7 colunas)
    cnpj_basico              text,
    opcao_simples            text,        -- S / N
    data_opcao_simples       text,
    data_exclusao_simples    text,
    opcao_mei                text,        -- S / N
    data_opcao_mei           text,
    data_exclusao_mei        text
);

DROP TABLE IF EXISTS staging.regime_tributario;
CREATE TABLE staging.regime_tributario (  -- entidades-*.csv (5 colunas, VÍRGULA, c/ header)
    ano                          text,
    cnpj                         text,     -- completo e formatado: 00.000.000/0001-91
    cnpj_da_scp                  text,     -- '0' = sem SCP
    forma_de_tributacao          text,
    quantidade_de_escrituracoes  text
);

-- Lookups (codigo;descricao)
DROP TABLE IF EXISTS staging.cnaes;        CREATE TABLE staging.cnaes        (codigo text, descricao text);
DROP TABLE IF EXISTS staging.naturezas;    CREATE TABLE staging.naturezas    (codigo text, descricao text);
DROP TABLE IF EXISTS staging.qualificacoes;CREATE TABLE staging.qualificacoes(codigo text, descricao text);
DROP TABLE IF EXISTS staging.paises;       CREATE TABLE staging.paises       (codigo text, descricao text);
DROP TABLE IF EXISTS staging.motivos;      CREATE TABLE staging.motivos      (codigo text, descricao text);
DROP TABLE IF EXISTS staging.municipios;   CREATE TABLE staging.municipios   (codigo text, descricao text);
