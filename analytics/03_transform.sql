-- ============================================================================
-- 03_transform.sql — converte staging (text) -> analytics (tipado)
-- Rodar APÓS o COPY bruto (load.sh). Dimensões primeiro, depois os fatos.
-- Idempotente: TRUNCATE nos destinos antes de inserir.
--
-- Esta é a versão SEQUENCIAL do transform da v2 (spec-carga.md): mesmo conteúdo
-- de sempre — o golden do T2 não muda — mas com a sanitização NOMEADA e o
-- descarte CONTADO. Antes, `nullif`, `parse_date`, filtros `~ '^\d{8}$'` e
-- `ON CONFLICT DO NOTHING` jogavam linhas fora em silêncio: 26.442 delas em
-- 2026-09, e ninguém sabe o que eram.
--
-- A versão em BLOCOS paralelos, que é a que o load.sh usa na carga grande, está
-- em 03_transform_v2.sql. As duas produzem o mesmo conteúdo, e é o T10 que
-- cobra isso — três vezes seguidas, porque determinismo que passa uma vez só
-- não provou nada.
--
-- Estrutura de cada tabela, sempre na mesma ordem:
--   1. TRUNCATE do destino
--   2. INSERT sanitizado  (a linha ruim não entra; a célula ruim vira NULL)
--   3. varredura de rejeito (UMA por tabela, cobrindo S4+S6+S11+S12 juntas)
--   4. contadores em carga.contador
--
-- O passo 3 custa uma varredura sequencial a mais da staging por tabela. É
-- deliberado: sem ela, "o que foi descartado" continua sendo pergunta sem
-- resposta, que é o problema que a R2 existe para resolver.
-- ============================================================================

\set ON_ERROR_STOP on

-- O schema `carga` (00_carga.sql) tem de existir ANTES daqui: é onde moram os
-- casts totais e a auditoria. Falhar com esta mensagem é muito melhor que
-- falhar com "function carga.num_smallint does not exist" na hora 16.
DO $$
BEGIN
    IF to_regclass('carga.rejeito') IS NULL THEN
        RAISE EXCEPTION
            'schema `carga` ausente — aplique analytics/00_carga.sql antes do transform';
    END IF;
END $$;

SELECT carga.resumo_atual();       -- abre (ou reusa) o resumo desta competência
SELECT carga.limpar_competencia(); -- recarregar o mesmo mês reconta do zero

-- ----------------------------------------------------------------------------
-- Dimensões
--
-- S5: o código tem de ser numérico E caber no tipo da coluna. O filtro de hoje
-- (`~ '^\d+$'`) deixava passar um '999999' que estouraria o smallint e mataria
-- a carga; `carga.num_smallint` recusa os dois casos do mesmo jeito.
-- ----------------------------------------------------------------------------
TRUNCATE analytics.dim_cnae, analytics.dim_natureza_juridica,
         analytics.dim_qualificacao, analytics.dim_pais,
         analytics.dim_motivo_situacao, analytics.dim_municipio;

WITH ins AS (
    INSERT INTO analytics.dim_cnae (codigo, descricao)
    SELECT carga.num_integer(codigo), descricao FROM staging.cnaes
    WHERE carga.num_integer(codigo) IS NOT NULL
    ON CONFLICT DO NOTHING RETURNING 1
), rej AS (
    INSERT INTO carga.rejeito (competencia, tabela, regra, coluna, linha_bruta)
    SELECT carga.competencia(), 'dim_cnae', 'S5', 'codigo', c::text
    FROM staging.cnaes c WHERE carga.num_integer(c.codigo) IS NULL
    RETURNING 1
)
SELECT carga.contar('dim_cnae', 'S5', (SELECT count(*) FROM rej)),
       carga.contar('dim_cnae', 'linhas_inseridas', (SELECT count(*) FROM ins));

WITH ins AS (
    INSERT INTO analytics.dim_natureza_juridica (codigo, descricao)
    SELECT carga.num_smallint(codigo), descricao FROM staging.naturezas
    WHERE carga.num_smallint(codigo) IS NOT NULL
    ON CONFLICT DO NOTHING RETURNING 1
), rej AS (
    INSERT INTO carga.rejeito (competencia, tabela, regra, coluna, linha_bruta)
    SELECT carga.competencia(), 'dim_natureza_juridica', 'S5', 'codigo', c::text
    FROM staging.naturezas c WHERE carga.num_smallint(c.codigo) IS NULL
    RETURNING 1
)
SELECT carga.contar('dim_natureza_juridica', 'S5', (SELECT count(*) FROM rej)),
       carga.contar('dim_natureza_juridica', 'linhas_inseridas', (SELECT count(*) FROM ins));

WITH ins AS (
    INSERT INTO analytics.dim_qualificacao (codigo, descricao)
    SELECT carga.num_smallint(codigo), descricao FROM staging.qualificacoes
    WHERE carga.num_smallint(codigo) IS NOT NULL
    ON CONFLICT DO NOTHING RETURNING 1
), rej AS (
    INSERT INTO carga.rejeito (competencia, tabela, regra, coluna, linha_bruta)
    SELECT carga.competencia(), 'dim_qualificacao', 'S5', 'codigo', c::text
    FROM staging.qualificacoes c WHERE carga.num_smallint(c.codigo) IS NULL
    RETURNING 1
)
SELECT carga.contar('dim_qualificacao', 'S5', (SELECT count(*) FROM rej)),
       carga.contar('dim_qualificacao', 'linhas_inseridas', (SELECT count(*) FROM ins));

WITH ins AS (
    INSERT INTO analytics.dim_pais (codigo, nome)
    SELECT carga.num_smallint(codigo), descricao FROM staging.paises
    WHERE carga.num_smallint(codigo) IS NOT NULL
    ON CONFLICT DO NOTHING RETURNING 1
), rej AS (
    INSERT INTO carga.rejeito (competencia, tabela, regra, coluna, linha_bruta)
    SELECT carga.competencia(), 'dim_pais', 'S5', 'codigo', c::text
    FROM staging.paises c WHERE carga.num_smallint(c.codigo) IS NULL
    RETURNING 1
)
SELECT carga.contar('dim_pais', 'S5', (SELECT count(*) FROM rej)),
       carga.contar('dim_pais', 'linhas_inseridas', (SELECT count(*) FROM ins));

WITH ins AS (
    INSERT INTO analytics.dim_motivo_situacao (codigo, descricao)
    SELECT carga.num_smallint(codigo), descricao FROM staging.motivos
    WHERE carga.num_smallint(codigo) IS NOT NULL
    ON CONFLICT DO NOTHING RETURNING 1
), rej AS (
    INSERT INTO carga.rejeito (competencia, tabela, regra, coluna, linha_bruta)
    SELECT carga.competencia(), 'dim_motivo_situacao', 'S5', 'codigo', c::text
    FROM staging.motivos c WHERE carga.num_smallint(c.codigo) IS NULL
    RETURNING 1
)
SELECT carga.contar('dim_motivo_situacao', 'S5', (SELECT count(*) FROM rej)),
       carga.contar('dim_motivo_situacao', 'linhas_inseridas', (SELECT count(*) FROM ins));

WITH ins AS (
    INSERT INTO analytics.dim_municipio (codigo, nome)
    SELECT carga.num_integer(codigo), descricao FROM staging.municipios
    WHERE carga.num_integer(codigo) IS NOT NULL
    ON CONFLICT DO NOTHING RETURNING 1
), rej AS (
    INSERT INTO carga.rejeito (competencia, tabela, regra, coluna, linha_bruta)
    SELECT carga.competencia(), 'dim_municipio', 'S5', 'codigo', c::text
    FROM staging.municipios c WHERE carga.num_integer(c.codigo) IS NULL
    RETURNING 1
)
SELECT carga.contar('dim_municipio', 'S5', (SELECT count(*) FROM rej)),
       carga.contar('dim_municipio', 'linhas_inseridas', (SELECT count(*) FROM ins));

-- ----------------------------------------------------------------------------
-- empresa
--
-- É a única tabela cuja PK fica VIVA durante a Fase 3 (a v2 dropa o resto dos
-- índices antes do transform). O `ON CONFLICT DO NOTHING` precisa dela para
-- arbitrar entre duplicatas de `cnpj_basico` — e é por isso que, no
-- 03_transform_v2.sql, `empresa` é a única que NÃO é fatiada em blocos: com
-- blocos concorrentes, "a primeira linha do arquivo vence" viraria sorteio.
-- ----------------------------------------------------------------------------
TRUNCATE analytics.empresa;
WITH ins AS (
    INSERT INTO analytics.empresa
        (cnpj_basico, razao_social, natureza_juridica_cod, qualificacao_resp_cod,
         capital_social, porte_cod, ente_federativo)
    SELECT
        cnpj_basico,
        nullif(razao_social, ''),
        carga.num_smallint(natureza_juridica),
        carga.num_smallint(qualificacao_resp),
        carga.num_capital(capital_social),
        carga.num_smallint(porte),
        nullif(ente_federativo, '')
    FROM staging.empresas
    WHERE cnpj_basico ~ '^\d{8}$'
    ON CONFLICT (cnpj_basico) DO NOTHING
    RETURNING 1
)
SELECT carga.contar('empresa', 'linhas_inseridas', (SELECT count(*) FROM ins));

INSERT INTO carga.rejeito (competencia, tabela, regra, coluna, linha_bruta)
SELECT carga.competencia(), 'empresa', r.regra, r.coluna, e::text
FROM staging.empresas e
CROSS JOIN LATERAL (SELECT e.cnpj_basico ~ '^\d{8}$' AS chave_ok) v
CROSS JOIN LATERAL (VALUES
    ('S4',  NULL::text,            NOT v.chave_ok),
    ('S11', 'natureza_juridica',   v.chave_ok AND carga.perdeu(e.natureza_juridica, carga.num_smallint(e.natureza_juridica) IS NULL)),
    ('S11', 'qualificacao_resp',   v.chave_ok AND carga.perdeu(e.qualificacao_resp, carga.num_smallint(e.qualificacao_resp) IS NULL)),
    ('S11', 'capital_social',      v.chave_ok AND carga.perdeu(e.capital_social,    carga.num_capital(e.capital_social)     IS NULL)),
    ('S11', 'porte',               v.chave_ok AND carga.perdeu(e.porte,             carga.num_smallint(e.porte)             IS NULL))
) AS r(regra, coluna, ruim)
WHERE r.ruim;

SELECT carga.contar('empresa', 'linhas_lidas',   (SELECT count(*) FROM staging.empresas)),
       carga.contar('empresa', 'S7_capital_com_virgula',
                    (SELECT count(*) FROM staging.empresas WHERE capital_social LIKE '%,%'));

-- ----------------------------------------------------------------------------
-- estabelecimento
--
-- S12 na chave: `cnpj` é char(14) e NOT NULL, então um `cnpj_ordem` com 5
-- dígitos não tem como virar NULL — a linha inteira sai, e vira rejeito. Nas
-- colunas nulificáveis (cep) o valor grande demais vira NULL, como manda a S12.
--
-- `ON CONFLICT DO NOTHING` SEM especificar a chave, e isto não é descuido: a
-- Fase 2 dropa a PK desta tabela antes do transform, e `ON CONFLICT (cnpj, uf)`
-- morreria com "there is no unique or exclusion constraint matching the ON
-- CONFLICT specification". Sem inferência, o comando funciona nos dois mundos —
-- dedupe quando há índice único, insere tudo quando não há (e aí é a Fase 4 que
-- decide o que fazer com as repetidas). O mesmo vale para `simples`, adiante.
-- Só `empresa` infere a chave, porque a PK dela é a única que a Fase 2 preserva.
-- ----------------------------------------------------------------------------
TRUNCATE analytics.estabelecimento;
WITH ins AS (
    INSERT INTO analytics.estabelecimento (
        cnpj, cnpj_basico, matriz_filial, nome_fantasia, situacao_cadastral,
        data_situacao_cadastral, motivo_situacao_cod, nome_cidade_exterior, pais_cod,
        data_inicio_atividade, cnae_fiscal_principal, tipo_logradouro, logradouro,
        numero, complemento, bairro, cep, uf, municipio_cod, ddd_telefone_1,
        ddd_telefone_2, ddd_fax, email, situacao_especial, data_situacao_especial)
    SELECT
        cnpj_basico || cnpj_ordem || cnpj_dv,
        cnpj_basico,
        carga.num_smallint(identificador_matriz_filial),
        nullif(nome_fantasia, ''),
        carga.num_smallint(situacao_cadastral),
        analytics.parse_date(data_situacao_cadastral),
        carga.num_smallint(motivo_situacao),
        nullif(nome_cidade_exterior, ''),
        carga.num_smallint(pais),
        analytics.parse_date(data_inicio_atividade),
        carga.num_integer(cnae_principal),
        nullif(tipo_logradouro, ''),
        nullif(logradouro, ''),
        nullif(numero, ''),
        nullif(complemento, ''),
        nullif(bairro, ''),
        carga.cabe(nullif(cep, ''), 8),
        coalesce(carga.cabe(nullif(btrim(uf), ''), 2), '??'),   -- DEFAULT partition pega '??'
        carga.num_integer(municipio),
        nullif(btrim(ddd_1 || telefone_1), ''),
        nullif(btrim(ddd_2 || telefone_2), ''),
        nullif(btrim(ddd_fax || fax), ''),
        nullif(correio_eletronico, ''),
        nullif(situacao_especial, ''),
        analytics.parse_date(data_situacao_especial)
    FROM staging.estabelecimentos
    WHERE cnpj_basico ~ '^\d{8}$'
      AND length(cnpj_basico || cnpj_ordem || cnpj_dv) <= 14
    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT carga.contar('estabelecimento', 'linhas_inseridas', (SELECT count(*) FROM ins));

INSERT INTO carga.rejeito (competencia, tabela, regra, coluna, linha_bruta)
SELECT carga.competencia(), 'estabelecimento', r.regra, r.coluna, e::text
FROM staging.estabelecimentos e
CROSS JOIN LATERAL (
    SELECT e.cnpj_basico ~ '^\d{8}$'
           AND length(e.cnpj_basico || e.cnpj_ordem || e.cnpj_dv) <= 14 AS entrou,
           e.cnpj_basico ~ '^\d{8}$' AS chave_ok
) v
CROSS JOIN LATERAL (VALUES
    ('S4',  NULL::text,   NOT v.chave_ok),
    ('S12', 'cnpj',       v.chave_ok AND NOT v.entrou),
    ('S11', 'identificador_matriz_filial', v.entrou AND carga.perdeu(e.identificador_matriz_filial, carga.num_smallint(e.identificador_matriz_filial) IS NULL)),
    ('S11', 'situacao_cadastral',          v.entrou AND carga.perdeu(e.situacao_cadastral, carga.num_smallint(e.situacao_cadastral) IS NULL)),
    ('S11', 'motivo_situacao',             v.entrou AND carga.perdeu(e.motivo_situacao,    carga.num_smallint(e.motivo_situacao)    IS NULL)),
    ('S11', 'pais',                        v.entrou AND carga.perdeu(e.pais,               carga.num_smallint(e.pais)               IS NULL)),
    ('S11', 'cnae_principal',              v.entrou AND carga.perdeu(e.cnae_principal,     carga.num_integer(e.cnae_principal)      IS NULL)),
    ('S11', 'municipio',                   v.entrou AND carga.perdeu(e.municipio,          carga.num_integer(e.municipio)           IS NULL)),
    ('S12', 'cep',                         v.entrou AND carga.perdeu(nullif(e.cep, ''),    carga.cabe(nullif(e.cep, ''), 8)         IS NULL)),
    ('S12', 'uf',                          v.entrou AND carga.perdeu(nullif(btrim(e.uf), ''), carga.cabe(nullif(btrim(e.uf), ''), 2) IS NULL)),
    ('S6',  'data_situacao_cadastral',     v.entrou AND carga.perdeu_data(e.data_situacao_cadastral, analytics.parse_date(e.data_situacao_cadastral) IS NULL)),
    ('S6',  'data_inicio_atividade',       v.entrou AND carga.perdeu_data(e.data_inicio_atividade, analytics.parse_date(e.data_inicio_atividade) IS NULL)),
    ('S6',  'data_situacao_especial',      v.entrou AND carga.perdeu_data(e.data_situacao_especial, analytics.parse_date(e.data_situacao_especial) IS NULL))
) AS r(regra, coluna, ruim)
WHERE r.ruim;

SELECT carga.contar('estabelecimento', 'linhas_lidas',
                    (SELECT count(*) FROM staging.estabelecimentos)),
       carga.contar('estabelecimento', 'S8_uf_default',
                    (SELECT count(*) FROM analytics.estabelecimento_default));

-- ----------------------------------------------------------------------------
-- estabelecimento_cnae_secundario (explode a lista separada por vírgula)
--
-- O DISTINCT substitui o `ON CONFLICT DO NOTHING` de antes. Não é cosmético: na
-- Fase 3 da v2 a PK desta tabela está dropada, então não há o que arbitrar um
-- conflito — o mesmo CNAE repetido 6× dentro de uma linha (caso real da 2.6)
-- tem de ser resolvido ANTES de chegar à tabela.
-- ----------------------------------------------------------------------------
TRUNCATE analytics.estabelecimento_cnae_secundario;
WITH bruto AS (
    SELECT cnpj_basico || cnpj_ordem || cnpj_dv AS cnpj,
           btrim(unnest(string_to_array(nullif(cnae_secundaria, ''), ','))) AS cnae
    FROM staging.estabelecimentos
    WHERE cnpj_basico ~ '^\d{8}$'
      AND length(cnpj_basico || cnpj_ordem || cnpj_dv) <= 14
      AND nullif(cnae_secundaria, '') IS NOT NULL
), ins AS (
    INSERT INTO analytics.estabelecimento_cnae_secundario (cnpj, cnae_cod)
    SELECT DISTINCT cnpj, carga.num_integer(cnae)
    FROM bruto WHERE carga.num_integer(cnae) IS NOT NULL
    RETURNING 1
), rej AS (
    INSERT INTO carga.rejeito (competencia, tabela, regra, coluna, linha_bruta)
    SELECT carga.competencia(), 'estabelecimento_cnae_secundario', 'S9', 'cnae',
           cnpj || ';' || cnae
    FROM bruto WHERE carga.num_integer(cnae) IS NULL
    RETURNING 1
)
SELECT carga.contar('estabelecimento_cnae_secundario', 'S9', (SELECT count(*) FROM rej)),
       carga.contar('estabelecimento_cnae_secundario', 'linhas_inseridas',
                    (SELECT count(*) FROM ins));

-- ----------------------------------------------------------------------------
-- socio
--
-- Sem chave natural e sem dedupe: `socio` ESPELHA a fonte (decisão 1 da spec,
-- seção 5.2). As 22 linhas idênticas que a Receita publica continuam entrando
-- duas vezes — apagar dado da fonte é irreversível, e a contagem delas fica em
-- carga.duplicata (escrita pela Fase 4), que é reversível e testável.
-- ----------------------------------------------------------------------------
TRUNCATE analytics.socio RESTART IDENTITY;
WITH ins AS (
    INSERT INTO analytics.socio (
        cnpj_basico, identificador_socio, nome_socio, cnpj_cpf_socio,
        qualificacao_socio_cod, data_entrada_sociedade, pais_cod, cpf_representante,
        nome_representante, qualificacao_repr_cod, faixa_etaria_cod)
    SELECT
        cnpj_basico,
        carga.num_smallint(identificador_socio),
        nullif(nome_socio, ''),
        carga.cabe(nullif(cnpj_cpf_socio, ''), 14),
        carga.num_smallint(qualificacao_socio),
        analytics.parse_date(data_entrada_sociedade),
        carga.num_smallint(pais),
        carga.cabe(nullif(cpf_representante, ''), 14),
        nullif(nome_representante, ''),
        carga.num_smallint(qualificacao_repr),
        carga.num_smallint(faixa_etaria)
    FROM staging.socios
    WHERE cnpj_basico ~ '^\d{8}$'
    RETURNING 1
)
SELECT carga.contar('socio', 'linhas_inseridas', (SELECT count(*) FROM ins));

INSERT INTO carga.rejeito (competencia, tabela, regra, coluna, linha_bruta)
SELECT carga.competencia(), 'socio', r.regra, r.coluna, s::text
FROM staging.socios s
CROSS JOIN LATERAL (SELECT s.cnpj_basico ~ '^\d{8}$' AS chave_ok) v
CROSS JOIN LATERAL (VALUES
    ('S4',  NULL::text,             NOT v.chave_ok),
    ('S11', 'identificador_socio',  v.chave_ok AND carga.perdeu(s.identificador_socio, carga.num_smallint(s.identificador_socio) IS NULL)),
    ('S11', 'qualificacao_socio',   v.chave_ok AND carga.perdeu(s.qualificacao_socio,  carga.num_smallint(s.qualificacao_socio)  IS NULL)),
    ('S11', 'pais',                 v.chave_ok AND carga.perdeu(s.pais,                carga.num_smallint(s.pais)                IS NULL)),
    ('S11', 'qualificacao_repr',    v.chave_ok AND carga.perdeu(s.qualificacao_repr,   carga.num_smallint(s.qualificacao_repr)   IS NULL)),
    ('S11', 'faixa_etaria',         v.chave_ok AND carga.perdeu(s.faixa_etaria,        carga.num_smallint(s.faixa_etaria)        IS NULL)),
    ('S12', 'cnpj_cpf_socio',       v.chave_ok AND carga.perdeu(nullif(s.cnpj_cpf_socio, ''),   carga.cabe(nullif(s.cnpj_cpf_socio, ''), 14)   IS NULL)),
    ('S12', 'cpf_representante',    v.chave_ok AND carga.perdeu(nullif(s.cpf_representante, ''), carga.cabe(nullif(s.cpf_representante, ''), 14) IS NULL)),
    ('S6',  'data_entrada_sociedade', v.chave_ok AND carga.perdeu_data(s.data_entrada_sociedade, analytics.parse_date(s.data_entrada_sociedade) IS NULL))
) AS r(regra, coluna, ruim)
WHERE r.ruim;

SELECT carga.contar('socio', 'linhas_lidas', (SELECT count(*) FROM staging.socios));

-- ----------------------------------------------------------------------------
-- simples
-- ----------------------------------------------------------------------------
TRUNCATE analytics.simples;
WITH ins AS (
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
    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT carga.contar('simples', 'linhas_inseridas', (SELECT count(*) FROM ins));

INSERT INTO carga.rejeito (competencia, tabela, regra, coluna, linha_bruta)
SELECT carga.competencia(), 'simples', r.regra, r.coluna, s::text
FROM staging.simples s
CROSS JOIN LATERAL (SELECT s.cnpj_basico ~ '^\d{8}$' AS chave_ok) v
CROSS JOIN LATERAL (VALUES
    ('S4', NULL::text,               NOT v.chave_ok),
    ('S6', 'data_opcao_simples',     v.chave_ok AND carga.perdeu_data(s.data_opcao_simples, analytics.parse_date(s.data_opcao_simples) IS NULL)),
    ('S6', 'data_exclusao_simples',  v.chave_ok AND carga.perdeu_data(s.data_exclusao_simples, analytics.parse_date(s.data_exclusao_simples) IS NULL)),
    ('S6', 'data_opcao_mei',         v.chave_ok AND carga.perdeu_data(s.data_opcao_mei, analytics.parse_date(s.data_opcao_mei) IS NULL)),
    ('S6', 'data_exclusao_mei',      v.chave_ok AND carga.perdeu_data(s.data_exclusao_mei, analytics.parse_date(s.data_exclusao_mei) IS NULL))
) AS r(regra, coluna, ruim)
WHERE r.ruim;

SELECT carga.contar('simples', 'linhas_lidas', (SELECT count(*) FROM staging.simples));

-- ----------------------------------------------------------------------------
-- S10 — o que o ON CONFLICT engoliu, agora contado
--
-- Duplicata de chave natural não é erro da carga: é o que a Receita publicou.
-- Antes, `DO NOTHING` a descartava sem deixar registro (3.939 CNAEs em 2026-09).
-- Aqui ela vira número: lidas − válidas − inseridas.
-- ----------------------------------------------------------------------------
INSERT INTO carga.contador (competencia, tabela, regra, quantidade)
SELECT carga.competencia(), t.tabela, 'S10_duplicata_descartada',
       greatest(t.validas - t.inseridas, 0)
FROM (
    SELECT 'empresa' AS tabela,
           (SELECT count(*) FROM staging.empresas WHERE cnpj_basico ~ '^\d{8}$') AS validas,
           (SELECT count(*) FROM analytics.empresa) AS inseridas
    UNION ALL
    SELECT 'simples',
           (SELECT count(*) FROM staging.simples WHERE cnpj_basico ~ '^\d{8}$'),
           (SELECT count(*) FROM analytics.simples)
) t;

-- regime_tributario: fonte/cadência próprias -> transform isolado em
-- analytics/regime_transform.sql (chamado pelo load.sh após os índices).
