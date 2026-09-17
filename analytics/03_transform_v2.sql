-- ============================================================================
-- 03_transform_v2.sql — o transform em BLOCOS PARALELOS (Fase 3 da spec)
--
-- Mesmo conteúdo do 03_transform.sql, produzido de outro jeito: cada tabela
-- grande é fatiada em faixas de `ctid` da staging, e as faixas podem ser
-- carregadas ao mesmo tempo, em sessões diferentes. A 2.8 mediu 2,5× com 4
-- faixas — e o recorte medido lá é exatamente este, faixa de posição física,
-- não `split` de arquivo nem CSV intermediário. O bloco é um predicado no FROM.
--
-- ⚠️ A DUPLICAÇÃO COM O 03_transform.sql É PROPOSITAL, e não deve ser
-- "consertada" fatorando um dos dois no outro. São dois caminhos independentes
-- que têm de concordar, e o T10 é o teste que cobra a concordância — três vezes
-- seguidas. Se os dois virarem o mesmo código, o T10 passa a provar nada.
--
-- Como este arquivo é usado:
--   psql -f 03_transform_v2.sql                 -> define as funções E carrega
--                                                  tudo, sequencialmente (T10)
--   psql -v driver=0 -f 03_transform_v2.sql     -> só define as funções; quem
--                                                  chama e paraleliza é o load.sh
--
-- O paralelismo real NÃO mora aqui: uma sessão psql executa um comando por vez.
-- Ele mora no load.sh, que dispara `SELECT carga.carregar_*(de, ate)` em N
-- sessões simultâneas, N = LOAD_JOBS, derivado do orçamento na Fase 0.
-- ============================================================================

\set ON_ERROR_STOP on

DO $$
BEGIN
    IF to_regclass('carga.rejeito') IS NULL THEN
        RAISE EXCEPTION
            'schema `carga` ausente — aplique analytics/00_carga.sql antes do transform';
    END IF;
END $$;

-- ----------------------------------------------------------------------------
-- A tabela de CNPJ básico — a única que NÃO é fatiada
--
-- Ela é a exceção porque a Fase 2 mantém a PK dela viva: o `ON CONFLICT` precisa
-- de um índice único para arbitrar qual das linhas repetidas fica. Com faixas
-- concorrentes, "a primeira linha do arquivo vence" deixaria de existir — quem
-- vence passaria a depender do escalonamento, e o dado do mês viraria sorteio
-- (o caso 08314885 da seção 2.6). Bloco único, sessão única, sem discussão.
--
-- O custo é aceitável e foi medido: 14,6 min contra 30,4 min da tabela de
-- estabelecimentos (2.1). O caminho crítico continua sendo o outro, e esse é
-- fatiado.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION carga.carregar_empresa() RETURNS bigint
    LANGUAGE plpgsql AS $fn$
DECLARE
    v_inseridas bigint;
BEGIN
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
    SELECT count(*) INTO v_inseridas FROM ins;

    INSERT INTO carga.rejeito (competencia, tabela, regra, coluna, linha_bruta)
    SELECT carga.competencia(), 'empresa', r.regra, r.coluna, e::text
    FROM staging.empresas e
    CROSS JOIN LATERAL (SELECT e.cnpj_basico ~ '^\d{8}$' AS chave_ok) v
    CROSS JOIN LATERAL (VALUES
        ('S4',  NULL::text,          NOT v.chave_ok),
        ('S11', 'natureza_juridica', v.chave_ok AND carga.perdeu(e.natureza_juridica, carga.num_smallint(e.natureza_juridica) IS NULL)),
        ('S11', 'qualificacao_resp', v.chave_ok AND carga.perdeu(e.qualificacao_resp, carga.num_smallint(e.qualificacao_resp) IS NULL)),
        ('S11', 'capital_social',    v.chave_ok AND carga.perdeu(e.capital_social,    carga.num_capital(e.capital_social)     IS NULL)),
        ('S11', 'porte',             v.chave_ok AND carga.perdeu(e.porte,             carga.num_smallint(e.porte)             IS NULL))
    ) AS r(regra, coluna, ruim)
    WHERE r.ruim;

    PERFORM carga.contar('empresa', 'linhas_inseridas', v_inseridas);
    PERFORM carga.contar('empresa', 'linhas_lidas', (SELECT count(*) FROM staging.empresas));
    PERFORM carga.contar('empresa', 'S7_capital_com_virgula',
                         (SELECT count(*) FROM staging.empresas WHERE capital_social LIKE '%,%'));
    RETURN v_inseridas;
END;
$fn$;

-- ----------------------------------------------------------------------------
-- As faixas
--
-- `relpages` do pg_class não serve: ele só é atualizado por ANALYZE/VACUUM, e
-- aqui a staging acabou de ser preenchida por COPY. O tamanho real do arquivo é
-- a única fonte confiável neste ponto da carga.
--
-- As páginas vão de 0 a p-1, então `ctid < (p,0)` cobre a tabela inteira e
-- nenhuma linha fica de fora — o erro clássico deste recorte.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION carga.faixas(rel regclass, blocos integer)
    RETURNS TABLE(bloco integer, de tid, ate tid) LANGUAGE sql STABLE AS $fn$
    SELECT i + 1,
           ('(' || (i * por_bloco) || ',0)')::tid,
           ('(' || least((i + 1) * por_bloco, paginas) || ',0)')::tid
    FROM (
        SELECT paginas, greatest(ceil(paginas::numeric / greatest(blocos, 1))::bigint, 1) AS por_bloco
        FROM (SELECT greatest(pg_relation_size(rel) / current_setting('block_size')::bigint, 1) AS paginas) t
    ) f
    CROSS JOIN generate_series(0, greatest(blocos, 1) - 1) i
    WHERE i * f.por_bloco < f.paginas;
$fn$;

-- ----------------------------------------------------------------------------
-- Preparação e fechamento — o que roda UMA vez, fora dos blocos
--
-- O TRUNCATE é o motivo de esta função existir. Ele continua no início do
-- transform, como sempre, mas tem de rodar antes de qualquer bloco: um TRUNCATE
-- dentro de um bloco apagaria o que os outros já inseriram.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION carga.preparar() RETURNS void LANGUAGE plpgsql AS $fn$
BEGIN
    PERFORM carga.resumo_atual();
    PERFORM carga.limpar_competencia();
    TRUNCATE analytics.dim_cnae, analytics.dim_natureza_juridica,
             analytics.dim_qualificacao, analytics.dim_pais,
             analytics.dim_motivo_situacao, analytics.dim_municipio;
    TRUNCATE analytics.empresa;
    TRUNCATE analytics.estabelecimento;
    TRUNCATE analytics.estabelecimento_cnae_secundario;
    TRUNCATE analytics.socio RESTART IDENTITY;
    TRUNCATE analytics.simples;
END;
$fn$;

-- ----------------------------------------------------------------------------
-- Dimensões — pequenas, não valem bloco; entram inteiras num job só
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION carga.carregar_dimensoes() RETURNS void LANGUAGE plpgsql AS $fn$
BEGIN
    INSERT INTO analytics.dim_cnae (codigo, descricao)
    SELECT carga.num_integer(codigo), descricao FROM staging.cnaes
    WHERE carga.num_integer(codigo) IS NOT NULL ON CONFLICT DO NOTHING;

    INSERT INTO analytics.dim_natureza_juridica (codigo, descricao)
    SELECT carga.num_smallint(codigo), descricao FROM staging.naturezas
    WHERE carga.num_smallint(codigo) IS NOT NULL ON CONFLICT DO NOTHING;

    INSERT INTO analytics.dim_qualificacao (codigo, descricao)
    SELECT carga.num_smallint(codigo), descricao FROM staging.qualificacoes
    WHERE carga.num_smallint(codigo) IS NOT NULL ON CONFLICT DO NOTHING;

    INSERT INTO analytics.dim_pais (codigo, nome)
    SELECT carga.num_smallint(codigo), descricao FROM staging.paises
    WHERE carga.num_smallint(codigo) IS NOT NULL ON CONFLICT DO NOTHING;

    INSERT INTO analytics.dim_motivo_situacao (codigo, descricao)
    SELECT carga.num_smallint(codigo), descricao FROM staging.motivos
    WHERE carga.num_smallint(codigo) IS NOT NULL ON CONFLICT DO NOTHING;

    INSERT INTO analytics.dim_municipio (codigo, nome)
    SELECT carga.num_integer(codigo), descricao FROM staging.municipios
    WHERE carga.num_integer(codigo) IS NOT NULL ON CONFLICT DO NOTHING;

    -- S5, uma varredura por dimensão; são tabelas de milhares de linhas.
    INSERT INTO carga.rejeito (competencia, tabela, regra, coluna, linha_bruta)
    SELECT carga.competencia(), 'dim_cnae', 'S5', 'codigo', c::text
    FROM staging.cnaes c WHERE carga.num_integer(c.codigo) IS NULL;
    INSERT INTO carga.rejeito (competencia, tabela, regra, coluna, linha_bruta)
    SELECT carga.competencia(), 'dim_natureza_juridica', 'S5', 'codigo', c::text
    FROM staging.naturezas c WHERE carga.num_smallint(c.codigo) IS NULL;
    INSERT INTO carga.rejeito (competencia, tabela, regra, coluna, linha_bruta)
    SELECT carga.competencia(), 'dim_qualificacao', 'S5', 'codigo', c::text
    FROM staging.qualificacoes c WHERE carga.num_smallint(c.codigo) IS NULL;
    INSERT INTO carga.rejeito (competencia, tabela, regra, coluna, linha_bruta)
    SELECT carga.competencia(), 'dim_pais', 'S5', 'codigo', c::text
    FROM staging.paises c WHERE carga.num_smallint(c.codigo) IS NULL;
    INSERT INTO carga.rejeito (competencia, tabela, regra, coluna, linha_bruta)
    SELECT carga.competencia(), 'dim_motivo_situacao', 'S5', 'codigo', c::text
    FROM staging.motivos c WHERE carga.num_smallint(c.codigo) IS NULL;
    INSERT INTO carga.rejeito (competencia, tabela, regra, coluna, linha_bruta)
    SELECT carga.competencia(), 'dim_municipio', 'S5', 'codigo', c::text
    FROM staging.municipios c WHERE carga.num_integer(c.codigo) IS NULL;

    INSERT INTO carga.contador (competencia, tabela, regra, quantidade)
    SELECT carga.competencia(), tabela, 'S5', count(*)
    FROM carga.rejeito
    WHERE competencia = carga.competencia() AND regra = 'S5'
    GROUP BY tabela;
END;
$fn$;

-- ----------------------------------------------------------------------------
-- estabelecimento — o caminho crítico, e o motivo de os blocos existirem
--
-- Sobre o `ON CONFLICT DO NOTHING` SEM especificar a chave: é o que faz esta
-- função funcionar nos dois mundos. Depois da Fase 2 a PK está dropada e não há
-- nada a arbitrar — todas as linhas entram, e a Fase 4 decide o que fazer com
-- as repetidas (índice não-único + quarentena, 7.1). Com a PK viva — que é o
-- caso quando esta função roda sem a Fase 2, nos testes — ele dedupe como
-- sempre. Especificar `(cnpj, uf)` quebraria o primeiro caso com "no unique or
-- exclusion constraint matching the ON CONFLICT specification".
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION carga.carregar_estabelecimento(p_de tid, p_ate tid)
    RETURNS bigint LANGUAGE plpgsql AS $fn$
DECLARE
    v_inseridas bigint;
BEGIN
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
            coalesce(carga.cabe(nullif(btrim(uf), ''), 2), '??'),
            carga.num_integer(municipio),
            nullif(btrim(ddd_1 || telefone_1), ''),
            nullif(btrim(ddd_2 || telefone_2), ''),
            nullif(btrim(ddd_fax || fax), ''),
            nullif(correio_eletronico, ''),
            nullif(situacao_especial, ''),
            analytics.parse_date(data_situacao_especial)
        FROM staging.estabelecimentos
        WHERE ctid >= p_de AND ctid < p_ate
          AND cnpj_basico ~ '^\d{8}$'
          AND length(cnpj_basico || cnpj_ordem || cnpj_dv) <= 14
        ON CONFLICT DO NOTHING
        RETURNING 1
    )
    SELECT count(*) INTO v_inseridas FROM ins;

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
        ('S6',  'data_inicio_atividade',       v.entrou AND carga.perdeu_data(e.data_inicio_atividade,   analytics.parse_date(e.data_inicio_atividade)   IS NULL)),
        ('S6',  'data_situacao_especial',      v.entrou AND carga.perdeu_data(e.data_situacao_especial,  analytics.parse_date(e.data_situacao_especial)  IS NULL))
    ) AS r(regra, coluna, ruim)
    WHERE e.ctid >= p_de AND e.ctid < p_ate AND r.ruim;

    PERFORM carga.contar('estabelecimento', 'linhas_inseridas', v_inseridas);
    RETURN v_inseridas;
END;
$fn$;

-- ----------------------------------------------------------------------------
-- CNAEs secundários — mesma fonte, e é por isso que ele anda JUNTO
--
-- A 2.9 mediu: lidos em sequência, os dois consumidores da mesma staging são
-- duas varreduras; disparados ao mesmo tempo, o segundo acha em cache o que o
-- primeiro trouxe — 22% mais barato (320 s contra 409 s). O load.sh dispara os
-- dois juntos, ocupando 2 dos LOAD_JOBS.
--
-- O DISTINCT resolve a repetição DENTRO do bloco (o mesmo CNAE 6× na lista de
-- uma linha, caso real da 2.6). Entre blocos não há o que resolver: exigiria o
-- mesmo estabelecimento em duas faixas, e a 2.6 mediu isso como zero.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION carga.carregar_cnae_secundario(p_de tid, p_ate tid)
    RETURNS bigint LANGUAGE plpgsql AS $fn$
DECLARE
    v_inseridas bigint;
BEGIN
    WITH bruto AS (
        SELECT cnpj_basico || cnpj_ordem || cnpj_dv AS cnpj,
               btrim(unnest(string_to_array(nullif(cnae_secundaria, ''), ','))) AS cnae
        FROM staging.estabelecimentos
        WHERE ctid >= p_de AND ctid < p_ate
          AND cnpj_basico ~ '^\d{8}$'
          AND length(cnpj_basico || cnpj_ordem || cnpj_dv) <= 14
          AND nullif(cnae_secundaria, '') IS NOT NULL
    ), ins AS (
        INSERT INTO analytics.estabelecimento_cnae_secundario (cnpj, cnae_cod)
        SELECT DISTINCT cnpj, carga.num_integer(cnae)
        FROM bruto WHERE carga.num_integer(cnae) IS NOT NULL
        ON CONFLICT DO NOTHING
        RETURNING 1
    ), rej AS (
        INSERT INTO carga.rejeito (competencia, tabela, regra, coluna, linha_bruta)
        SELECT carga.competencia(), 'estabelecimento_cnae_secundario', 'S9', 'cnae',
               cnpj || ';' || cnae
        FROM bruto WHERE carga.num_integer(cnae) IS NULL
        RETURNING 1
    )
    SELECT count(*) INTO v_inseridas FROM ins;

    PERFORM carga.contar('estabelecimento_cnae_secundario', 'linhas_inseridas', v_inseridas);
    RETURN v_inseridas;
END;
$fn$;

-- ----------------------------------------------------------------------------
-- socio — espelha a fonte, sem dedupe (decisão 1 da spec, seção 5.2)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION carga.carregar_socio(p_de tid, p_ate tid)
    RETURNS bigint LANGUAGE plpgsql AS $fn$
DECLARE
    v_inseridas bigint;
BEGIN
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
        WHERE ctid >= p_de AND ctid < p_ate
          AND cnpj_basico ~ '^\d{8}$'
        RETURNING 1
    )
    SELECT count(*) INTO v_inseridas FROM ins;

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
        ('S12', 'cnpj_cpf_socio',       v.chave_ok AND carga.perdeu(nullif(s.cnpj_cpf_socio, ''),    carga.cabe(nullif(s.cnpj_cpf_socio, ''), 14)    IS NULL)),
        ('S12', 'cpf_representante',    v.chave_ok AND carga.perdeu(nullif(s.cpf_representante, ''), carga.cabe(nullif(s.cpf_representante, ''), 14) IS NULL)),
        ('S6',  'data_entrada_sociedade', v.chave_ok AND carga.perdeu_data(s.data_entrada_sociedade, analytics.parse_date(s.data_entrada_sociedade) IS NULL))
    ) AS r(regra, coluna, ruim)
    WHERE s.ctid >= p_de AND s.ctid < p_ate AND r.ruim;

    PERFORM carga.contar('socio', 'linhas_inseridas', v_inseridas);
    RETURN v_inseridas;
END;
$fn$;

-- ----------------------------------------------------------------------------
-- simples
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION carga.carregar_simples(p_de tid, p_ate tid)
    RETURNS bigint LANGUAGE plpgsql AS $fn$
DECLARE
    v_inseridas bigint;
BEGIN
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
        WHERE ctid >= p_de AND ctid < p_ate
          AND cnpj_basico ~ '^\d{8}$'
        ON CONFLICT DO NOTHING
        RETURNING 1
    )
    SELECT count(*) INTO v_inseridas FROM ins;

    INSERT INTO carga.rejeito (competencia, tabela, regra, coluna, linha_bruta)
    SELECT carga.competencia(), 'simples', r.regra, r.coluna, s::text
    FROM staging.simples s
    CROSS JOIN LATERAL (SELECT s.cnpj_basico ~ '^\d{8}$' AS chave_ok) v
    CROSS JOIN LATERAL (VALUES
        ('S4', NULL::text,               NOT v.chave_ok),
        ('S6', 'data_opcao_simples',     v.chave_ok AND carga.perdeu_data(s.data_opcao_simples,    analytics.parse_date(s.data_opcao_simples)    IS NULL)),
        ('S6', 'data_exclusao_simples',  v.chave_ok AND carga.perdeu_data(s.data_exclusao_simples, analytics.parse_date(s.data_exclusao_simples) IS NULL)),
        ('S6', 'data_opcao_mei',         v.chave_ok AND carga.perdeu_data(s.data_opcao_mei,        analytics.parse_date(s.data_opcao_mei)        IS NULL)),
        ('S6', 'data_exclusao_mei',      v.chave_ok AND carga.perdeu_data(s.data_exclusao_mei,     analytics.parse_date(s.data_exclusao_mei)     IS NULL))
    ) AS r(regra, coluna, ruim)
    WHERE s.ctid >= p_de AND s.ctid < p_ate AND r.ruim;

    PERFORM carga.contar('simples', 'linhas_inseridas', v_inseridas);
    RETURN v_inseridas;
END;
$fn$;

-- ----------------------------------------------------------------------------
-- Fechamento — contadores que só fazem sentido com todos os blocos prontos
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION carga.finalizar() RETURNS void LANGUAGE plpgsql AS $fn$
BEGIN
    PERFORM carga.contar('estabelecimento', 'linhas_lidas',
                         (SELECT count(*) FROM staging.estabelecimentos));
    PERFORM carga.contar('estabelecimento', 'S8_uf_default',
                         (SELECT count(*) FROM analytics.estabelecimento_default));
    PERFORM carga.contar('socio', 'linhas_lidas', (SELECT count(*) FROM staging.socios));
    PERFORM carga.contar('simples', 'linhas_lidas', (SELECT count(*) FROM staging.simples));

    -- S10: o que o ON CONFLICT engoliu. Depois da Fase 2 a PK está dropada e
    -- este número é zero por construção — a duplicata sobrevive até a Fase 4,
    -- que a põe em quarentena. Antes da Fase 2 (testes) ele conta de verdade.
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
END;
$fn$;

-- ----------------------------------------------------------------------------
-- Driver embutido
--
-- Roda todos os blocos EM SEQUÊNCIA, numa sessão só. Serve para dois casos:
-- rodar o arquivo na mão, e o T10 — que compara este caminho com o do
-- 03_transform.sql e exige hash idêntico, três vezes seguidas.
--
-- `-v driver=0` pula este trecho: é assim que o load.sh carrega só as
-- definições e depois dispara os blocos em N sessões paralelas.
-- ----------------------------------------------------------------------------
\if :{?driver}
\else
  \set driver 1
\endif

\if :driver
DO $drv$
DECLARE
    v_blocos integer := coalesce(nullif(current_setting('carga.blocos', true), ''), '1')::integer;
    f record;
BEGIN
    PERFORM carga.preparar();
    PERFORM carga.carregar_dimensoes();
    PERFORM carga.carregar_empresa();

    FOR f IN SELECT * FROM carga.faixas('staging.estabelecimentos', v_blocos) ORDER BY bloco LOOP
        PERFORM carga.carregar_estabelecimento(f.de, f.ate);
        PERFORM carga.carregar_cnae_secundario(f.de, f.ate);
    END LOOP;

    FOR f IN SELECT * FROM carga.faixas('staging.socios', v_blocos) ORDER BY bloco LOOP
        PERFORM carga.carregar_socio(f.de, f.ate);
    END LOOP;

    FOR f IN SELECT * FROM carga.faixas('staging.simples', v_blocos) ORDER BY bloco LOOP
        PERFORM carga.carregar_simples(f.de, f.ate);
    END LOOP;

    PERFORM carga.finalizar();
END;
$drv$;
\endif
