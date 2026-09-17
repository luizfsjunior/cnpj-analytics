"""T1 — a estrutura final do banco não muda (R1 da spec-carga.md).

O requisito é **idêntico**, não "equivalente": a API e os consumidores que acessam
o banco direto não podem perceber a troca da carga. O critério é o
`pg_dump --schema-only --schema=analytics`, normalizado e comparado por hash.

Este teste tem de estar VERDE hoje, contra a carga atual — é o que prova que o
contrato descreve o que existe. Ele fica verde depois da v2 se, e só se, a Fase 4
recriar todos os índices exatamente como o `04_indexes.sql` os cria.

Precisa do container do compose de pé; pula sozinho se não estiver.
"""

import pytest

from fixture_carga import SQL, dump_schema, hash_schema, preparar_banco, rodar_transform


def _aplicar_indices(psql_db):
    r = psql_db.arquivo(SQL / "04_indexes.sql")
    assert r.returncode == 0, f"04_indexes.sql falhou:\n{r.stderr[-2000:]}"


def test_transform_nao_altera_a_estrutura(psql_db):
    """Rodar o transform não pode mexer em uma vírgula do schema."""
    preparar_banco(psql_db)
    _aplicar_indices(psql_db)
    dump_antes = dump_schema(psql_db.nome_banco)

    rodar_transform(psql_db)

    dump_depois = dump_schema(psql_db.nome_banco)
    if dump_antes != dump_depois:
        import difflib
        diff = "\n".join(difflib.unified_diff(
            dump_antes.splitlines(), dump_depois.splitlines(),
            fromfile="antes", tofile="depois", lineterm="", n=1))
        pytest.fail(
            "a estrutura de analytics mudou durante a carga — R1 violado.\n" + diff[:3000]
        )


def test_schema_mais_indices_e_reprodutivel(psql_db):
    """Aplicar 01+04 duas vezes dá a mesma estrutura.

    Guarda contra `CREATE INDEX IF NOT EXISTS` que silenciosamente não recria o
    que deveria — o modo mais provável de a Fase 4 da v2 quebrar o R1 sem que
    ninguém perceba.
    """
    preparar_banco(psql_db)
    _aplicar_indices(psql_db)
    primeira = hash_schema(psql_db.nome_banco)

    r = psql_db.arquivo(SQL / "01_schema.sql")
    assert r.returncode == 0, r.stderr
    _aplicar_indices(psql_db)

    assert hash_schema(psql_db.nome_banco) == primeira


def test_indices_das_particoes_tem_nome_gerado_pelo_postgres(psql_db):
    """Os índices das 28 partições NÃO têm nome escolhido por nós.

    Descoberto ao escrever o T1. O `04_indexes.sql` cria o índice no **pai**
    particionado, e o Postgres propaga para cada partição gerando o nome
    (`estabelecimento_ac_cnpj_basico_idx`) — inclusive truncando em 63
    caracteres quando passa do limite, como em
    `estabelecimento_default_cnae_fiscal_principal_situacao_cada_idx`.

    Por que isso importa para a v2: a Fase 4 recria índices a partir do DDL
    salvo. Se ela recriar **partição a partição**, os nomes gerados podem sair
    diferentes, e o T1 acusa mudança de estrutura — corretamente. A Fase 4 tem
    de recriar **pelo pai**, deixando o Postgres regenerar os mesmos nomes, na
    mesma ordem.

    Este teste documenta a restrição e falha se a premissa mudar.
    """
    preparar_banco(psql_db)
    _aplicar_indices(psql_db)

    r = psql_db.sql(
        "SELECT count(*) FROM pg_index i "
        "JOIN pg_class c ON c.oid = i.indrelid "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = 'analytics' AND indexrelid::regclass::text ~ '_idx$'"
    )
    assert r.returncode == 0, r.stderr
    quantidade = int(next(l.strip() for l in r.stdout.splitlines() if l.strip().isdigit()))
    assert quantidade > 100, (
        "esperava mais de 100 índices com nome gerado (6 por partição × 28+1). "
        f"Vieram {quantidade} — a premissa da Fase 4 mudou, revise a spec."
    )


def test_recriar_indices_pelo_pai_preserva_os_nomes(psql_db):
    """O ensaio da Fase 4: dropar do pai e recriar reproduz a mesma estrutura.

    É o teste que prova que a v2 **pode** dropar os índices antes do transform
    sem violar o R1 — e ele passa hoje, antes de qualquer implementação, o que
    significa que a Fase 4 é viável como especificada.
    """
    preparar_banco(psql_db)
    _aplicar_indices(psql_db)
    antes = dump_schema(psql_db.nome_banco)

    r = psql_db.sql(
        "DO $$ DECLARE r record; BEGIN "
        "  FOR r IN SELECT indexrelid::regclass::text AS nome FROM pg_index i "
        "           JOIN pg_class c ON c.oid = i.indrelid "
        "           JOIN pg_namespace n ON n.oid = c.relnamespace "
        "           WHERE n.nspname = 'analytics' AND NOT indisprimary "
        "             AND c.relname = 'estabelecimento' "
        "  LOOP EXECUTE 'DROP INDEX ' || r.nome; END LOOP; END $$;"
    )
    assert r.returncode == 0, r.stderr
    _aplicar_indices(psql_db)

    depois = dump_schema(psql_db.nome_banco)
    if antes != depois:
        import difflib
        diff = "\n".join(difflib.unified_diff(
            antes.splitlines(), depois.splitlines(),
            fromfile="antes", tofile="depois", lineterm="", n=1))
        pytest.fail("recriar pelo pai NÃO reproduziu a estrutura:\n" + diff[:3000])
