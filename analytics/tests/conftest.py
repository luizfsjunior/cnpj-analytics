"""Infra dos testes do analytics/ — load.sh (shell) e os .sql.

Dois mundos bem diferentes convivem aqui:

* `ambiente_stub` roda o **load.sh de verdade** com um PATH em que `unzip`,
  `psql`, `curl` e (quando o teste quer) `rg` são scripts controlados. Nenhum
  byte da Receita é lido e nenhum banco é tocado — o que se observa é o
  comportamento do script: com o que ele se importa, o que ele tolera e com que
  código de saída ele termina.

* `psql_db` sobe um banco descartável no container do compose para os testes que
  precisam de um Postgres real (os .sql). Pulam sozinhos se o container não
  estiver de pé.
"""

import os
import shutil
import subprocess
import textwrap
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Achar o bash CERTO — armadilha que já custou caro:
#
# `subprocess.run(["bash", ...])` no Windows NÃO roda o Git Bash. O CreateProcess
# procura em System32 antes do PATH, e lá mora o bash.exe **launcher do WSL**.
# O WSL não herda o ambiente do processo pai: todas as variáveis do teste somem,
# o load.sh cai nos defaults (DB=cnpj, sem PGHOST -> `docker compose exec`,
# DATA_DIR no fallback ../minha-receita/data) e a "simulação" vira uma carga real
# contra o container. Por isso aqui se usa caminho absoluto + teste de sanidade.
# ---------------------------------------------------------------------------

# Ordem importa: `Git\usr\bin\bash.exe` é o bash puro e preserva o PATH que
# passamos. Já `Git\bin\bash.exe` é um wrapper que antepõe /mingw64/bin e
# /usr/bin — com ele o `unzip` real vence o stub (o `psql`, que não existe no
# MSYS, ainda cairia no stub: o resultado é um teste meio stubado, pior que um
# que falha).
CANDIDATOS_BASH = [
    os.environ.get("BASH_PARA_TESTES", ""),
    os.path.expandvars(r"%LOCALAPPDATA%\Programs\Git\usr\bin\bash.exe"),
    r"C:\Program Files\Git\usr\bin\bash.exe",
    os.path.expandvars(r"%LOCALAPPDATA%\Programs\Git\bin\bash.exe"),
    r"C:\Program Files\Git\bin\bash.exe",
    "/usr/bin/bash",
    "/bin/bash",
]


def _herda_ambiente(exe: str) -> bool:
    """O bash escolhido enxerga as variáveis que passamos? (o do WSL não)"""
    try:
        r = subprocess.run(
            [exe, "-c", "echo [$SANIDADE_CONFTEST]"],
            env={**os.environ, "SANIDADE_CONFTEST": "ok"},
            capture_output=True, text=True, encoding="utf-8", timeout=30,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return "[ok]" in r.stdout


@pytest.fixture(scope="session")
def bash_exe():
    for cand in CANDIDATOS_BASH:
        if cand and Path(cand).exists() and _herda_ambiente(cand):
            return cand
    pytest.skip(
        "nenhum bash que herde o ambiente foi encontrado "
        "(o bash.exe do System32 é o launcher do WSL e não serve); "
        "aponte um com BASH_PARA_TESTES=<caminho>"
    )

RAIZ = Path(__file__).resolve().parent.parent.parent
LOAD_SH = "analytics/load.sh"   # relativo: o bash roda com cwd=RAIZ
PG_SERVICE = "postgres-cnpj-rfb"


def posix(p) -> str:
    """'C:\\Users\\x' -> '/c/Users/x'.

    Variáveis como DATA_DIR chegam ao bash como string crua: um caminho Windows
    perde as barras invertidas no caminho (viram escapes) e o script não acha
    nada. O PATH não passa por aqui — o MSYS já o converte na inicialização.
    """
    s = Path(p).resolve().as_posix()
    if len(s) > 1 and s[1] == ":":
        s = "/" + s[0].lower() + s[2:]
    return s

# Zips que o load.sh procura. O conteúdo é irrelevante: `unzip` é stub.
# `entidades-lucro-real.zip` NÃO é decoração: o load.sh troca o DATA_DIR pelo
# ../minha-receita/data se não achar esse arquivo (é a âncora do modo
# REGIME_ONLY). Sem ele, o teste sai lendo a pasta de dados real.
ZIPS = [
    "Cnaes.zip", "Naturezas.zip", "Qualificacoes.zip", "Paises.zip",
    "Motivos.zip", "Municipios.zip", "Empresas0.zip", "Estabelecimentos0.zip",
    "Socios0.zip", "Simples.zip", "entidades-lucro-real.zip",
]

# Uma linha no layout de Estabelecimentos (só o 1º campo importa: cnpj_basico).
LINHA_CSV = '"12345678";"0001";"91";"1";"FANTASIA";"02";"20200101"' + ';""' * 23

# Quantas colunas cada zip tem, para o stub de `unzip` cuspir o layout CERTO de
# cada um. Antes o stub devolvia a linha de Estabelecimentos para todos os zips,
# o que bastava enquanto ninguém olhava o formato. A conferência de layout da
# Fase 1 (R2.2) olha: com um stub uniforme, `Empresas0.zip` chegaria com 30
# colunas em vez de 7 e toda carga simulada reprovaria.
COLUNAS_POR_ZIP = {
    "Empresas": 7, "Estabelecimentos": 30, "Socios": 11, "Simples": 7,
    "Cnaes": 2, "Naturezas": 2, "Qualificacoes": 2, "Paises": 2,
    "Motivos": 2, "Municipios": 2,
}


def _escrever_stub(caminho: Path, corpo: str) -> None:
    caminho.write_text("#!/bin/sh\n" + textwrap.dedent(corpo), encoding="utf-8", newline="\n")
    caminho.chmod(0o755)


@pytest.fixture
def stub_bin(tmp_path):
    """Diretório de stubs que entra na frente do PATH.

    `psql` aceita qualquer coisa e devolve o mínimo que o load.sh precisa para
    seguir adiante; `unzip` cospe uma linha de CSV; `curl` falha (o de-para IBGE
    é opcional e o script já tolera a falha, então o teste não depende de rede).
    """
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()

    # Consome stdin sempre (senão o produtor do pipe morre de SIGPIPE) e responde
    # às duas consultas cujo resultado o load.sh realmente lê.
    _escrever_stub(bin_dir / "psql", f"""
        cat > /dev/null 2>&1
        for arg in "$@"; do
            case "$arg" in
                *"FROM pg_database"*) echo 1 ;;
                *"SELECT DISTINCT cnpj_basico"*) echo 12345678 ;;
            esac
        done
        exit 0
    """)

    # O último argumento é o caminho do zip (`unzip -p <zip>`): o stub escolhe o
    # número de campos pelo nome do arquivo, para que a conferência de layout do
    # load.sh veja o mesmo que veria com os zips reais.
    casos = "\n".join(
        f'            *{nome}*) campos={n} ;;' for nome, n in COLUNAS_POR_ZIP.items()
    )
    _escrever_stub(bin_dir / "unzip", f"""
        for arg in "$@"; do zip="$arg"; done
        campos=30
        case "$zip" in
{casos}
        esac
        linha='"12345678"'
        i=1
        while [ "$i" -lt "$campos" ]; do linha="$linha;\\"\\""; i=$((i+1)); done
        echo "$linha"
        exit 0
    """)

    # Sem rede nos testes: o load.sh trata a falha do de-para IBGE com um aviso.
    _escrever_stub(bin_dir / "curl", """
        exit 1
    """)

    return bin_dir


@pytest.fixture
def data_dir(tmp_path):
    """Pasta de zips vazios — `unzip` é stub, o conteúdo nunca é lido."""
    d = tmp_path / "data"
    d.mkdir()
    for nome in ZIPS:
        (d / nome).touch()
    return d


@pytest.fixture
def ibge_cache(tmp_path):
    """Cache do de-para IBGE já preenchido, para o load.sh não sair na rede.

    `fetch_cached` só chama `curl` quando o cache está ausente ou vencido; com os
    arquivos aqui, o teste não depende de internet nem espera timeout de rede.
    """
    d = tmp_path / "cache"
    d.mkdir()
    (d / "tabmun.csv").write_text("7107;;SAO PAULO;SP;3550308\n", encoding="utf-8", newline="\n")
    (d / "ibge_municipios.json").write_text('[{"id":3550308,"nome":"São Paulo"}]', encoding="utf-8", newline="\n")
    return d


@pytest.fixture
def rodar_load(bash_exe, stub_bin, data_dir, ibge_cache, tmp_path):
    """Executa o load.sh com o ambiente stubado. Devolve o CompletedProcess.

    `extra_env` sobrescreve/injeta variáveis; `com_rg` instala um stub de `rg`
    com o código de saída pedido (127 = binário ausente, 1 = nenhum match).
    """

    def _rodar(*, sample="1", com_rg=None, extra_env=None, timeout=180):
        if com_rg is not None:
            _escrever_stub(stub_bin / "rg", f"exit {com_rg}\n")

        env = dict(os.environ)
        env["PATH"] = str(stub_bin) + os.pathsep + env["PATH"]
        env.update({
            # PGHOST setado -> o load.sh usa `psql` direto (nosso stub), e não
            # `docker compose exec`. Ver o bloco de conexão do script.
            "PGHOST": "localhost",
            "PGPORT": "5432",
            "PGUSER": "cnpj",
            "PGPASSWORD": "cnpj",
            "DB": "banco_de_teste",
            "SAMPLE": sample,
            "DATA_DIR": posix(data_dir),
            "IBGE_CACHE_DIR": posix(ibge_cache),
            "TIMING": "0",   # saída enxuta; a instrumentação tem teste próprio
            # TUNE fica em 1 (o default): o ALTER SYSTEM cai no psql stub e não
            # toca em nada. TUNE=0 tem bug próprio e teste próprio — usar aqui
            # contaminaria todos os outros casos com aquela falha.
            "TUNE": "1",
        })
        env.update(extra_env or {})

        # Cinto de segurança: se por qualquer motivo o ambiente não chegar ao
        # script, ele usaria `docker compose exec` no container REAL e o
        # DATA_DIR real. Melhor abortar o teste do que "simular" uma carga de
        # verdade. PG_SERVICE aponta para um serviço inexistente de propósito.
        env["PG_SERVICE"] = "servico-que-nao-existe-teste"
        sanidade = subprocess.run(
            [bash_exe, "-c", 'echo "[$DB]"; command -v unzip; command -v psql'],
            cwd=str(RAIZ), env=env, capture_output=True, text=True, encoding="utf-8", timeout=30,
        )
        assert env["DB"] in sanidade.stdout, (
            "o ambiente não chegou ao bash — o teste rodaria contra o banco e os "
            f"dados REAIS. bash={bash_exe} saída={sanidade.stdout!r}"
        )
        # E os stubs precisam VENCER os binários reais: com o bash errado, o
        # `unzip` de /usr/bin ganha do stub e o teste passa a ler zip de verdade.
        alvo = posix(stub_bin)
        for linha in sanidade.stdout.splitlines()[1:]:
            assert linha.strip().startswith(alvo), (
                f"o stub não está vencendo o PATH ({linha.strip()!r} em vez de "
                f"algo em {alvo}); bash={bash_exe}"
            )

        return subprocess.run(
            [bash_exe, LOAD_SH],
            cwd=str(RAIZ), env=env, capture_output=True, text=True, encoding="utf-8",
            timeout=timeout, errors="replace",
        )

    return _rodar


# --------------------------------------------------------------------------
# Postgres real (testes dos .sql)
# --------------------------------------------------------------------------

def _compose(*args, **kw):
    return subprocess.run(
        ["docker", "compose", *args], cwd=str(RAIZ),
        capture_output=True, text=True, encoding="utf-8", errors="replace", **kw,
    )


def _postgres_no_ar() -> bool:
    r = _compose("exec", "-T", PG_SERVICE, "pg_isready", "-U", "cnpj")
    return r.returncode == 0


@pytest.fixture(scope="session")
def postgres():
    if shutil.which("docker") is None:
        pytest.skip("docker não está no PATH")
    if not _postgres_no_ar():
        pytest.skip(f"container {PG_SERVICE} não está de pé (docker compose up -d {PG_SERVICE})")
    return True


@pytest.fixture
def psql_db(postgres):
    """Banco descartável. Devolve um runner de SQL; dropa o banco no fim."""
    nome = f"cnpj_test_{os.getpid()}"

    def _admin(sql):
        return _compose("exec", "-T", PG_SERVICE, "psql", "-U", "cnpj", "-d", "postgres",
                        "-v", "ON_ERROR_STOP=1", "-c", sql)

    _admin(f'DROP DATABASE IF EXISTS "{nome}" WITH (FORCE)')
    criado = _admin(f'CREATE DATABASE "{nome}"')
    assert criado.returncode == 0, criado.stderr

    class Runner:
        nome_banco = nome

        def sql(self, texto):
            """Roda SQL inline. Não levanta: o teste inspeciona o resultado."""
            return _compose("exec", "-T", PG_SERVICE, "psql", "-U", "cnpj", "-d", nome,
                            "-v", "ON_ERROR_STOP=1", "-c", texto)

        def arquivo(self, caminho):
            """Roda um .sql do repo via stdin (o container não monta o repo)."""
            sql = Path(caminho).read_text(encoding="utf-8")
            return _compose("exec", "-T", PG_SERVICE, "psql", "-U", "cnpj", "-d", nome,
                            "-v", "ON_ERROR_STOP=1", input=sql)

        def colunas(self, tabela, schema="analytics"):
            r = self.sql(
                "SELECT string_agg(column_name, ',' ORDER BY ordinal_position) "
                f"FROM information_schema.columns "
                f"WHERE table_schema='{schema}' AND table_name='{tabela}'"
            )
            assert r.returncode == 0, r.stderr
            linhas = [l.strip() for l in r.stdout.splitlines()]
            for l in linhas:
                if "," in l:
                    return l.split(",")
            return []

    yield Runner()

    _admin(f'DROP DATABASE IF EXISTS "{nome}" WITH (FORCE)')
