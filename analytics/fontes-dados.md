# Fontes dos dados (Receita Federal — CNPJ)

Os dados abertos de CNPJ migraram (fim de jan/2026) para um portal Nextcloud da RFB.
O caminho estático antigo (`arquivos.receitafederal.gov.br/dados/cnpj/.../AAAA-MM/`)
foi descontinuado (404). Hoje há **duas famílias de arquivos, em shares diferentes**.

## 1. Dados principais do CNPJ (37 zips/mês)

Empresas, Estabelecimentos, Sócios, Simples + lookups (Cnaes, Motivos, Municipios,
Naturezas, Paises, Qualificacoes). ~7,6 GB/mês.

- **Oficial (Nextcloud RFB):** token `YggdBLfdninEJX9`
  - Listar meses: `curl -sk -X PROPFIND -u "YggdBLfdninEJX9:" -H "Depth: 1" https://arquivos.receitafederal.gov.br/public.php/webdav/`
  - Baixar: `curl -sk -u "YggdBLfdninEJX9:" -C - -o Empresas0.zip https://arquivos.receitafederal.gov.br/public.php/webdav/AAAA-MM/Empresas0.zip`
- **Espelho (CDN Cloudflare, mais rápido):** Casa dos Dados
  - `https://dados-abertos-rf-cnpj.casadosdados.com.br/arquivos/AAAA-MM-DD/Empresas0.zip`

Carregados por `analytics/load.sh` (delimitador `;`, encoding LATIN9).

## 2. Regime tributário (`entidades-*.zip`) — **share SEPARADO**

NÃO ficam junto dos 37 zips mensais nem no espelho. São distribuídos à parte, num
share Nextcloud próprio: **token `MPPfFit7g7zdA8C`**.

| Arquivo | Conteúdo | Tamanho aprox. |
|---|---|---|
| `entidades-lucro-real.zip` | `Lucro Real.csv` | ~11 MB |
| `entidades-lucro-presumido.zip` | vários `Lucro Presumido AAAA.csv` (por ano) | ~36 MB |
| `entidades-lucro-arbitrado.zip` | `Lucro Arbitrado.csv` | ~65 KB |
| `entidades-imunes-e-isentas.zip` | `Imunes e Isentas.csv` | ~11 MB |
| `entidades-regime-tributario-dicionarios/` | dicionário/layout | — |
| `renuncia-irpj-csll-ecf.csv` | renúncia IRPJ/CSLL via ECF (bônus) | — |

Baixar:
```bash
BASE=https://arquivos.receitafederal.gov.br/public.php/webdav
for f in entidades-imunes-e-isentas entidades-lucro-arbitrado \
         entidades-lucro-presumido entidades-lucro-real; do
  curl -sk -u "MPPfFit7g7zdA8C:" -o "$f.zip" "$BASE/$f.zip"
done
```
Listar: `curl -sk -X PROPFIND -u "MPPfFit7g7zdA8C:" -H "Depth: 1" $BASE/`

**Reencontrar o link se o token mudar:** dados.gov.br → buscar "CNPJ" → conjunto CNPJ →
aba **Recursos** → **Regime Tributário**. (Caminho confirmado pelo mantenedor do
`minha-receita` na issue #4 do Codeberg.)

### Formato dos CSVs de regime

Cabeçalho `ano,cnpj,cnpj_da_scp,forma_de_tributacao,quantidade_de_escrituracoes`,
**delimitador vírgula**, encoding ASCII/UTF-8. O `cnpj` vem **completo e formatado**
(`00.000.000/0001-91`) e a **ordem varia** (não é só matriz 0001). `cnpj_da_scp = 0`
significa "sem SCP".

> ℹ️ A versão atual (arquivos de 2026-01-15) é uniforme: todos com cabeçalho e vírgula.
> Versões antigas tinham `Lucro Presumido`/`Imunes` com `;` e sem cabeçalho — por isso
> `load.sh` filtra qualquer linha de cabeçalho com `grep -vi`.

Carregados pelo `analytics/load.sh`: na carga completa (função `copy_regime`) ou,
incrementalmente sem tocar nas demais tabelas, com `REGIME_ONLY=1 bash
analytics/load.sh`. O transform/índices ficam em `analytics/regime_transform.sql`.

### Tabela de destino

`analytics.regime_tributario` — grão = (cnpj completo, ano, forma). Como a ordem do
CNPJ varia e há SCP, **não** se chaveia por `cnpj_basico`; usa-se PK surrogate (`id`)
e índices em `cnpj_basico`, `cnpj` e `(ano, forma_de_tributacao)`. Duplicatas exatas
(mesma linha repetida entre arquivos) são removidas com `SELECT DISTINCT`.

Volume (carga 2026-01): **~10,6 milhões de linhas**, ~2,83 mi de empresas distintas,
anos 2016–2024.

## Fontes parciais alternativas (só imunes/isentas)

Caso o share de regime saia do ar, o recorte imunes/isentas existe noutro formato
(renúncia fiscal, não o layout `entidades-*`):

- dados.gov.br: conjunto "Entidades Imunes e Isentas de Tributos Federais".
- Portal da Transparência: endpoint `GET /api-de-dados/renuncias-fiscais-empresas-imunes-isentas`
  (header `chave-api-dados: <token>`, paginado via `?pagina=N`).

> ⚠️ A partir de jul/2026 o CNPJ passa a aceitar letras (alfanumérico) — os tipos
> `char(n)` no schema já preveem isso, mas a limpeza `regexp_replace(cnpj,'\D','')`
> do regime precisará ser revista quando os dados alfanuméricos chegarem.

## 3. De-para de municípios → código IBGE

O `Municipios.csv` da Receita traz **só** `codigo;descricao`, e esse código é o do
**SIAFI** (4 dígitos), não o do IBGE (7 dígitos). Como o código IBGE é a chave que
usamos para cruzar com qualquer outra base pública, `analytics.dim_municipio` guarda
os dois — `codigo` (Receita/SIAFI) e `codigo_ibge` —, além da `uf`, que também não
vem no arquivo da Receita.

Duas fontes externas, baixadas pelo `load.sh` e **cacheadas em `$IBGE_CACHE_DIR`**
(default: o próprio `DATA_DIR`):

| Arquivo no cache | Fonte | Papel |
|---|---|---|
| `tabmun.csv` | TABMUN do Tesouro Nacional, via CKAN (`package_show` do dataset `abb968cb-…`) | **Primária**: de-para SIAFI → IBGE + UF. `codigo_siafi;cnpj;nome;uf;codigo_ibge`, `;`, sem cabeçalho, campos com padding. |
| `ibge_municipios.json` | API de localidades do IBGE (`servicodados.ibge.gov.br/api/v1/localidades/municipios`) | **Fallback** por nome normalizado, para municípios novos ainda ausentes do TABMUN. |

A URL do CSV do TABMUN não é fixa (o Tesouro republica o recurso), por isso ela é
descoberta no CKAN a cada download; há uma URL de último recurso embutida no
`load.sh` caso o CKAN não responda.

### Como o de-para é aplicado (`analytics/ibge_transform.sql`)

1. `UPDATE` a partir do TABMUN casando `dim_municipio.codigo = codigo_siafi`
   (descarta as 19 linhas "DEMAIS MUNICIPIOS", que vêm com `codigo_ibge = 0000000`).
2. Para o que sobrou sem código, casa por **nome normalizado** (maiúsculas, sem
   acento, só `[A-Z0-9]`) contra a lista do IBGE, exigindo **match único no país** —
   homônimo fica `NULL` de propósito, melhor um furo visível que um município errado.
3. `uf` de quem veio pelo fallback sai dos **2 primeiros dígitos do código IBGE**
   (11=RO … 53=DF).
4. `EXTERIOR` (SIAFI 9707) não é município: fica sem IBGE, com `uf = 'EX'` (mesma
   sigla da partição `analytics.estabelecimento_ex`).
5. Validações que **abortam** o passo se algo furar: total ≥ 5.570, ninguém sem
   `codigo_ibge` além do EXTERIOR, ninguém sem `uf`, nenhum código IBGE repetido, e
   as âncoras São Paulo (7107 → 3550308) e Rio de Janeiro (6001 → 3304557).

Situação em set/2026: dos 5.572 municípios do arquivo da Receita, **5.571 recebem
código IBGE**; o único sem é o `EXTERIOR`. O TABMUN cobre 5.570 — Boa Esperança do
Norte/MT (SIAFI 1182 → IBGE 5101837) só é resolvido pelo fallback da API do IBGE.

### Rodando isoladamente

```bash
# preenche/atualiza só o de-para, numa base já carregada (não toca no resto)
IBGE_ONLY=1 DB=cnpj_full bash analytics/load.sh

# força rebaixar as fontes ignorando o cache
IBGE_ONLY=1 IBGE_REFRESH=1 DB=cnpj_full bash analytics/load.sh
```

Na carga completa o passo roda sozinho depois do `03_transform.sql` (a
`dim_municipio` é truncada lá, então o de-para precisa vir depois). Falha de rede
ou validação reprovada **não derruba** a carga: imprime o aviso e segue, e o passo
pode ser refeito com o `IBGE_ONLY=1` acima.
