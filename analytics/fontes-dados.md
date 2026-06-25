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
