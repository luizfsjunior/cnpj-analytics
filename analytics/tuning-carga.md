# Tuning de carga (bulk load) — cnpj-analytics

Receita de configuração pra acelerar a carga completa (`DB=cnpj_full bash analytics/load.sh`,
~71,8M estabelecimentos). O Postgres roda em container Docker sobre WSL2 no Windows.

> ⚠️ **Regra de ouro:** `shared_buffers` e a RAM do WSL2 só mudam com **restart**, e restart
> **mata uma carga em andamento** (`docker compose up --build` recria o postgres — ver memória
> do projeto). Faça os ajustes de restart **antes** de iniciar a carga. Os ajustes via
> `reload` (SIGHUP) podem ser feitos a quente, sem derrubar a carga.

## Diagnóstico padrão (antes de mexer)

```bash
# config atual
docker exec cnpj-analytics-postgres-1 psql -U cnpj -d cnpj_full -c "
SELECT name, setting, unit FROM pg_settings
WHERE name IN ('shared_buffers','maintenance_work_mem','work_mem',
               'max_parallel_maintenance_workers','max_wal_size','synchronous_commit');"

# CPU/RAM do container (mostra o teto que o WSL2 deu)
docker stats cnpj-analytics-postgres-1 --no-stream

# o que está rodando agora (state, wait_event)
docker exec cnpj-analytics-postgres-1 psql -U cnpj -d cnpj_full -c "
SELECT pid, state, wait_event_type, wait_event, now()-query_start AS dur
FROM pg_stat_activity WHERE datname='cnpj_full' AND state<>'idle' AND pid<>pg_backend_pid();"
```

Defaults do Postgres são minúsculos: `shared_buffers=128MB`, `maintenance_work_mem=64MB`,
`work_mem=4MB` — ridículos numa máquina de 32 GB.

## Parte A — Ajustes que precisam de RESTART (fazer ANTES da carga)

Máquina host tem **32 GB**. Por padrão o WSL2 só entrega metade (~15,5 GB) ao container.

### 1. Liberar RAM pro WSL2

Arquivo `C:\Users\luiz.junior\.wslconfig`:

```ini
[wsl2]
memory=24GB
```

Aplicar: `wsl --shutdown` (encerra o WSL; o Docker Desktop sobe de novo).

### 2. `shared_buffers` — o grande knob (corta os `DataFileRead`)

```bash
docker exec cnpj-analytics-postgres-1 psql -U cnpj -d cnpj_full \
  -c "ALTER SYSTEM SET shared_buffers = '8GB';"
# precisa reiniciar o container do postgres pra aplicar:
docker compose restart postgres        # SÓ quando NÃO há carga rodando
```

Regra geral: `shared_buffers` ≈ 25% da RAM disponível ao container.

## Parte B — Ajustes via RELOAD (podem ser feitos a QUENTE, sem restart)

Todos abaixo aplicam com `pg_reload_conf()` (SIGHUP). Não derrubam a carga.

> `ALTER SYSTEM` **não roda dentro de transação** → mandar cada um em `-c` separado.

```bash
docker exec cnpj-analytics-postgres-1 psql -U cnpj -d cnpj_full \
  -c "ALTER SYSTEM SET maintenance_work_mem = '2GB';" \
  -c "ALTER SYSTEM SET max_parallel_maintenance_workers = 4;" \
  -c "ALTER SYSTEM SET max_wal_size = '8GB';" \
  -c "ALTER SYSTEM SET work_mem = '256MB';" \
  -c "ALTER SYSTEM SET synchronous_commit = 'off';" \
  -c "SELECT pg_reload_conf();"
```

| Parâmetro | Default | Carga | Por quê |
|---|---|---|---|
| `maintenance_work_mem` | 64 MB | **2 GB** | acelera `CREATE INDEX` (fase [5/5]), inclusive os GIN trgm |
| `max_parallel_maintenance_workers` | 2 | **4** | constrói índice em paralelo |
| `max_wal_size` | 1 GB | **8 GB** | menos checkpoints durante bulk → menos full-page writes |
| `work_mem` | 4 MB | **256 MB** | sorts/hashes dos `INSERT ... SELECT` |
| `synchronous_commit` | on | **off** | commit não espera o `fsync` do WAL (ver risco abaixo) |

> ⚠️ **Atenção ao `maintenance_work_mem` com paralelismo:** o pico é
> `(nº workers + 1) × maintenance_work_mem`. Com 4 workers, 2 GB → ~10 GB de pico.
> Não exagere o valor a ponto de estourar a RAM do container.

> ⚠️ **`synchronous_commit = off`:** numa crash você pode perder os últimos ~poucos
> commits. Pra **carga** é aceitável (o `load.sh` é re-executável do zero). **Reverter**
> ao terminar: `ALTER SYSTEM RESET synchronous_commit; SELECT pg_reload_conf();`

## Pós-carga — reverter o que era só pra carga

```bash
docker exec cnpj-analytics-postgres-1 psql -U cnpj -d cnpj_full \
  -c "ALTER SYSTEM RESET synchronous_commit;" \
  -c "ALTER SYSTEM RESET work_mem;" \
  -c "SELECT pg_reload_conf();"
```

`shared_buffers`, `maintenance_work_mem` e `max_wal_size` podem ficar — ajudam também as
queries e o `REFRESH MATERIALIZED VIEW` do dia a dia. Conferir com `ALTER SYSTEM RESET ...`
caso queira voltar ao default.

## Por que "disco a 10%" no Gerenciador de Tarefas é normal na carga

`wait_event = DataFileRead` = esperando o disco responder a leituras **aleatórias** de
páginas. O gargalo é **latência**, não banda — move pouco dado por vez, então o "% ativo"
fica baixo mesmo o processo estando 100% "esperando disco". Subir `shared_buffers` (mais
página em cache) é o que reduz isso. Além disso, o Gerenciador do Windows frequentemente
subnotifica o I/O que acontece dentro da VM do WSL2.
