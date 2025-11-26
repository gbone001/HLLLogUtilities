# ADR 0001 – Migrate Session Storage to PostgreSQL
*Status*: Proposed  
*Date*: 2025-11-20  
*Deciders*: HLLLogUtilities maintainers

## Context
- Sessions are queued through `HLLCaptureSession.create_in_db`, which writes metadata into `sessions` (SQLite) and spawns a dedicated `session{id}` table. `HLLCaptureSession.push_to_db()` buffers log lines and bulk inserts into that table; later cleanup drops the table entirely.
- AutoSessions reuse the exact database path, so long‑running servers can accumulate hundreds of SQLite tables and migrations rely on brittle `DB_VERSION` checks inside `lib/storage.py`.
- Future requirements include dashboards, ad‑hoc queries, and cross-session analytics, which benefit from a normalized, centralized store and first-class migration tooling.

## Decision
We will consolidate storage into PostgreSQL with the following schema, powered by SQLAlchemy Core (asyncpg driver) and Alembic migrations.

### Schema Snapshot
| Table | Purpose | Key Columns |
| --- | --- | --- |
| `sessions` | Metadata for each capture session (manual or auto) | `id SERIAL PK`, `guild_id`, `name`, `start_time timestamptz`, `end_time timestamptz NULL`, `is_auto boolean`, `credentials_id FK`, `modifier_flags bigint`, `deleted_at timestamptz NULL` |
| `session_logs` (partitioned) | Fact table for every log line; partitions by month (`event_time`) or by `session_id` for easy pruning | `session_id FK`, `event_time timestamptz`, `event_type text`, players/weapon columns mirroring current `LogLine` model, `PRIMARY KEY (session_id, event_time, log_seq)` |
| `session_modifiers` | Historic modifier snapshots per session (optional normalized view) | `session_id FK`, `modifier_name`, `enabled boolean` |
| `credentials` | Existing table migrated wholesale with additional columns for autosession defaults, timestamps | `id PK`, `guild_id`, `name`, `address`, `port`, `password_enc`, `default_modifiers`, `autosession_enabled`, `created_at`, `updated_at` |
| `autosession_state` | Tracks throttling metadata currently held in memory/logs to support ops dashboards | `credentials_id FK`, `last_seen_playercount`, `last_error`, `failed_attempts`, `cooldown_until` |
| `hss_api_keys` | Same columns as today, moved under Alembic control |

**Partitioning strategy**: monthly `FOR VALUES FROM ('2025-11-01') TO ('2025-12-01')` partitions keyed on `event_time` keep retention fast (drop partition to delete old data) while still allowing per-session indexes (`session_id` btree, `(session_id,event_type)` covering indexes for exports). If we later need per-session isolation, we can combine with hash partitions by `session_id` within each monthly range.

**Indexes/views**:
- `session_logs` indexes on `(session_id, event_time)` and `(guild_id, session_id)` to satisfy export filters.
- Materialized views for popular aggregations (kills per player, timelines) feeding visualization tools.

### Access Layer & Tooling
- **Access layer**: SQLAlchemy Core + asyncpg engine. Core keeps query definitions close to today’s raw SQL yet yields dialect portability and compile-time schema definitions. asyncpg offers excellent async performance and COPY support.
- **Runtime versioning**: CI and self-hosted environments should stick to Python 3.11/3.12 for now so asyncpg wheels install cleanly; 3.13+ currently requires compiling the driver.
- **Migrations**: Adopt Alembic (`alembic/` directory) with async revision scripts. We will:
  1. Initialize Alembic with `alembic init alembic`.
  2. Capture the PostgreSQL schema above as the baseline migration (`alembic/versions/0001_initial.py`).
  3. Replace `DB_VERSION` logic in `lib/storage.py` with Alembic CLI documentation (`alembic upgrade head`).

### Configuration Additions
Add a `[Database]` section to `config.ini`:
```
[Database]
; Storage mode: sqlite (default), dual (mirror), or postgres (Postgres-only). Override via HLL_STORAGE_MODE env var.
Mode=sqlite
; SQLAlchemy-style URL, e.g. postgresql+asyncpg://user:pass@host:5432/hll_logs
Url=
; Minimum connections to keep in the async pool
PoolMinSize=5
; Maximum connections allowed in the pool
PoolMaxSize=20
; Optional per-statement timeout (seconds) applied via SET LOCAL
StatementTimeoutSeconds=5
```
Bots running without this section default to SQLite (compat mode) until we cut over; staging/production will require a filled URL.

### Alembic Baseline Instructions
1. Install tooling: `pip install alembic sqlalchemy[asyncio] asyncpg`.
2. `alembic init alembic` (committed). Update `alembic.ini` with the `sqlalchemy.url` placeholder referencing `[Database].Url` from `config.ini` (read via env var, e.g. `HLL_DB_URL`).
3. Create baseline revision:
   ```bash
   alembic revision -m "initial schema" --autogenerate
   alembic upgrade head
   ```
4. Document commands in README/CONTRIBUTING so contributors run `alembic upgrade head` before starting the bot.

## Consequences
- Pros: unified schema enables global analytics, easier clean-up via partition drops, industry-standard migrations, async connection pooling.
- Cons: requires operating a PostgreSQL cluster and managing Alembic workflows; contributors must install the new dependencies.

## Follow-up Tasks
1. Implement repository layer (`lib/storage/postgres.py`) mirroring current CRUD/mutation paths.
2. Wire runtime objects to the new pool and phase out SQLite codepaths.
3. Provide migration scripts to backfill existing sessions into PostgreSQL.
4. Update developer docs (README/CONTRIBUTING) with setup steps and Alembic commands.

## Migration & Backfill

### 5.1 Offline Migration Script
We will add `scripts/sqlite_to_postgres.py`, a one-shot utility that:
- Opens the legacy `sessions.db` via SQLAlchemy (synchronous engine) and streams rows in primary-key order.
- Connects to PostgreSQL using the asyncpg DSN from `[Database].Url` (script uses `asyncio.run` to share COPY helpers from `lib/storage/postgres.py`).
- Recreates sessions, modifiers, and credentials first (respecting existing IDs), then bulk loads each `session{id}` log table into its target partition using COPY with 1k-row batches to keep memory bounded.

Usage:
```pwsh
python -m scripts.sqlite_to_postgres \
  --sqlite-path sessions.db \
  --postgres-dsn postgresql://user:pass@host:5432/hll_logs \
  --start-session-id 0 --end-session-id 999999 \
  --batch-size 1000 --dry-run
```

Operational steps:
1. Quiesce the bot (or run during scheduled downtime) so SQLite no longer mutates.
2. Take backups (see §5.3) and record the latest `session_id` for verification.
3. Run the script with `--dry-run` to validate connectivity, then without to perform the copy.
4. Compare row counts per table (script reports checksum + counts) before switching to dual-write mode.

### 5.2 Dual-write & Feature Flag Rollout
Introduce a `Mode` flag inside `[Database]` (or env `HLL_STORAGE_MODE`) with values:
- `sqlite` (default legacy path)
- `dual` (write-through to both stores; reads still prefer SQLite)
- `postgres` (PostgreSQL single-writer, SQLite disabled)

**Operational guardrail**: before switching to `dual` or `postgres`, operators must set `HLL_DB_URL` (or `[Database].Url`) so the bot can build a PostgreSQL pool. Without a DSN the runtime will warn and fall back to `sqlite` to avoid partially-enabled dual writes.

Implementation plan:
1. Repository factory returns both `SQLiteStorage` and `PostgresStorage` instances.
2. When `Mode=dual`, session mutations (create/update/delete, log inserts) call both backends; discrepancies raise alerts but continue running to avoid downtime.
3. Reads during the dual phase remain on SQLite until parity checks confirm correctness, after which `Mode=postgres` flips the bot fully to PostgreSQL.
4. Feature flag can be toggled per environment via config reload or owner-only Discord command to ease staged rollout (dev → staging → prod).

### 5.3 Rollback & Backups
Always snapshot both databases before switching modes:
- SQLite: `sqlite3 sessions.db ".backup backups/sessions-$(Get-Date -Format yyyyMMddHHmm).db"`
- PostgreSQL: `pg_dump "$env:HLL_DB_URL" --file backups/pg_dump_$(Get-Date -Format yyyyMMddHHmm).sql`

Rollback procedure:
1. Set `[Database].Mode=sqlite` (or clear `HLL_DB_URL`) and restart the bot to return to the legacy path.
2. Restore SQLite from the `.backup` file if needed (replace `sessions.db`).
3. Drop/recreate the PostgreSQL database or `alembic downgrade base` before re-running the migration script.
4. Re-run the copy script once the issue is resolved, verify counts, then re-enable `Mode=dual`.
