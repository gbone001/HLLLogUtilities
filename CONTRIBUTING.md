# Contributing Guide

> **Python version:** Please develop and run tests under Python 3.11 or 3.12. The Postgres driver (`asyncpg`) does not publish prebuilt wheels for 3.13+ yet, so attempting installs on newer runtimes will require platform-specific build tools and often fails in CI.

## Database & Dual-Write

Developers testing the PostgreSQL pipeline should enable dual-write locally:

```
[Database]
Mode=dual
Url=postgresql://postgres:postgres@localhost:6543/postgres
```

- `Mode=dual` keeps the legacy SQLite flow online while mirroring every write operation (sessions, credentials, HSS keys, logs) into PostgreSQL. This is the safest way to validate the new storage layer before cutting over.
- `Mode=sqlite` disables all Postgres work. `Mode=postgres` is reserved for the future full cutover.
- Environment overrides: `HLL_DB_URL` takes precedence over `Url`, and `HLL_STORAGE_MODE` wins over `Mode`.

### Backfilling existing data

Use the helper script to migrate historical data before enabling dual-write in staging/production:

```pwsh
python -m scripts.sqlite_to_postgres `
  --sqlite-path sessions.db `
  --postgres-dsn postgresql://postgres:postgres@localhost:6543/postgres `
  --batch-size 2000
```

Add `--dry-run` for a connectivity smoke test. The script copies credentials, HSS API keys, session metadata, and all session log tables (streamed in batches) while preserving the original IDs so dual-write can pick up seamlessly afterward.

### Operational checklist

1. Stop the bot (or schedule downtime).
2. Run `python -m scripts.sqlite_to_postgres --dry-run` to verify connectivity.
3. Re-run without `--dry-run` to perform the copy.
4. Switch `[Database].Mode` to `dual` and restart the bot.
5. Monitor the logs for `postgres write backlog` warnings; these indicate saturation of the async write queue.
