# HLLLogUtilities – Copilot Instructions
## Purpose & Layout
- Discord bot (`bot.py`) records Hell Let Loose sessions by loading every cog in `cogs/` and exposing slash commands guarded by Manage Server perms.
- Domain logic lives in `lib/` (sessions, autosessions, modifiers, RCON, storage) while shared helpers (config, ttl cache, embed helpers) sit in `utils.py` and `discord_utils.py`.
- Expect long-running async tasks; avoid blocking the event loop and prefer the provided schedulers/loops.
## Session Lifecycle
- Slash commands in `cogs/sessions.py` call `lib.session.HLLCaptureSession` which schedules `activate`/`deactivate` via `utils.schedule_coro` and streams logs through `lib.rcon.HLLRcon`.
- Each session writes to a dedicated SQLite table (`session{id}`) created through `lib.storage.LogLine._get_create_query`; cleanup uses `HLLCaptureSession.delete()` + `delete_logs`.
- Session modifiers are bit-flags (`lib.modifiers.ModifierFlags`) that instantiate modifier classes per session; reuse `SessionModifierView` to edit flags so defaults persist on `Credentials`.
## AutoSession & Credentials
- Auto sessions are managed per credential (`lib.autosession.AutoSessionManager`) and start once player thresholds from `config.ini` are met; they share the same `HLLCaptureSession` path but with open-ended end times.
- `lib.credentials.Credentials` caches guild-specific RCON details and optional defaults for modifiers + autosession toggles; persist changes with `save()` to keep SQLite in sync.
- AutoSession loops aggressively back off after repeated RCON failures; when adding new checks, respect `NUM_ITERATIONS_UNTIL_COOLDOWN_EXPIRE` and avoid extra network calls inside the loop.
## External Integrations
- RCON access uses `hllrcon` (see `lib/rcon/rcon.py`); every snapshot combines realtime `get_admin_log` parsing with `get_players/get_server_session` to build `Snapshot` objects and `EventModel`s.
- Hell Let Loose Skill System uploads flow through `lib/hss/api.py`; hook into `Bot.hss` (`bot.py`) rather than constructing new clients so retries + base URL config stay centralized.
- When implementing new exports, prefer the existing `discord.File`/`StringIO` streaming patterns in `cogs/exports.py` to keep memory bounded.
## Storage & Config
- SQLite (`sessions.db`) is auto-migrated in `lib/storage.py`; bump `DB_VERSION` + add migration steps when schema changes, never edit tables manually elsewhere.
- Global settings read via `utils.get_config()`; avoid re-reading files and instead extend `config.ini` sections (Bot, Session, AutoSession, HSS, OneManArty) so env-specific overrides remain simple.
- Logs persist under `logs/` using `utils.get_logger`/`get_autosession_logger`; reuse those helpers for new long-lived tasks to keep format consistency.
## Discord UX Conventions
- Buttons/modals rely on `discord_utils.CallableButton`, `View`, and `only_once` wrappers; always delete ephemeral prompts after use to prevent `ExpiredButtonError`.
- Embeds should match existing tone (see `SessionCreateView.get_embed`) and include Discord timestamps (`<t:...>`) plus modifier links from `cogs.credentials.MODIFIERS_URL`.
- Keep user-visible strings localized within cogs; background modules should log via the session logger instead of sending Discord messages directly.
## Developer Workflows
- Local run: `pip install -r requirements.txt`, fill `config.ini`, create `sessions.db` (auto on first run), then `python bot.py`; use Python ≥3.11 for `discord.py 2.x`.
- Connectivity smoke test: run `python test_connection.py` to verify RCON reachability without starting the full bot.
- Docker path: ensure `sessions.db` exists, then `docker-compose up -d`; bot restarts automatically and replays cogs via `Bot.setup_hook`.
- After code changes, restart the bot or use the owner-only `!reload <cog>` commands defined in `bot.py` to hot-swap cogs without dropping the process.
