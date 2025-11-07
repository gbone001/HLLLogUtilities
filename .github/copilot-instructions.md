# HLLLogUtilities Development Guide

## Architecture Overview

**HLLLogUtilities** is a Discord bot that captures and exports detailed logs from Hell Let Loose game servers via RCON, featuring an asynchronous session-based architecture with optional game rule enforcement ("modifiers").

### Core Components

- **Sessions** (`lib/session.py`): Recording windows that poll RCON at ~5s intervals. Sessions manage multiple RCON worker connections, event listeners, and modifiers lifecycle. Sessions are stored in SQLite (`sessions.db`) with associated credentials.

- **RCON Layer** (`lib/rcon/`): Wraps `hllrcon` library. `HLLRcon.create_snapshot()` polls game state (players, squads, teams, logs) and computes diffs to emit events (kills, role changes, team switches, squad joins, etc.) via `Snapshot.compare_older()`.

- **Event System** (`lib/events.py`): Decorator-based listeners (`@on_player_kill`, `@event_listener`) with conditions and cooldowns. Events flow: RCON → Snapshot → EventModel → Listeners in Modifiers/Session.

- **Modifiers** (`lib/modifiers/`): Event-driven game rule enforcers. Each modifier inherits `Modifier` base class and uses `@event_listener` decorators. Examples: `NoPantherModifier` kills crew when Panther is used; `OneArtyModifier` enforces single artillery gunner per team with complex state tracking.

- **Storage** (`lib/storage.py`): SQLite database for sessions, credentials, logs. `LogLine._get_create_query()` dynamically generates table schemas. Sessions auto-delete after 14 days (`DELETE_SESSION_AFTER`).

- **Discord Cogs** (`cogs/`): Commands organized as discord.py extensions. `sessions.py` handles session CRUD; `exports.py` generates logs in txt/csv/json formats and submits to HeLO System (HSS); `credentials.py` manages RCON server credentials.

- **AutoSession** (`lib/autosession.py`): Background task that monitors server player count and auto-creates sessions when ≥70 players online, stops at ≤30 players or 5hr limit (configurable in `config.ini`).

## Key Patterns & Conventions

### Session Lifecycle
1. Created via `/session new` Discord command → stored in DB
2. Scheduled activation via `schedule_coro()` at start_time
3. On activate: spawn RCON workers, initialize modifiers, start iteration loop
4. Each iteration: `create_snapshot()` → emit events → process via modifiers → buffer logs
5. Logs bulk-inserted when buffer reaches `NUM_LOGS_REQUIRED_FOR_INSERT` (default 1000)
6. On deactivate: flush logs, close RCON connections, cleanup

### Event Listener Pattern
Modifiers define event handlers using decorators with built-in filtering:
```python
@on_player_kill()  # Convenience decorator for player_kill events
@add_condition(lambda mod, event: VEHICLES.get(event.weapon) == "Panther")
@add_cooldown("player_id", duration=10)  # Prevent duplicate triggers
async def punish_on_panther_usage(self, event: PlayerKillEvent):
    # Implementation
```

### RCON Command Execution
Always use `self.get_rcon().client.method()` in modifiers. Common commands: `kill_player()`, `message_player()`, `kick_player()`. Batch operations with `asyncio.gather()` for performance.

### Configuration Management
`config.ini` drives behavior (max durations, intervals, thresholds). Access via `get_config().getint('Section', 'Key')`. Critical sections: `[Session]`, `[AutoSession]`, modifier-specific sections.

### Discord Integration
- All commands require `Manage Server` permission (set in `sync_commands()`)
- Use `View` subclasses with `CallableButton`/`CallableSelect` for interactive UIs (see `SessionCreateView`)
- Modal forms for credential input (`RCONCredentialsModal`)
- Autocomplete functions for better UX (`autocomplete_sessions`, `autocomplete_credentials`)

## Development Workflows

### Adding a New Modifier
1. Create `lib/modifiers/your_modifier.py` inheriting `Modifier`
2. Define `Config` inner class with `id`, `name`, `emoji`, `description`
3. Add event listeners with appropriate decorators
4. Add to `ALL_MODIFIERS` tuple in `lib/modifiers/__init__.py`
5. Add flag bit to `ModifierFlags` class
6. Update `config.ini` if modifier needs custom settings

### Testing Locally
```bash
# Native execution
pip install -r requirements.txt
python bot.py  # Requires config.ini with valid Discord token

# Docker execution
sqlite3 sessions.db "VACUUM;"
docker-compose up -d
docker-compose logs -f  # View logs
```

### Database Migrations
Use `update_table_columns()` or `rename_table_columns()` in `lib/storage.py` for schema changes. Increment `DB_VERSION` constant. Migrations run automatically on startup.

## Critical Files

- `bot.py` - Discord bot entry point, loads cogs via `load_all_cogs()`
- `lib/session.py` - Session state machine and RCON iteration loop
- `lib/rcon/rcon.py` - Game state polling and event emission
- `lib/rcon/models.py` - Pydantic models for game entities (Player, Team, Squad, etc.)
- `lib/modifiers/base.py` - Modifier framework with event listener discovery
- `cogs/sessions.py` - Session Discord commands with 180s timeout Views
- `lib/hss/api.py` - Integration with HeLO System (Hell Let Loose stats platform)

## Common Gotchas

- **Player Name Incompatibility**: Names with space/special char at position 20 break RCON. Set `KickIncompatibleNames=1` or use `SteamApiKey` for detection.
- **Session Auto-Deletion**: Sessions delete 14 days after `end_time`, not creation. Export logs before expiry.
- **RCON Connection Pool**: `NUM_RCON_WORKERS` (default 4) balances speed vs server load. More workers = faster iterations but more connections.
- **Modifier State**: Modifiers reset on `ActivationEvent` and `server_match_start`. Ensure initialization in both event handlers.
- **Log Buffer Flushing**: Large sessions (>1M logs) may cause memory issues if `NUM_LOGS_REQUIRED_FOR_INSERT` is too high.

## External Integrations

- **HeLO System (HSS)**: Optional stats submission. Requires API key via `/apikeys` command. Submits CSV exports for competitive match tracking.
- **Steam API**: Optional. Enables detection of problematic player names. Get key at https://steamcommunity.com/dev/apikey
