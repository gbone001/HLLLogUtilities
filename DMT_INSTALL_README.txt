DMT BUNDLE (HLL Log Utilities + hll-geofences)

FILES
-----
cogs/rules_dmt_t6.py            -> DMT rules watcher + Discord alerts + interim scoring
cogs/ruleset_dmt_t6_core.py     -> Core evaluator and scorecard
cogs/dmt_phase_switch.py        -> Switch geofences from hold to mid-only at 1:15:00
cogs/geofence_tap.py            -> Mirror geofence punish/warn events into Discord
docker-compose.override.yml     -> Adds hll-geofences sidecar
geofences/config.yml            -> Phase A fences (pre-combat hold)
geofences/config.mid.yml        -> Phase B fences (mid-only)


INSTALL
-------
1) Unzip at the ROOT of your HLL Log Utilities project.
2) Add or update config.ini with the snippet in CONFIG_SNIPPET.ini (DiscordChannelId & PrecombatHold).
3) Ensure your bot loads cogs/* automatically, or add to your loader.
4) If using Docker Compose, `docker compose up -d` will start the geofence sidecar.
5) Make sure RCON_ADDRESS and RCON_V2_PASSWORD are set in your env.

RUNTIME
-------
- On round start, emit `on_dmt_match_start` (or adjust logic) so dmt_phase_switch flips to Phase B at 1:15:00.
- Violations appear in the Discord channel configured.
- Geofence warnings/punishments also appear in that channel.

NOTE
----
Polygons in geofences configs are placeholders. Replace with correct coordinates per map/layer.
