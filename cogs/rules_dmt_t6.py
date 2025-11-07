# cogs/rules_dmt_t6.py
from __future__ import annotations
import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

import discord
from discord.ext import commands, tasks

# Adjust these imports to your project's layout if different
try:
    from utils import get_config
except Exception:
    # minimal fallback
    import configparser, os
    def get_config():
        cfg = configparser.ConfigParser()
        cfg.read(os.environ.get("HLL_CONFIG", "config.ini"))
        return cfg

from .ruleset_dmt_t6_core import (
    DmtT6Config, DmtT6Evaluator, Violation, DmtScorecard, IngestContext
)

LOG = logging.getLogger("cogs.rules_dmt_t6")

class DmtT6Cog(commands.Cog):
    \"\"\"Live DMT T6 ruleset enforcement + scoring helper.\"\"\"

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        cfg = get_config()
        self.cfg = DmtT6Config(
            discord_channel_id=cfg.getint('DMT_T6', 'DiscordChannelId', fallback=0),
            precombat_hold_time=cfg.get('DMT_T6', 'PrecombatHold', fallback="1:15:00"),
            require_mode_warfare=True,
            suspect_heavy_weapons=[
                "88mm","kwk 8.8","tiger 88","panther 75","is-2 122","jumbo 75 smoke"
            ],
            technician_role_names=["Technician","technician","Tech"],
            fourth_cap_illegal=True,
            commander_max_pstrikes=2,
            allowed_commander_abilities={
                "recon_plane","encourage","resource_swap","spawn_vehicle","supply_drop_hq_only","precision_strike"
            },
            playable_maps_whitelist=[]
        )
        self.eval = DmtT6Evaluator(self.cfg)
        self._last_seen_iso: Optional[str] = None
        self._scorecard = DmtScorecard()
        # start polling loop
        self._poll_logs.start()

    def cog_unload(self):
        self._poll_logs.cancel()

    @tasks.loop(seconds=2.5)
    async def _poll_logs(self):
        \"\"\"Pull new log rows from your existing logs table/source and evaluate.

        Expected schema (adapt if your project differs):
        columns: event_time, event_type, player_name, player_team, player_role,
                 player2_name, team_name, weapon, message, old, new
        \"\"\"
        try:
            rows = await self._fetch_recent_rows()
            if not rows:
                return

            ctx = IngestContext(now=datetime.now(tz=timezone.utc))
            for r in rows:
                event = {
                    "event_time": r.get("event_time"),
                    "event_type": r.get("event_type"),
                    "player_name": r.get("player_name"),
                    "player_team": r.get("player_team"),
                    "player_role": r.get("player_role"),
                    "player2_name": r.get("player2_name"),
                    "team_name": r.get("team_name"),
                    "weapon": r.get("weapon"),
                    "message": r.get("message"),
                    "old": r.get("old"),
                    "new": r.get("new"),
                }
                vios = self.eval.ingest_event(event, ctx)
                for vio in vios:
                    await self._announce_violation(vio)
                self._scorecard.ingest_event(event, ctx)

            # mark last seen
            self._last_seen_iso = rows[-1]["event_time"]
        except Exception as e:
            LOG.exception("DMT poll failed: %s", e)

    @_poll_logs.before_loop
    async def _wait_ready(self):
        await self.bot.wait_until_ready()

    async def _fetch_recent_rows(self) -> List[Dict[str, Any]]:
        \"\"\"OVERRIDE THIS to hook into your project's log stream.\n
        For portability we keep a simple JSONL tailer option here:
        If you drop JSON lines into runtime/log_tail.jsonl, we'll read them.
        Replace this with your DB query in production.
        \"\"\"
        path = os.environ.get("HLL_LOG_JSONL", "runtime/log_tail.jsonl")
        if not os.path.exists(path):
            return []
        out: List[Dict[str, Any]] = []
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line=line.strip()
                if not line: continue
                try:
                    row = json.loads(line)
                except Exception:
                    continue
                # only new rows
                if self._last_seen_iso and row.get("event_time") <= self._last_seen_iso:
                    continue
                out.append(row)
        out.sort(key=lambda x: x.get("event_time",""))
        return out

    async def _announce_violation(self, vio: Violation):
        chan_id = int(self.cfg.discord_channel_id or 0)
        if not chan_id:
            LOG.warning("DMT_T6.DiscordChannelId not set; violation=%s", vio)
            return
        channel = self.bot.get_channel(chan_id)
        if not isinstance(channel, (discord.TextChannel, discord.Thread)):
            LOG.warning("DMT channel not found: %s", chan_id)
            return
        embed = discord.Embed(
            title=f"⚠️ DMT T6 Violation: {vio.code}",
            description=vio.human,
            timestamp=datetime.now(tz=timezone.utc)
        )
        if vio.evidence:
            embed.add_field(name="Evidence", value=vio.evidence, inline=False)
        if vio.severity:
            embed.add_field(name="Severity", value=vio.severity, inline=True)
        await channel.send(embed=embed)

    @commands.hybrid_command(description="Show current DMT T6 interim totals (unofficial).")
    @commands.has_permissions(manage_guild=True)
    async def dmt_score(self, ctx: commands.Context):
        data = self._scorecard.render_unofficial()
        await ctx.reply(f"```\n{data}\n```")

async def setup(bot: commands.Bot):
    await bot.add_cog(DmtT6Cog(bot))
