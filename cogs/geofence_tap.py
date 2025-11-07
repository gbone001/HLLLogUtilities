# cogs/geofence_tap.py
from __future__ import annotations
import re, logging, os
from discord.ext import commands

LOG = logging.getLogger("cogs.geofence_tap")
GEONOTE = re.compile(r"GEOfence|geofence|outside the fence|return to zone|punished", re.I)

class GeofenceTap(commands.Cog):
    \"\"\"Mirror geofence warnings/punishments into the DMT Discord channel.\"\"\"
    def __init__(self, bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_hll_log(self, row: dict):
        msg = (row.get("message") or "") + " " + (row.get("weapon") or "")
        if GEONOTE.search(msg):
            await self._post(row)

    async def _post(self, row: dict):
        chan_id = int(self.bot.cfg.get('DMT_T6','DiscordChannelId', fallback="0"))
        ch = self.bot.get_channel(chan_id) if chan_id else None
        if not ch: 
            LOG.debug("No DMT_T6.DiscordChannelId set; skipping geofence alert")
            return
        await ch.send(f"🧭 Geofence action: `{row.get('player_name')}` — {row.get('message') or 'punished'}")

async def setup(bot): 
    await bot.add_cog(GeofenceTap(bot))
