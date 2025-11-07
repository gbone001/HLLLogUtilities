# cogs/dmt_phase_switch.py
from __future__ import annotations
import asyncio, shutil, logging, os
from datetime import datetime, timezone
from discord.ext import commands, tasks

LOG = logging.getLogger("cogs.dmt_phase_switch")

PHASE_B_PATH = os.environ.get("DMT_GEOFENCE_PHASE_B","geofences/config.mid.yml")
ACTIVE_PATH  = os.environ.get("DMT_GEOFENCE_ACTIVE","geofences/config.yml")
CONTAINER    = os.environ.get("DMT_GEOFENCE_CONTAINER","hll-geofences")

class DmtPhaseSwitch(commands.Cog):
    \"\"\"Switch hll-geofences from Phase A (pre-combat hold) to Phase B (mid-only) at 1:15:00.\"\"\"
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.start_time: datetime | None = None
        self._tick.start()

    def cog_unload(self):
        self._tick.cancel()

    @tasks.loop(seconds=5)
    async def _tick(self):
        if not self.start_time:
            return
        delta = datetime.now(timezone.utc) - self.start_time
        if delta.total_seconds() >= 75*60:  # 1:15:00
            try:
                shutil.copyfile(PHASE_B_PATH, ACTIVE_PATH)
                # restart the geofence container to reload config
                proc = await asyncio.create_subprocess_exec("docker", "restart", CONTAINER)
                await proc.wait()
                LOG.info("DMT: switched geofences to PHASE B (mid-only)")
            finally:
                self._tick.cancel()

    @commands.Cog.listener()
    async def on_dmt_match_start(self):
        # Fire this event when your ruleset sees round start
        self.start_time = datetime.now(timezone.utc)

async def setup(bot: commands.Bot):
    await bot.add_cog(DmtPhaseSwitch(bot))
