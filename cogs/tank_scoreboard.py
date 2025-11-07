from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from io import StringIO

import discord
from discord import Interaction, app_commands
from discord.ext import commands, tasks

from cogs.sessions import autocomplete_sessions
from discord_utils import CustomException
from lib.converters import ExportFormats, ScoreboardMode
from lib.session import HLLCaptureSession, SESSIONS
from lib.tank_scoreboard import TankScoreConfig, build_tank_scoreboard


@dataclass
class TankScorePost:
    channel_id: int
    message_id: int
    final_sent: bool = False


class TankScoreboard(commands.Cog):
    """Live tank-score utilities."""

    TankScoreGroup = app_commands.Group(
        name="tank_score",
        description="Tank scoreboard utilities",
        default_permissions=discord.Permissions(manage_guild=True),
    )

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._reset_after: dict[int, datetime] = {}
        self._config = TankScoreConfig.from_env()
        self._default_enabled = False
        self._posts: dict[int, TankScorePost] = {}
        self._post_updater.start()

    def cog_unload(self):
        self._post_updater.cancel()

    def _get_session(self, session_id: int) -> HLLCaptureSession:
        try:
            return SESSIONS[session_id]
        except KeyError as exc:
            raise CustomException(
                "Unknown session",
                "That capture session is not active."
            ) from exc
    
    def _is_enabled(self, session: HLLCaptureSession) -> bool:
        return getattr(session, "tank_score_enabled", self._default_enabled)

    def _ensure_enabled(self, session: HLLCaptureSession):
        if not self._is_enabled(session):
            raise CustomException(
                "Tank scoreboard disabled",
                "Enable it first with `/tank_score enable`.",
            )

    @TankScoreGroup.command(name="view", description="View the tank scoreboard for a session.")
    @app_commands.describe(
        session="An active capture session",
        export_format="Choose how to render the scoreboard",
        ephemeral="If enabled, only you can see the response",
    )
    @app_commands.autocomplete(session=autocomplete_sessions)
    async def tank_score_view(
        self,
        interaction: Interaction,
        session: int,
        export_format: ExportFormats = ExportFormats.text,
        ephemeral: bool = True,
    ):
        session_obj = self._get_session(session)
        self._ensure_enabled(session_obj)
        from_time = self._reset_after.get(session)
        logs = session_obj.get_logs(from_=from_time)
        if not logs:
            raise CustomException(
                "No logs yet",
                "No matching logs were found for this session window.",
            )

        scoreboard = build_tank_scoreboard(logs, config=self._config)
        converter = export_format.value
        payload = converter.create_scoreboard(scoreboard, mode=ScoreboardMode.tank)

        if export_format == ExportFormats.text:
            content = f"Tank scoreboard for **{session_obj.name}**\n```{payload}```"
            await interaction.response.send_message(content=content, ephemeral=ephemeral)
        else:
            fp = StringIO(payload)
            filename = f"{session_obj.name}_tank.{converter.ext()}"
            file = discord.File(fp, filename=filename)
            await interaction.response.send_message(
                content=f"Tank scoreboard for **{session_obj.name}**",
                file=file,
                ephemeral=ephemeral,
            )

    @TankScoreGroup.command(name="reset", description="Reset the tank scoreboard window for a session.")
    @app_commands.describe(session="An active capture session")
    @app_commands.autocomplete(session=autocomplete_sessions)
    async def tank_score_reset(self, interaction: Interaction, session: int):
        session_obj = self._get_session(session)
        self._ensure_enabled(session_obj)
        self._reset_after[session] = datetime.now(tz=timezone.utc)
        await interaction.response.send_message(
            f"Tank scoreboard window reset for **{session_obj.name}**.",
            ephemeral=True,
        )

    @TankScoreGroup.command(name="enable", description="Enable or disable the tank scoreboard commands for this session.")
    @app_commands.describe(
        session="An active capture session",
        enabled="Whether tank scoreboard commands should be allowed",
    )
    @app_commands.autocomplete(session=autocomplete_sessions)
    async def tank_score_enable(self, interaction: Interaction, session: int, enabled: bool):
        session_obj = self._get_session(session)
        setattr(session_obj, "tank_score_enabled", enabled)
        if not enabled:
            self._reset_after.pop(session, None)
            await self._remove_post(session)
        state = "enabled" if enabled else "disabled"
        if enabled:
            post_channel = interaction.channel
            if not isinstance(post_channel, discord.TextChannel):
                raise CustomException(
                    "Unsupported channel",
                    "Enable tank scoreboard from a server text channel so the persistent post can be created.",
                )
            await self._ensure_post(session_obj, post_channel)
        await interaction.response.send_message(
            f"Tank scoreboard {state} for **{session_obj.name}**.",
            ephemeral=True,
        )

    async def _ensure_post(self, session: HLLCaptureSession, channel: discord.TextChannel):
        if session.id in self._posts:
            return
        content = await self._build_scoreboard_content(session)
        message = await channel.send(content)
        self._posts[session.id] = TankScorePost(channel_id=channel.id, message_id=message.id)

    async def _remove_post(self, session_id: int):
        post = self._posts.pop(session_id, None)
        if not post:
            return
        channel = self.bot.get_channel(post.channel_id)
        if not isinstance(channel, discord.TextChannel):
            return
        try:
            message = await channel.fetch_message(post.message_id)
        except discord.NotFound:
            return
        try:
            await message.delete()
        except discord.HTTPException:
            pass

    async def _build_scoreboard_content(self, session: HLLCaptureSession) -> str:
        logs = session.get_logs(from_=self._reset_after.get(session.id))
        if not logs:
            return f"Tank scoreboard for **{session.name}**\n```\nNo data yet...\n```"

        scoreboard = build_tank_scoreboard(logs, config=self._config)
        payload = ExportFormats.text.value.create_scoreboard(scoreboard, mode=ScoreboardMode.tank)
        final_tag = ""
        if any(log.event_type == "server_match_end" for log in logs):
            final_tag = "\n\n**Final score**"
        return f"Tank scoreboard for **{session.name}**{final_tag}\n```{payload}```"

    @tasks.loop(minutes=5)
    async def _post_updater(self):
        if not self._posts:
            return
        for session_id, post in list(self._posts.items()):
            session = SESSIONS.get(session_id)
            if not session or not self._is_enabled(session):
                continue
            if post.final_sent:
                continue
            try:
                content = await self._build_scoreboard_content(session)
            except Exception:
                continue

            channel = self.bot.get_channel(post.channel_id)
            if not isinstance(channel, discord.TextChannel):
                continue
            try:
                message = await channel.fetch_message(post.message_id)
            except discord.NotFound:
                del self._posts[session_id]
                continue
            try:
                await message.edit(content=content)
            except discord.HTTPException:
                continue

            if "**Final score**" in content and not post.final_sent:
                post.final_sent = True

    @_post_updater.before_loop
    async def _wait_ready(self):
        await self.bot.wait_until_ready()


async def setup(bot: commands.Bot):
    await bot.add_cog(TankScoreboard(bot))
