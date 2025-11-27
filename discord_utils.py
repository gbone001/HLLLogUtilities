import discord
from discord import ui, app_commands, Interaction, ButtonStyle, Emoji, PartialEmoji, SelectOption
from discord.ext import commands
from discord.utils import escape_markdown as esc_md, MISSING

from datetime import datetime, timedelta
import traceback

from lib.exceptions import HSSConnectionError
from utils import ttl_cache

from typing import Callable, Optional, Union, List, Any, Sequence


class EmbedBuilder:
    """Fluent helper for constructing Discord embeds consistently."""

    def __init__(
        self,
        *,
        title: str | None = None,
        description: str | None = None,
        color: discord.Colour | discord.Color | None = None,
    ):
        self._embed = discord.Embed(title=title, description=description, color=color)

    def set_title(self, title: str):
        self._embed.title = title
        return self

    def set_description(self, description: str):
        self._embed.description = description
        return self

    def set_color(self, color: discord.Colour | discord.Color):
        self._embed.color = color
        return self

    def set_author(self, *, name: str, icon_url: str | None = None):
        self._embed.set_author(name=name, icon_url=icon_url)
        return self

    def set_footer(self, *, text: str | None = None, icon_url: str | None = None):
        self._embed.set_footer(text=text, icon_url=icon_url)
        return self

    def set_thumbnail(self, url: str):
        self._embed.set_thumbnail(url=url)
        return self

    def set_timestamp(self, timestamp: datetime):
        self._embed.timestamp = timestamp
        return self

    def add_field(self, *, name: str, value: str, inline: bool = False):
        self._embed.add_field(name=name, value=value, inline=inline)
        return self

    def add_inline_field(self, *, name: str, value: str):
        return self.add_field(name=name, value=value, inline=True)

    def add_modifiers_field(self, title: str, modifiers: Sequence[str]):
        if modifiers:
            self._embed.add_field(name=title, value="\n".join(modifiers), inline=False)
        return self

    def build(self) -> discord.Embed:
        return self._embed

class CallableButton(ui.Button):
    def __init__(self,
        callback: Callable,
        *args: Any,
        style: ButtonStyle = ButtonStyle.secondary,
        label: Optional[str] = None,
        disabled: bool = False,
        custom_id: Optional[str] = None,
        url: Optional[str] = None,
        emoji: Optional[Union[str, Emoji, PartialEmoji]] = None,
        row: Optional[int] = None,
        **kwargs: Any
    ):
        super().__init__(
            style=style,
            label=label,
            disabled=disabled,
            custom_id=custom_id,
            url=url,
            emoji=emoji,
            row=row
        )
        self._callback = callback
        self._args = args
        self._kwargs = kwargs

    async def callback(self, interaction: Interaction):
        await self._callback(interaction, *self._args, **self._kwargs)

class CallableSelect(ui.Select):
    def __init__(self,
        callback: Callable,
        *args,
        custom_id: str = MISSING,
        placeholder: Optional[str] = None,
        min_values: int = 1,
        max_values: int = 1,
        options: List[SelectOption] = MISSING,
        disabled: bool = False,
        row: Optional[int] = None,
        **kwargs
    ):
        super().__init__(
            custom_id=custom_id,
            placeholder=placeholder,
            min_values=min_values,
            max_values=max_values,
            options=options,
            disabled=disabled,
            row=row
        )
        self._callback = callback
        self._args = args
        self._kwargs = kwargs

    async def callback(self, interaction: Interaction):
        await self._callback(interaction, self.values, *self._args, **self._kwargs)


def get_error_embed(title: str, description: str = None):
    embed = discord.Embed(color=discord.Color.from_rgb(221, 46, 68))
    embed.set_author(name=title, icon_url='https://cdn.discordapp.com/emojis/808045512393621585.png')
    if description:
        embed.description = description
    return embed

def get_success_embed(title: str, description: str = None):
    embed = discord.Embed(color=discord.Color(7844437))
    embed.set_author(name=title, icon_url="https://cdn.discordapp.com/emojis/809149148356018256.png")
    if description:
        embed.description = description
    return embed

def get_question_embed(title: str, description: str = None):
    embed = discord.Embed(color=discord.Color(3315710))
    embed.set_author(name=title, icon_url='https://cdn.discordapp.com/attachments/729998051288285256/924971834343059496/unknown.png')
    if description:
        embed.description = description
    return embed


class ExpiredButtonError(Exception):
    """Raised when pressing a button that has already expired"""

class CustomException(Exception):
    """Raised to log a custom exception"""
    def __init__(self, error, *args):
        self.error = error
        super().__init__(*args)


async def handle_error(interaction: Interaction, error: Exception):
    if isinstance(error, (app_commands.CommandInvokeError, commands.CommandInvokeError)):
        error = error.original

    if isinstance(error, (app_commands.CommandNotFound, commands.CommandNotFound)):
        embed = get_error_embed(title='Unknown command!')

    elif type(error).__name__ == CustomException.__name__:
        embed = get_error_embed(title=error.error, description=str(error))
    
    elif isinstance(error, ExpiredButtonError):
        embed = get_error_embed(title="This action no longer is available.")
    elif isinstance(error, (app_commands.CommandOnCooldown, commands.CommandOnCooldown)):
        sec = timedelta(seconds=int(error.retry_after))
        d = datetime(1,1,1) + sec
        output = ("%dh%dm%ds" % (d.hour, d.minute, d.second))
        if output.startswith("0h"):
            output = output.replace("0h", "")
        if output.startswith("0m"):
            output = output.replace("0m", "")
        embed = get_error_embed(title="That command is still on cooldown!", description="Cooldown expires in " + output + ".")
    elif isinstance(error, (app_commands.MissingPermissions, commands.MissingPermissions)):
        embed = get_error_embed(title="Missing required permissions to use that command!", description=str(error))
    elif isinstance(error, (app_commands.BotMissingPermissions, commands.BotMissingPermissions)):
        embed = get_error_embed(title="I am missing required permissions to use that command!", description=str(error))
    elif isinstance(error, (app_commands.CheckFailure, commands.CheckFailure)):
        embed = get_error_embed(title="Couldn't run that command!", description=None)
    elif isinstance(error, commands.MissingRequiredArgument):
        embed = get_error_embed(title="Missing required argument(s)!")
        embed.description = str(error)
    elif isinstance(error, commands.MaxConcurrencyReached):
        embed = get_error_embed(title="You can't do that right now!")
        embed.description = str(error)
    elif isinstance(error, commands.BadArgument):
        embed = get_error_embed(title="Invalid argument!", description=esc_md(str(error)))
    elif isinstance(error, HSSConnectionError):
        embed = get_error_embed(title="Couldn't connect to HLL Skill System!", description=esc_md(str(error)))
    else:
        embed = get_error_embed(title="An unexpected error occured!", description=esc_md(str(error)))
        try:
            raise error
        except:
            traceback.print_exc()

    if isinstance(interaction, Interaction):
        if interaction.response.is_done() or interaction.is_expired():
            await interaction.followup.send(embed=embed, ephemeral=True)
        else:
            await interaction.response.send_message(embed=embed, ephemeral=True)
    else:
        await interaction.send(embed=embed)


class View(ui.View):
    async def on_error(self, interaction: Interaction, error: Exception, item, /) -> None:
        await handle_error(interaction, error)

class Modal(ui.Modal):
    async def on_error(self, interaction: Interaction, error: Exception, /) -> None:
        await handle_error(interaction, error)

def only_once(func):
    func.__has_been_ran_once = False
    async def decorated(*args, **kwargs):
        if func.__has_been_ran_once:
            raise ExpiredButtonError
        res = await func(*args, **kwargs)
        func.__has_been_ran_once = True
        return res
    return decorated

@ttl_cache(size=100, seconds=60*60*24)
async def get_command_mention(tree: discord.app_commands.CommandTree, name: str, subcommands: str = None):
    commands = await tree.fetch_commands()
    command = next(cmd for cmd in commands if cmd.name == name)
    if subcommands:
        return f"</{command.name} {subcommands}:{command.id}>"
    else:
        return f"</{command.name}:{command.id}>"