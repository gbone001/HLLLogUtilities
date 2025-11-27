from datetime import datetime, timezone

import discord

from discord_utils import EmbedBuilder


def test_embed_builder_sets_core_properties():
    builder = EmbedBuilder(title="Session", description="Details", color=discord.Colour.blue())

    embed = (
        builder
        .set_author(name="Author", icon_url="https://example.com/icon.png")
        .set_footer(text="Footer", icon_url="https://example.com/footer.png")
        .set_thumbnail(url="https://example.com/thumb.png")
        .set_timestamp(datetime.now(timezone.utc))
        .build()
    )

    assert embed.title == "Session"
    assert embed.description == "Details"
    assert embed.color == discord.Colour.blue()
    assert embed.author.name == "Author"
    assert embed.footer.text == "Footer"
    assert embed.thumbnail.url == "https://example.com/thumb.png"
    assert embed.timestamp is not None


def test_embed_builder_fields_and_modifiers():
    embed = (
        EmbedBuilder(title="Test")
        .add_field(name="Label", value="Value")
        .add_inline_field(name="Inline", value="Inline value")
        .add_modifiers_field("Modifiers", ["One", "Two"])
        .build()
    )

    assert len(embed.fields) == 3
    assert embed.fields[0].inline is False
    assert embed.fields[1].inline is True
    assert embed.fields[2].value == "One\nTwo"


def test_embed_builder_skips_empty_modifiers():
    embed = EmbedBuilder(title="Empty Mods").add_modifiers_field("Mods", []).build()

    assert len(embed.fields) == 0
