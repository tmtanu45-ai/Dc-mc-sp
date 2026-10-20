import asyncio
import os
import re
from collections import defaultdict, deque
from urllib.parse import urlparse

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv

load_dotenv()

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
JAMENDO_CLIENT_ID = os.getenv("JAMENDO_CLIENT_ID")

if not DISCORD_TOKEN or not JAMENDO_CLIENT_ID:
    raise RuntimeError(
        "Missing DISCORD_TOKEN or JAMENDO_CLIENT_ID environment variable."
    )

intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)

http = None
queues = defaultdict(deque)
current_tracks = {}
playback_locks = defaultdict(asyncio.Lock)
MAX_QUEUE_SIZE = 50


def spotify_track_url(value: str) -> bool:
    """Accept public Spotify track URLs only."""
    try:
        parsed = urlparse(value.strip())
        return (
            parsed.scheme == "https"
            and parsed.hostname == "open.spotify.com"
            and re.fullmatch(
                r"/track/[A-Za-z0-9]+/?", parsed.path
            ) is not None
        )
    except (ValueError, AttributeError):
        return False


async def get_spotify_metadata(url: str) -> dict:
    """Retrieve public track title and artwork through Spotify oEmbed."""
    async with http.get(
        "https://open.spotify.com/oembed",
        params={"url": url},
        timeout=aiohttp.ClientTimeout(total=15),
    ) as response:
        if response.status != 200:
            raise ValueError(
                "Spotify could not read that track link. "
                "Make sure the track is public."
            )
        data = await response.json()

    title = data.get("title")
    if not title:
        raise ValueError("Spotify returned no track title.")

    return {
        "title": title,
        "thumbnail": data.get("thumbnail_url"),
        "spotify_url": url,
    }


async def search_jamendo(query: str) -> dict:
    """Search Jamendo for an audio track."""
    params = {
        "client_id": JAMENDO_CLIENT_ID,
        "format": "json",
        "limit": 10,
        "namesearch": query,
        "audioformat": "mp32",
    }

    async with http.get(
        "https://api.jamendo.com/v3.0/tracks/",
        params=params,
        timeout=aiohttp.ClientTimeout(total=20),
    ) as response:
        if response.status != 200:
            raise RuntimeError(
                f"Jamendo API error: HTTP {response.status}"
            )
        data = await response.json()

    for item in data.get("results", []):
        audio_url = item.get("audio")
        if audio_url:
            return {
                "title": item.get("name", "Unknown track"),
                "artist": item.get("artist_name", "Unknown artist"),
                "audio": audio_url,
                "thumbnail": item.get("image"),
                "license": item.get("license_ccurl"),
            }

    raise ValueError(
        "No playable Jamendo match was found. Try a different song."
    )


async def resolve_track(query: str) -> dict:
    """Resolve a Spotify track URL or plain text search."""
    spotify_data = None

    if "spotify" in query.lower():
        if not spotify_track_url(query):
            raise ValueError(
                "Paste a public Spotify track URL, such as "
                "https://open.spotify.com/track/TRACK_ID"
            )

        spotify_data = await get_spotify_metadata(query.strip())
        search_query = spotify_data["title"]
    else:
        search_query = query.strip()

    if not search_query:
        raise ValueError("Enter a song name or Spotify track URL.")

    track = await search_jamendo(search_query)

    if spotify_data:
        track["spotify_url"] = spotify_data["spotify_url"]
        track["spotify_title"] = spotify_data["title"]
        track["spotify_thumbnail"] = spotify_data["thumbnail"]

    return track


def make_embed(track: dict, heading: str = "Now Playing") -> discord.Embed:
    title = track.get("spotify_title") or track["title"]

    embed = discord.Embed(
        title=title,
        description=f"**{heading}**\nArtist: {track['artist']}",
        color=discord.Color.green(),
    )

    if track.get("spotify_url"):
        embed.url = track["spotify_url"]
        embed.add_field(
            name="Spotify link",
            value=f"[Open in Spotify]({track['spotify_url']})",
            inline=False,
        )

    artwork = (
        track.get("spotify_thumbnail")
        or track.get("thumbnail")
    )
    if artwork:
        embed.set_thumbnail(url=artwork)

    if track.get("license"):
        embed.add_field(
            name="Audio licence",
            value=f"[Review licence]({track['license']})",
            inline=False,
        )

    embed.set_footer(text="Spotify metadata • Jamendo audio")
    return embed


async def play_next(guild: discord.Guild):
    """Start the next track in this guild's queue."""
    async with playback_locks[guild.id]:
        voice = guild.voice_client

        if not voice or not voice.is_connected():
            current_tracks.pop(guild.id, None)
            return

        if voice.is_playing() or voice.is_paused():
            return

        if not queues[guild.id]:
            current_tracks.pop(guild.id, None)
            return

        track = queues[guild.id].popleft()
        current_tracks[guild.id] = track

        try:
            source = discord.FFmpegPCMAudio(
                track["audio"],
                before_options=(
                    "-reconnect 1 -reconnect_streamed 1 "
                    "-reconnect_delay_max 5"
                ),
                options="-vn",
            )

            def after_playing(error):
                if error:
                    print(f"Playback error: {error}")

                future = asyncio.run_coroutine_threadsafe(
                    play_next(guild), bot.loop
                )

                def report_result(done):
                    try:
                        done.result()
                    except Exception as exc:
                        print(f"Queue error: {exc}")

                future.add_done_callback(report_result)

            voice.play(source, after=after_playing)
            print(
                f"Playing in {guild.name}: "
                f"{track.get('spotify_title') or track['title']}"
            )

        except Exception as error:
            print(f"Could not start audio: {error}")
            current_tracks.pop(guild.id, None)
            asyncio.create_task(play_next(guild))


async def get_voice(interaction: discord.Interaction):
    guild = interaction.guild

    if guild is None:
        raise ValueError("This command must be used in a server.")

    member = guild.get_member(interaction.user.id)
    if member is None or member.voice is None:
        raise ValueError(
            "Join a Discord voice channel before using /play."
        )

    channel = member.voice.channel
    voice = guild.voice_client

    if voice and voice.is_connected():
        if voice.channel != channel:
            if voice.is_playing() or voice.is_paused():
                raise ValueError(
                    "The bot is playing in another voice channel."
                )
            await voice.move_to(channel)
    else:
        await channel.connect(timeout=20, reconnect=True)

    return guild.voice_client


@bot.event
async def on_ready():
    print(f"Logged in as {bot.user} (ID: {bot.user.id})")

    try:
        synced = await bot.tree.sync()
        print(f"Synced {len(synced)} slash commands.")
    except Exception as error:
        print(f"Slash command sync failed: {error}")


@bot.tree.command(
    name="play",
    description="Play a song or Spotify track link"
)
@app_commands.describe(
    query="Song title or public Spotify track URL"
)
async def play(interaction: discord.Interaction, query: str):
    await interaction.response.defer(thinking=True)

    try:
        voice = await get_voice(interaction)
        track = await resolve_track(query)
        guild = interaction.guild

        if guild is None or voice is None:
            raise ValueError("The voice connection was not established.")

        if len(queues[guild.id]) >= MAX_QUEUE_SIZE:
            raise ValueError("The queue is full. Maximum: 50 tracks.")

        queues[guild.id].append(track)

        if not voice.is_playing() and not voice.is_paused():
            await play_next(guild)

        await interaction.followup.send(
            embed=make_embed(track, "Added to queue")
        )

    except Exception as error:
        await interaction.followup.send(
            f"❌ {error}", ephemeral=True
        )


@bot.tree.command(name="pause", description="Pause the current song")
async def pause(interaction: discord.Interaction):
    voice = interaction.guild.voice_client if interaction.guild else None

    if voice and voice.is_playing():
        voice.pause()
        await interaction.response.send_message("⏸️ Paused.")
    else:
        await interaction.response.send_message(
            "Nothing is playing.", ephemeral=True
        )


@bot.tree.command(name="resume", description="Resume the current song")
async def resume(interaction: discord.Interaction):
    voice = interaction.guild.voice_client if interaction.guild else None

    if voice and voice.is_paused():
        voice.resume()
        await interaction.response.send_message("▶️ Resumed.")
    else:
        await interaction.response.send_message(
            "Playback is not paused.", ephemeral=True
        )


@bot.tree.command(name="skip", description="Skip the current song")
async def skip(interaction: discord.Interaction):
    voice = interaction.guild.voice_client if interaction.guild else None

    if voice and (voice.is_playing() or voice.is_paused()):
        voice.stop()
        await interaction.response.send_message("⏭️ Skipped.")
    else:
        await interaction.response.send_message(
            "Nothing is playing.", ephemeral=True
        )


@bot.tree.command(name="queue", description="Show the music queue")
async def show_queue(interaction: discord.Interaction):
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message(
            "Use this command in a server.", ephemeral=True
        )
        return

    embed = discord.Embed(
        title="🎵 Music Queue",
        color=discord.Color.blurple(),
    )

    playing = current_tracks.get(guild.id)
    embed.add_field(
        name="Now playing",
        value=(
            playing.get("spotify_title") or playing["title"]
            if playing else "Nothing playing"
        ),
        inline=False,
    )

    items = list(queues[guild.id])
    if items:
        embed.add_field(
            name="Up next",
            value="\n".join(
                f"**{index}.** "
                f"{item.get('spotify_title') or item['title']} "
                f"— {item['artist']}"
                for index, item in enumerate(items[:10], 1)
            ),
            inline=False,
        )
    else:
        embed.add_field(
            name="Up next",
            value="The queue is empty.",
            inline=False,
        )

    await interaction.response.send_message(embed=embed)


@bot.tree.command(
    name="nowplaying",
    description="Show the current song and artwork"
)
async def nowplaying(interaction: discord.Interaction):
    track = (
        current_tracks.get(interaction.guild.id)
        if interaction.guild else None
    )

    if not track:
        await interaction.response.send_message(
            "Nothing is playing.", ephemeral=True
        )
        return

    await interaction.response.send_message(embed=make_embed(track))


@bot.tree.command(
    name="stop",
    description="Stop playback and clear the queue"
)
async def stop(interaction: discord.Interaction):
    guild = interaction.guild
    voice = guild.voice_client if guild else None

    if guild:
        queues[guild.id].clear()

    if voice:
        voice.stop()

    await interaction.response.send_message(
        "⏹️ Playback stopped and the queue cleared."
    )


@bot.tree.command(
    name="leave",
    description="Clear the queue and disconnect the bot"
)
async def leave(interaction: discord.Interaction):
    guild = interaction.guild
    voice = guild.voice_client if guild else None

    if guild:
        queues[guild.id].clear()
        current_tracks.pop(guild.id, None)

    if voice:
        if voice.is_playing() or voice.is_paused():
            voice.stop()
        await voice.disconnect()

    await interaction.response.send_message(
        "👋 Disconnected from the voice channel."
    )


async def main():
    global http

    timeout = aiohttp.ClientTimeout(total=25)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        http = session
        async with bot:
            await bot.start(DISCORD_TOKEN)


if __name__ == "__main__":
    asyncio.run(main())
          
