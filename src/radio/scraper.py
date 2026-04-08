from __future__ import annotations

import asyncio
import datetime
from dataclasses import dataclass

import httpx
from bs4 import BeautifulSoup

BASE_URL = "https://radio357.pl/twoje-357/playlista/dzien"
UNAVAILABLE_TEXT = "Playlista nie jest dostępna"


@dataclass(frozen=True)
class SongPlay:
    date: datetime.date
    time: str  # HH:MM
    show: str
    artist: str
    title: str


def parse_playlist(html: str, date: datetime.date) -> tuple[SongPlay, ...] | None:
    if UNAVAILABLE_TEXT in html:
        return None

    soup = BeautifulSoup(html, "lxml")
    plays: list[SongPlay] = []

    for group in soup.find_all(class_="programGroup"):
        h3 = group.find("h3")
        if not h3:
            continue

        # Strip the programGroupDate span to get the bare show name
        date_span = h3.find("span", class_="programGroupDate")
        if date_span:
            date_span.extract()
        show = h3.get_text(strip=True)

        for song_div in group.find_all(class_="programGroupSong"):
            time_div = song_div.find(class_="songDate")
            artist_h5 = song_div.find(class_="songArtis")
            title_div = song_div.find(class_="songTitle")

            if not (time_div and artist_h5 and title_div):
                continue

            plays.append(
                SongPlay(
                    date=date,
                    time=time_div.get_text(strip=True),
                    show=show,
                    artist=artist_h5.get_text(strip=True),
                    title=title_div.get_text(strip=True),
                )
            )

    return tuple(plays)


async def fetch_playlist(
    client: httpx.AsyncClient, date: datetime.date
) -> tuple[SongPlay, ...] | None:
    url = f"{BASE_URL}/{date.isoformat()}/"
    resp = await client.get(url)
    resp.raise_for_status()
    return parse_playlist(resp.text, date)


async def scrape_range(
    from_date: datetime.date,
    to_date: datetime.date,
    skip_dates: frozenset[datetime.date] = frozenset(),
) -> tuple[SongPlay, ...]:
    dates = [
        from_date + datetime.timedelta(days=i)
        for i in range((to_date - from_date).days + 1)
        if (from_date + datetime.timedelta(days=i)) not in skip_dates
    ]

    semaphore = asyncio.Semaphore(5)
    all_plays: list[SongPlay] = []

    async def fetch_one(client: httpx.AsyncClient, date: datetime.date) -> None:
        async with semaphore:
            result = await fetch_playlist(client, date)
            if result is not None:
                all_plays.extend(result)
                print(f"{date}: {len(result)} songs")
            else:
                print(f"{date}: unavailable")

    async with httpx.AsyncClient(follow_redirects=True) as client:
        await asyncio.gather(*(fetch_one(client, d) for d in dates))

    return tuple(sorted(all_plays, key=lambda s: (s.date, s.time)))


async def find_earliest_date(client: httpx.AsyncClient) -> datetime.date:
    lo = datetime.date(2022, 4, 1)
    hi = datetime.date(2022, 8, 1)

    while (hi - lo).days > 1:
        mid = lo + datetime.timedelta(days=(hi - lo).days // 2)
        result = await fetch_playlist(client, mid)
        if result is not None:
            hi = mid
        else:
            lo = mid

    # Check hi itself
    result = await fetch_playlist(client, hi)
    if result is not None:
        return hi
    return lo
