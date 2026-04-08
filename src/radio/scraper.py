from __future__ import annotations

import asyncio
import datetime
import logging
from dataclasses import dataclass

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

BASE_URL = "https://radio357.pl/twoje-357/playlista/dzien"
UNAVAILABLE_TEXT = "Playlista nie jest dostępna"


@dataclass(frozen=True)
class SongPlay:
    date: datetime.date
    time: str  # HH:MM
    program: str
    artist: str
    title: str


def parse_playlist(html: str, date: datetime.date) -> tuple[SongPlay, ...] | None:
    if UNAVAILABLE_TEXT in html:
        return None

    soup = BeautifulSoup(html, "lxml")
    plays: list[SongPlay] = []

    # Shows are h3 headers and songs are divs within a flat container.
    # Iterate linearly: h3 updates current program, songs inherit it.
    container = soup.find(class_="programGroup")
    if not container:
        return None

    current_program = ""
    for element in container.children:
        if not hasattr(element, "name") or element.name is None:
            continue

        if element.name == "h3":
            date_span = element.find("span", class_="programGroupDate")
            if date_span:
                date_span.extract()
            current_program = element.get_text(strip=True)

        elif "programGroupSong" in (element.get("class") or []):
            time_div = element.find(class_="songDate")
            artist_h5 = element.find(class_="songArtis")
            title_div = element.find(class_="songTitle")

            if not (time_div and artist_h5 and title_div):
                continue

            plays.append(
                SongPlay(
                    date=date,
                    time=time_div.get_text(strip=True),
                    program=current_program,
                    artist=artist_h5.get_text(strip=True),
                    title=title_div.get_text(strip=True),
                )
            )

    return tuple(plays) if plays else None


async def fetch_playlist(
    client: httpx.AsyncClient, date: datetime.date, retries: int = 3
) -> tuple[SongPlay, ...] | None:
    url = f"{BASE_URL}/{date.isoformat()}/"
    for attempt in range(retries):
        try:
            resp = await client.get(url)
            resp.raise_for_status()
            return parse_playlist(resp.text, date)
        except httpx.HTTPError as exc:
            if attempt < retries - 1:
                wait = 2 ** attempt
                logger.warning("date=%s attempt=%d/%d error=%s retrying_in=%ds", date, attempt + 1, retries, exc, wait)
                await asyncio.sleep(wait)
            else:
                logger.error("date=%s error=%s status=failed", date, exc)
                return None


async def scrape_range(
    from_date: datetime.date,
    to_date: datetime.date,
    skip_dates: frozenset[datetime.date] = frozenset(),
) -> tuple[SongPlay, ...]:
    dates = [
        d for i in range((to_date - from_date).days + 1)
        if (d := from_date + datetime.timedelta(days=i)) not in skip_dates
    ]

    total = len(dates)
    logger.info("scrape_start from=%s to=%s total_dates=%d skipped=%d", from_date, to_date, total, (to_date - from_date).days + 1 - total)

    semaphore = asyncio.Semaphore(5)
    all_plays: list[SongPlay] = []
    completed = 0
    unavailable = 0
    failed = 0
    total_songs = 0

    async def fetch_one(client: httpx.AsyncClient, date: datetime.date) -> None:
        nonlocal completed, unavailable, failed, total_songs
        async with semaphore:
            result = await fetch_playlist(client, date)
            completed += 1

            if result is not None:
                all_plays.extend(result)
                total_songs += len(result)
                logger.debug("date=%s songs=%d", date, len(result))
            else:
                unavailable += 1
                logger.debug("date=%s status=unavailable", date)

            if completed % 50 == 0 or completed == total:
                logger.info(
                    "progress=%d/%d songs=%d unavailable=%d",
                    completed, total, total_songs, unavailable,
                )

    async with httpx.AsyncClient(follow_redirects=True) as client:
        await asyncio.gather(*(fetch_one(client, d) for d in dates))

    logger.info(
        "scrape_done dates=%d songs=%d unavailable=%d",
        completed, total_songs, unavailable,
    )

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

    # Check both boundary candidates
    for candidate in (lo, hi):
        result = await fetch_playlist(client, candidate)
        if result is not None:
            return candidate
    return hi  # fallback — should not happen with valid bounds
