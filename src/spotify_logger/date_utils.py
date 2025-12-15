from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import ClassVar

from zoneinfo import ZoneInfo


MONTH_NAMES_EN: dict[int, str] = {
    1: "January",
    2: "February",
    3: "March",
    4: "April",
    5: "May",
    6: "June",
    7: "July",
    8: "August",
    9: "September",
    10: "October",
    11: "November",
    12: "December",
}


@dataclass(frozen=True)
class UserTimezone:
    """Wrapper around IANA timezone string."""

    name: str

    _UTC: ClassVar["UserTimezone"]

    @property
    def zoneinfo(self) -> ZoneInfo:
        return ZoneInfo(self.name)

    @classmethod
    def utc(cls) -> "UserTimezone":
        return cls("UTC")


UserTimezone._UTC = UserTimezone("UTC")


def parse_spotify_played_at(played_at_iso: str) -> datetime:
    """Parse Spotify played_at (ISO8601 in UTC) into aware datetime in UTC."""
    dt = datetime.fromisoformat(played_at_iso.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def format_log_datetime(played_at_iso: str, user_tz: UserTimezone) -> str:
    """
    Format Spotify played_at timestamp into the required log format:

    {Month} {day}, {year} at {hour}:{minute:02d}{AMPM}
    e.g. "November 12, 2025 at 10:42AM"
    """
    utc_dt = parse_spotify_played_at(played_at_iso)
    local_dt = utc_dt.astimezone(user_tz.zoneinfo)

    month_name = MONTH_NAMES_EN[local_dt.month]
    day = local_dt.day
    year = local_dt.year

    hour_24 = local_dt.hour
    am_pm = "AM" if hour_24 < 12 else "PM"
    hour_12 = hour_24 % 12
    if hour_12 == 0:
        hour_12 = 12

    minute = local_dt.minute

    return f"{month_name} {day}, {year} at {hour_12}:{minute:02d}{am_pm}"


def now_utc_iso() -> str:
    """Return current UTC time as ISO8601 string."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

