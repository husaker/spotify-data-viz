from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple

import gspread

from .date_utils import now_utc_iso
from .sheets_client import get_or_create_worksheet, set_hidden


LOG_SHEET_TITLE = "log"
APP_STATE_TITLE = "__app_state"
DEDUPE_TITLE = "__dedupe"
TRACKS_TITLE = "__tracks"
ARTISTS_TITLE = "__artists"

LOG_HEADERS = ["Date", "Track", "Artist", "Spotify ID", "URL"]
APP_STATE_HEADERS = ["key", "value"]
DEDUPE_HEADERS = ["dedupe_key"]


APP_STATE_DEFAULTS: Dict[str, str] = {
    "enabled": "false",
    "timezone": "UTC",
    "last_synced_after_ts": "0",
    "spotify_user_id": "",
    "refresh_token_enc": "",
    "created_at": "",  # will be set on init
    "updated_at": "",  # will be set on init / updates
    "last_error": "",
}


@dataclass
class AppState:
    enabled: bool
    timezone: str
    last_synced_after_ts: str
    spotify_user_id: str
    refresh_token_enc: str
    created_at: str
    updated_at: str
    last_error: str = ""


def _ensure_headers(ws: gspread.Worksheet, expected_headers: List[str]) -> None:
    """Ensure the first row contains the expected headers; overwrite if needed."""
    values = ws.row_values(1)
    if values != expected_headers:
        ws.update("1:1", [expected_headers])


def prepare_user_sheet(spreadsheet: gspread.Spreadsheet, timezone: str | None = None) -> None:
    """
    Ensure required worksheets and headers exist for a given user sheet.

    - log: public log with fixed 5-column headers
    - __app_state: key/value store with defaults
    - __dedupe: dedupe_key column
    - __tracks / __artists: created empty if missing
    """
    # log
    log_ws = get_or_create_worksheet(spreadsheet, LOG_SHEET_TITLE)
    _ensure_headers(log_ws, LOG_HEADERS)

    # __app_state
    app_state_ws = get_or_create_worksheet(spreadsheet, APP_STATE_TITLE, rows=50, cols=2)
    _ensure_headers(app_state_ws, APP_STATE_HEADERS)
    set_hidden(spreadsheet, APP_STATE_TITLE, True)

    # if there are no key/value rows beyond header, write defaults
    existing = app_state_ws.get_all_records()
    if not existing:
        now_iso = now_utc_iso()
        rows = []
        for key, default_value in APP_STATE_DEFAULTS.items():
            value = default_value
            if key in ("created_at", "updated_at"):
                value = now_iso
            if key == "timezone" and timezone:
                value = timezone
            rows.append([key, value])
        if rows:
            app_state_ws.append_rows(rows, value_input_option="RAW")

    # __dedupe
    dedupe_ws = get_or_create_worksheet(spreadsheet, DEDUPE_TITLE, rows=1000, cols=1)
    _ensure_headers(dedupe_ws, DEDUPE_HEADERS)
    set_hidden(spreadsheet, DEDUPE_TITLE, True)

    # __tracks
    get_or_create_worksheet(spreadsheet, TRACKS_TITLE, rows=1000, cols=10)
    set_hidden(spreadsheet, TRACKS_TITLE, True)

    # __artists
    get_or_create_worksheet(spreadsheet, ARTISTS_TITLE, rows=1000, cols=10)
    set_hidden(spreadsheet, ARTISTS_TITLE, True)


def validate_log_headers(spreadsheet: gspread.Spreadsheet) -> Tuple[bool, gspread.Worksheet]:
    """
    Check if log sheet exists and has correct headers.
    Returns (is_valid, worksheet).
    """
    log_ws = get_or_create_worksheet(spreadsheet, LOG_SHEET_TITLE)
    values = log_ws.row_values(1)
    is_valid = values == LOG_HEADERS
    return is_valid, log_ws


def fix_log_headers(spreadsheet: gspread.Spreadsheet) -> None:
    """Forcefully overwrite first row in log with correct headers."""
    log_ws = get_or_create_worksheet(spreadsheet, LOG_SHEET_TITLE)
    _ensure_headers(log_ws, LOG_HEADERS)


def read_app_state(spreadsheet: gspread.Spreadsheet) -> AppState:
    ws = get_or_create_worksheet(spreadsheet, APP_STATE_TITLE, rows=50, cols=2)
    _ensure_headers(ws, APP_STATE_HEADERS)
    rows = ws.get_all_records()
    data: Dict[str, str] = {row["key"]: str(row.get("value", "")) for row in rows}
    # ensure defaults present
    for key, default in APP_STATE_DEFAULTS.items():
        data.setdefault(key, default)
    return AppState(
        enabled=data.get("enabled", "false").lower() == "true",
        timezone=data.get("timezone", "UTC"),
        last_synced_after_ts=data.get("last_synced_after_ts", "0"),
        spotify_user_id=data.get("spotify_user_id", ""),
        refresh_token_enc=data.get("refresh_token_enc", ""),
        created_at=data.get("created_at", ""),
        updated_at=data.get("updated_at", ""),
        last_error=data.get("last_error", ""),
    )


def write_app_state(spreadsheet: gspread.Spreadsheet, state_updates: Dict[str, str]) -> None:
    """
    Update __app_state keys with given values.
    Missing keys are preserved.
    """
    ws = get_or_create_worksheet(spreadsheet, APP_STATE_TITLE, rows=50, cols=2)
    _ensure_headers(ws, APP_STATE_HEADERS)
    rows = ws.get_all_records()
    current: Dict[str, str] = {row["key"]: str(row.get("value", "")) for row in rows}
    for k, v in state_updates.items():
        current[k] = v

    # ensure all defaults exist
    for key, default in APP_STATE_DEFAULTS.items():
        current.setdefault(key, default)

    # rewrite everything (header row already set)
    data_rows = [[k, v] for k, v in current.items()]
    if data_rows:
        ws.resize(rows=len(data_rows) + 1, cols=2)
        ws.update("2:{}".format(len(data_rows) + 1), data_rows)


def read_recent_dedupe_keys(
    spreadsheet: gspread.Spreadsheet, limit_rows: int
) -> List[str]:
    ws = get_or_create_worksheet(spreadsheet, DEDUPE_TITLE, rows=1000, cols=1)
    _ensure_headers(ws, DEDUPE_HEADERS)
    all_values = ws.col_values(1)
    # drop header
    keys = all_values[1:]
    if limit_rows <= 0 or len(keys) <= limit_rows:
        return keys
    return keys[-limit_rows:]


def append_dedupe_keys(spreadsheet: gspread.Spreadsheet, keys: List[str]) -> None:
    if not keys:
        return
    ws = get_or_create_worksheet(spreadsheet, DEDUPE_TITLE, rows=1000, cols=1)
    _ensure_headers(ws, DEDUPE_HEADERS)
    rows = [[k] for k in keys]
    ws.append_rows(rows, value_input_option="RAW")


def append_log_rows(spreadsheet: gspread.Spreadsheet, rows: List[List[str]]) -> None:
    """
    Append log rows (Date, Track, Artist, Spotify ID, URL).
    """
    if not rows:
        return
    ws = get_or_create_worksheet(spreadsheet, LOG_SHEET_TITLE)
    _ensure_headers(ws, LOG_HEADERS)
    ws.append_rows(rows, value_input_option="RAW")

