from __future__ import annotations

import logging
from typing import Dict, List, Tuple

import gspread

from .config import get_config
from .crypto_utils import decrypt_refresh_token
from .date_utils import UserTimezone, format_log_datetime, now_utc_iso
from .sheet_structure import (
    AppState,
    append_dedupe_keys,
    append_log_rows,
    prepare_user_sheet,
    read_app_state,
    read_recent_dedupe_keys,
    write_app_state,
)
from .sheets_client import open_registry_sheet, open_user_sheet
from .spotify_client import (
    get_recently_played,
    get_spotify_user_profile,
    parse_recently_played_items,
    refresh_access_token,
)

logger = logging.getLogger(__name__)


def _read_registry_rows(ws: gspread.Worksheet) -> List[Dict[str, str]]:
    return ws.get_all_records()


def _update_registry_row(
    ws: gspread.Worksheet,
    row_index: int,
    updates: Dict[str, str],
) -> None:
    """
    Update specific columns in registry row (1-based index, including header).
    Assumes headers in first row.
    """
    headers = ws.row_values(1)
    row_num = row_index + 1  # records() are 2..N
    row_values = ws.row_values(row_num)
    # pad row_values to headers length
    if len(row_values) < len(headers):
        row_values.extend([""] * (len(headers) - len(row_values)))

    header_to_index = {h: i for i, h in enumerate(headers)}
    for key, value in updates.items():
        if key in header_to_index:
            idx = header_to_index[key]
            row_values[idx] = value

    # write complete row back
    ws.update(f"{row_num}:{row_num}", [row_values])


def _ensure_registry_headers(ws: gspread.Worksheet) -> None:
    expected = ["user_sheet_id", "enabled", "created_at", "last_seen_at", "last_sync_at", "last_error"]
    headers = ws.row_values(1)
    if headers != expected:
        ws.update("1:1", [expected])


def sync_all_enabled_users() -> None:
    """
    Entry point for cron/worker: iterate over registry and sync all enabled users.
    """
    cfg = get_config()  # noqa: F841  # currently unused, but keeps pattern consistent
    registry_ss = open_registry_sheet()
    registry_ws = registry_ss.worksheet("registry")
    _ensure_registry_headers(registry_ws)

    rows = _read_registry_rows(registry_ws)
    for idx, row in enumerate(rows, start=2):  # row 2..N in sheet
        try:
            if str(row.get("enabled", "")).lower() != "true":
                continue
            user_sheet_id = str(row.get("user_sheet_id", "")).strip()
            if not user_sheet_id:
                continue
            sync_single_user(user_sheet_id)
            _update_registry_row(
                registry_ws,
                row_index=idx - 1,
                updates={"last_sync_at": now_utc_iso(), "last_error": ""},
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Error syncing user_sheet_id=%s: %s", row.get("user_sheet_id"), exc)
            _update_registry_row(
                registry_ws,
                row_index=idx - 1,
                updates={"last_error": str(exc)},
            )


def _compute_after_ms(state: AppState, lookback_minutes: int) -> int | None:
    try:
        last_ts = int(state.last_synced_after_ts)
    except (TypeError, ValueError):
        last_ts = 0
    if last_ts <= 0:
        return None
    # subtract small lookback window in ms
    return max(0, last_ts - lookback_minutes * 60 * 1000)


def _make_dedupe_key(spotify_user_id: str, played_at_iso: str, track_id: str | None) -> str:
    return f"{spotify_user_id}|{played_at_iso}|{track_id or ''}"


def sync_single_user(user_sheet_url_or_id: str) -> None:
    """
    Synchronize a single user's sheet:
    - restore/prepare sheet structure if missing
    - read app_state and decrypt refresh_token
    - refresh access_token
    - fetch recently-played
    - dedupe and append to log + __dedupe
    - update last_synced_after_ts / updated_at / last_error
    """
    ss = open_user_sheet(user_sheet_url_or_id)
    prepare_user_sheet(ss)
    state = read_app_state(ss)

    if not state.refresh_token_enc:
        raise RuntimeError("Missing refresh_token_enc in __app_state")

    refresh_token = decrypt_refresh_token(state.refresh_token_enc)
    tokens = refresh_access_token(refresh_token)

    # if refresh returned new refresh_token, update app_state
    if tokens.refresh_token != refresh_token:
        from .crypto_utils import encrypt_refresh_token

        new_enc = encrypt_refresh_token(tokens.refresh_token)
        write_app_state(ss, {"refresh_token_enc": new_enc})
        state.refresh_token_enc = new_enc

    # ensure spotify_user_id present
    spotify_user_id = state.spotify_user_id
    if not spotify_user_id:
        profile = get_spotify_user_profile(tokens.access_token)
        spotify_user_id = str(profile.get("id", ""))
        write_app_state(ss, {"spotify_user_id": spotify_user_id})

    cfg = get_config()
    after_ms = _compute_after_ms(state, cfg.spotify_recently_played_lookback_minutes)
    payload = get_recently_played(
        tokens.access_token,
        limit=cfg.spotify_recently_played_limit,
        after_ms=after_ms,
    )
    items = parse_recently_played_items(payload)

    if not items:
        write_app_state(ss, {"updated_at": now_utc_iso()})
        return

    # dedupe
    recent_keys = set(
        read_recent_dedupe_keys(ss, cfg.dedupe_read_rows),
    )
    new_log_rows: List[List[str]] = []
    new_dedupe_keys: List[str] = []
    max_played_at_ms = 0

    for item in items:
        played_at = item.get("played_at")
        track_id = item.get("track_id")
        if not played_at or not track_id:
            continue
        dedupe_key = _make_dedupe_key(spotify_user_id, played_at, track_id)
        if dedupe_key in recent_keys:
            continue

        # format Date in user's timezone
        tz = UserTimezone(state.timezone or "UTC")
        date_str = format_log_datetime(played_at, tz)
        log_row = [
            date_str,
            item.get("track_name", ""),
            item.get("artist_names", ""),
            track_id,
            item.get("spotify_url", ""),
        ]
        new_log_rows.append(log_row)
        new_dedupe_keys.append(dedupe_key)

        # compute ms since epoch
        from datetime import datetime, timezone as dt_timezone

        dt = datetime.fromisoformat(played_at.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=dt_timezone.utc)
        ms = int(dt.timestamp() * 1000)
        if ms > max_played_at_ms:
            max_played_at_ms = ms

    if new_log_rows:
        append_log_rows(ss, new_log_rows)
        append_dedupe_keys(ss, new_dedupe_keys)

    updates: Dict[str, str] = {"updated_at": now_utc_iso(), "last_error": ""}
    if max_played_at_ms:
        updates["last_synced_after_ts"] = str(max_played_at_ms)
    write_app_state(ss, updates)

