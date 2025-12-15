from __future__ import annotations

from typing import Dict, List

import gspread

from .date_utils import now_utc_iso
from .sheets_client import open_registry_sheet


REGISTRY_TITLE = "registry"
REGISTRY_HEADERS = ["user_sheet_id", "enabled", "created_at", "last_seen_at", "last_sync_at", "last_error"]


def _ensure_registry_worksheet() -> gspread.Worksheet:
    ss = open_registry_sheet()
    try:
        ws = ss.worksheet(REGISTRY_TITLE)
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(REGISTRY_TITLE, rows=1000, cols=len(REGISTRY_HEADERS))
    headers = ws.row_values(1)
    if headers != REGISTRY_HEADERS:
        ws.update("1:1", [REGISTRY_HEADERS])
    return ws


def _read_registry(ws: gspread.Worksheet) -> List[Dict[str, str]]:
    return ws.get_all_records()


def ensure_registry_entry(user_sheet_id: str) -> None:
    """
    Ensure there is a registry row for given user_sheet_id.
    If missing, append; if exists, update last_seen_at.
    """
    ws = _ensure_registry_worksheet()
    rows = _read_registry(ws)
    now_iso = now_utc_iso()
    for idx, row in enumerate(rows, start=2):
        if str(row.get("user_sheet_id", "")).strip() == user_sheet_id:
            # update last_seen_at
            _update_registry_row(ws, idx, {"last_seen_at": now_iso})
            return

    # append new row
    new_row = [user_sheet_id, "false", now_iso, now_iso, "", ""]
    ws.append_rows([new_row], value_input_option="RAW")


def set_registry_enabled(user_sheet_id: str, enabled: bool) -> None:
    """
    Update enabled flag for given user_sheet_id.
    """
    ws = _ensure_registry_worksheet()
    rows = _read_registry(ws)
    for idx, row in enumerate(rows, start=2):
        if str(row.get("user_sheet_id", "")).strip() == user_sheet_id:
            _update_registry_row(ws, idx, {"enabled": "true" if enabled else "false"})
            return
    # if no row yet, create one with appropriate enabled state
    ensure_registry_entry(user_sheet_id)
    if enabled:
        set_registry_enabled(user_sheet_id, enabled)


def _update_registry_row(ws: gspread.Worksheet, row_num: int, updates: Dict[str, str]) -> None:
    headers = ws.row_values(1)
    row_values = ws.row_values(row_num)
    if len(row_values) < len(headers):
        row_values.extend([""] * (len(headers) - len(row_values)))
    header_to_idx = {h: i for i, h in enumerate(headers)}
    for key, value in updates.items():
        if key in header_to_idx:
            row_values[header_to_idx[key]] = value
    ws.update(f"{row_num}:{row_num}", [row_values])

