from __future__ import annotations

import json
import re
from typing import Any

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from .config import get_config


SHEET_URL_RE = re.compile(r"/spreadsheets/d/([a-zA-Z0-9-_]+)")


def extract_sheet_id(sheet_url_or_id: str) -> str:
    """Accept full URL or raw ID and return the sheet ID."""
    sheet_url_or_id = sheet_url_or_id.strip()
    m = SHEET_URL_RE.search(sheet_url_or_id)
    if m:
        return m.group(1)
    # Assume it's already an ID
    return sheet_url_or_id


def get_gspread_client() -> gspread.Client:
    """Return authorized gspread client using service account JSON from env."""
    cfg = get_config()

    credentials_dict = json.loads(cfg.google_service_account_json)
    if "private_key" in credentials_dict:
        credentials_dict["private_key"] = credentials_dict["private_key"].replace("\\n", "\n")

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
    return gspread.authorize(creds)


def open_user_sheet(sheet_url_or_id: str) -> gspread.Spreadsheet:
    client = get_gspread_client()
    sheet_id = extract_sheet_id(sheet_url_or_id)
    return client.open_by_key(sheet_id)


def open_registry_sheet() -> gspread.Spreadsheet:
    cfg = get_config()
    client = get_gspread_client()
    return client.open_by_key(cfg.registry_sheet_id)


def get_or_create_worksheet(
    spreadsheet: gspread.Spreadsheet, title: str, rows: int = 1000, cols: int = 26
) -> gspread.Worksheet:
    try:
        return spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        return spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)


def set_hidden(spreadsheet: gspread.Spreadsheet, title: str, hidden: bool = True) -> None:
    """Mark worksheet as hidden if backend supports it. Best-effort; ignore failures."""
    try:
        ws = spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        return

    try:
        # gspread >=5.8: use update_sheet_properties via batch_update
        spreadsheet.batch_update(
            {
                "requests": [
                    {
                        "updateSheetProperties": {
                            "properties": {"sheetId": ws.id, "hidden": hidden},
                            "fields": "hidden",
                        }
                    }
                ]
            }
        )
    except Exception:
        # Non-critical; UI can still work without hidden flag
        return

