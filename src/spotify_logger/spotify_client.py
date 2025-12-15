from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

import requests

from .config import get_config


SPOTIFY_TOKEN_URL = "https://accounts.spotify.com/api/token"
SPOTIFY_API_BASE = "https://api.spotify.com/v1"


@dataclass
class SpotifyTokens:
    access_token: str
    refresh_token: str
    expires_in: int
    token_type: str
    scope: str | None = None


def exchange_code_for_tokens(code: str) -> SpotifyTokens:
    """
    Exchange authorization code for access + refresh tokens.
    """
    cfg = get_config()
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": cfg.spotify_redirect_uri,
        "client_id": cfg.spotify_client_id,
        "client_secret": cfg.spotify_client_secret,
    }
    resp = requests.post(SPOTIFY_TOKEN_URL, data=data, timeout=10)
    resp.raise_for_status()
    payload = resp.json()
    return SpotifyTokens(
        access_token=payload["access_token"],
        refresh_token=payload["refresh_token"],
        expires_in=payload["expires_in"],
        token_type=payload.get("token_type", "Bearer"),
        scope=payload.get("scope"),
    )


def refresh_access_token(refresh_token: str) -> SpotifyTokens:
    """
    Use refresh_token to obtain new access token (and possibly new refresh_token).
    """
    cfg = get_config()
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": cfg.spotify_client_id,
        "client_secret": cfg.spotify_client_secret,
    }
    resp = requests.post(SPOTIFY_TOKEN_URL, data=data, timeout=10)
    resp.raise_for_status()
    payload = resp.json()
    return SpotifyTokens(
        access_token=payload["access_token"],
        refresh_token=payload.get("refresh_token", refresh_token),
        expires_in=payload["expires_in"],
        token_type=payload.get("token_type", "Bearer"),
        scope=payload.get("scope"),
    )


def get_spotify_user_profile(access_token: str) -> Dict[str, Any]:
    headers = {"Authorization": f"Bearer {access_token}"}
    resp = requests.get(f"{SPOTIFY_API_BASE}/me", headers=headers, timeout=10)
    resp.raise_for_status()
    return resp.json()


def get_recently_played(
    access_token: str,
    limit: int = 50,
    after_ms: int | None = None,
) -> Dict[str, Any]:
    """
    Call GET /me/player/recently-played.

    :param after_ms: Unix timestamp in ms; if provided, only items played after this time are returned.
    """
    headers = {"Authorization": f"Bearer {access_token}"}
    params: Dict[str, Any] = {"limit": min(limit, 50)}
    if after_ms is not None:
        params["after"] = int(after_ms)
    resp = requests.get(
        f"{SPOTIFY_API_BASE}/me/player/recently-played",
        headers=headers,
        params=params,
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def parse_recently_played_items(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract simplified track play records from recently-played response.
    Each record contains:
      - played_at (ISO8601)
      - track_id
      - track_name
      - artist_names (comma-separated)
      - spotify_url
    """
    items = []
    for item in payload.get("items", []):
        played_at = item.get("played_at")
        track = item.get("track") or {}
        track_id = track.get("id")
        track_name = track.get("name") or ""
        artists = track.get("artists") or []
        artist_names = ", ".join(a.get("name", "") for a in artists if a)
        external_urls = track.get("external_urls") or {}
        spotify_url = external_urls.get("spotify") or (f"https://open.spotify.com/track/{track_id}" if track_id else "")
        items.append(
            {
                "played_at": played_at,
                "track_id": track_id,
                "track_name": track_name,
                "artist_names": artist_names,
                "spotify_url": spotify_url,
                "raw_track": track,
            }
        )
    return items

