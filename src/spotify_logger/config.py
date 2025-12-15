import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class AppConfig:
    """Application-level configuration loaded from environment variables."""

    spotify_client_id: str
    spotify_client_secret: str
    spotify_redirect_uri: str

    google_service_account_json: str
    google_service_account_email: str

    registry_sheet_id: str

    fernet_key: str

    # Worker tuning
    dedupe_read_rows: int = 5000
    spotify_recently_played_limit: int = 50  # per request
    spotify_recently_played_lookback_minutes: int = 60

    @classmethod
    def from_env(cls) -> "AppConfig":
        """Load configuration from environment variables with basic validation."""

        def env(name: str, required: bool = True, default: Optional[str] = None) -> str:
            value = os.getenv(name, default)
            if required and not value:
                raise RuntimeError(f"Environment variable {name} is required")
            return value or ""

        return cls(
            spotify_client_id=env("SPOTIFY_CLIENT_ID"),
            spotify_client_secret=env("SPOTIFY_CLIENT_SECRET"),
            spotify_redirect_uri=env("SPOTIFY_REDIRECT_URI"),
            google_service_account_json=env("GOOGLE_SERVICE_ACCOUNT_JSON"),
            google_service_account_email=env("GOOGLE_SERVICE_ACCOUNT_EMAIL"),
            registry_sheet_id=env("REGISTRY_SHEET_ID"),
            fernet_key=env("FERNET_KEY"),
        )


def get_config() -> AppConfig:
    """Convenience accessor used across the app."""
    return AppConfig.from_env()

