from cryptography.fernet import Fernet, InvalidToken

from .config import get_config


def _get_fernet() -> Fernet:
    cfg = get_config()
    return Fernet(cfg.fernet_key.encode("utf-8"))


def encrypt_refresh_token(refresh_token: str) -> str:
    """Encrypt refresh token using Fernet; returns base64-encoded string."""
    f = _get_fernet()
    token_bytes = refresh_token.encode("utf-8")
    encrypted = f.encrypt(token_bytes)
    return encrypted.decode("utf-8")


def decrypt_refresh_token(refresh_token_enc: str) -> str:
    """Decrypt refresh token; raises InvalidToken on failure."""
    if not refresh_token_enc:
        raise ValueError("Empty encrypted refresh token")
    f = _get_fernet()
    try:
        decrypted = f.decrypt(refresh_token_enc.encode("utf-8"))
    except InvalidToken as exc:
        raise InvalidToken("Failed to decrypt refresh token") from exc
    return decrypted.decode("utf-8")

