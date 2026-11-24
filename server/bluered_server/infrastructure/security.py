"""Argon2id password hashing and JWT token provider implementations."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import jwt
from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError, VerifyMismatchError

from bluered_server.contracts import IPasswordHasher, ITokenProvider


class Argon2PasswordHasher(IPasswordHasher):
    """Argon2id password hasher backed by argon2-cffi."""

    def __init__(self, hasher: PasswordHasher | None = None) -> None:
        self._hasher = hasher or PasswordHasher()

    def hash_password(self, password: str) -> str:
        return self._hasher.hash(password)

    def verify_password(self, password: str, password_hash: str) -> bool:
        try:
            return self._hasher.verify(password_hash, password)
        except (VerifyMismatchError, VerificationError, InvalidHashError):
            return False


class JwtTokenProvider(ITokenProvider):
    """Stateless HS256 JWT token issuer and verifier backed by PyJWT."""

    def __init__(
        self,
        secret_key: str,
        *,
        algorithm: str = "HS256",
        ttl_seconds: int = 86400,
    ) -> None:
        if not secret_key:
            raise ValueError("secret_key must be non-empty")
        self._secret_key = secret_key
        self._algorithm = algorithm
        self._ttl_seconds = ttl_seconds

    def issue_token(self, user_id: str, username: str) -> str:
        now = datetime.now(UTC)
        payload: dict[str, Any] = {
            "sub": user_id,
            "username": username,
            "iat": int(now.timestamp()),
            "exp": int((now + timedelta(seconds=self._ttl_seconds)).timestamp()),
        }
        return jwt.encode(payload, self._secret_key, algorithm=self._algorithm)

    def verify_token(self, token: str) -> tuple[str, str] | None:
        try:
            decoded = jwt.decode(
                token,
                self._secret_key,
                algorithms=[self._algorithm],
            )
        except jwt.PyJWTError:
            return None
        sub = decoded.get("sub")
        username = decoded.get("username")
        if not isinstance(sub, str) or not isinstance(username, str):
            return None
        return sub, username
