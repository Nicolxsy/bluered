"""Authentication & session-issuance service (Cas_07)."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from bluered_server.contracts import (
    IPasswordHasher,
    ISessionStore,
    ITokenProvider,
    IUserRepository,
)
from bluered_server.domain import PeerSession, UserAccount

from .errors import (
    AccountLockedError,
    AuthenticationError,
    DuplicateSessionError,
    DuplicateUserError,
    ValidationError,
)


class AuthService:
    """User registration, Argon2id login, JWT issuance, lockout, and single-session guard."""

    def __init__(
        self,
        user_repo: IUserRepository,
        session_store: ISessionStore,
        password_hasher: IPasswordHasher,
        token_provider: ITokenProvider,
        *,
        max_failed_attempts: int = 5,
        lock_duration_seconds: int = 300,
        session_ttl_seconds: int = 45,
    ) -> None:
        self._user_repo = user_repo
        self._session_store = session_store
        self._password_hasher = password_hasher
        self._token_provider = token_provider
        self._max_failed_attempts = max_failed_attempts
        self._lock_duration_seconds = lock_duration_seconds
        self._session_ttl_seconds = session_ttl_seconds

    async def register(self, username: str, password: str) -> UserAccount:
        """Register a new user account with Argon2id password hash."""
        clean_username = username.strip()
        if not (3 <= len(clean_username) <= 64):
            raise ValidationError("username length must be between 3 and 64 characters")
        if len(password) < 6 or len(password) > 256:
            raise ValidationError("password length must be between 6 and 256 characters")

        existing = await self._user_repo.get_by_username(clean_username)
        if existing is not None:
            raise DuplicateUserError(f"Username '{clean_username}' is already registered")

        pw_hash = self._password_hasher.hash_password(password)
        account = UserAccount(
            user_id=uuid4().hex,
            username=clean_username,
            password_hash=pw_hash,
        )
        await self._user_repo.create(account)
        return account

    async def login(
        self,
        username: str,
        password: str,
        ip_address: str,
        tcp_port: int,
        *,
        now: datetime | None = None,
    ) -> tuple[UserAccount, str]:
        """Authenticate credentials, enforce lockout & single-session policy, and issue JWT."""
        clean_username = username.strip()
        account = await self._user_repo.get_by_username(clean_username)
        if account is None:
            raise AuthenticationError("Invalid username or password")

        current = now or datetime.now(UTC)
        if account.is_locked(now=current):
            raise AccountLockedError("Account is temporarily locked due to failed login attempts")

        if not self._password_hasher.verify_password(password, account.password_hash):
            account.record_failed_login(
                max_attempts=self._max_failed_attempts,
                lock_duration_seconds=self._lock_duration_seconds,
                now=current,
            )
            await self._user_repo.update(account)
            if account.is_locked(now=current):
                raise AccountLockedError(
                    "Account is temporarily locked due to failed login attempts"
                )
            raise AuthenticationError("Invalid username or password")

        existing_session = await self._session_store.get_session(account.user_id)
        if existing_session is not None and not existing_session.is_expired(now=current):
            raise DuplicateSessionError(f"User '{account.username}' already has an active session")

        account.record_successful_login()
        await self._user_repo.update(account)

        session = PeerSession(
            user_id=account.user_id,
            username=account.username,
            ip_address=ip_address,
            tcp_port=tcp_port,
            last_heartbeat_at=current,
            ttl_seconds=self._session_ttl_seconds,
        )
        await self._session_store.upsert_session(session)

        token = self._token_provider.issue_token(account.user_id, account.username)
        return account, token

    def verify_token(self, token: str) -> tuple[str, str] | None:
        """Verify a session JWT and return (user_id, username) if valid."""
        return self._token_provider.verify_token(token)
