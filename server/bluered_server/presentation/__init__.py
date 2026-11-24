"""Presentation layer exports (FastAPI REST & WebSocket routes) for BlueRed server."""

from __future__ import annotations

from .app import ServerServices, create_fastapi_app
from .schemas import (
    CreateCommunityRequest,
    LoginRequest,
    LoginResponse,
    PublishReviewRequest,
    RegisterRequest,
    RegisterResponse,
)

__all__ = [
    "CreateCommunityRequest",
    "LoginRequest",
    "LoginResponse",
    "PublishReviewRequest",
    "RegisterRequest",
    "RegisterResponse",
    "ServerServices",
    "create_fastapi_app",
]
