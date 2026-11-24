"""Application-layer exceptions for BlueRed central server."""

from __future__ import annotations


class ServerApplicationError(Exception):
    """Base exception for server application services."""


class AuthenticationError(ServerApplicationError):
    """Raised when login credentials or tokens are invalid."""


class AccountLockedError(AuthenticationError):
    """Raised when an account is temporarily locked due to failed login attempts."""


class DuplicateUserError(ServerApplicationError):
    """Raised when attempting to register an already-taken username."""


class DuplicateSessionError(ServerApplicationError):
    """Raised when a user attempts to log in while another active session exists."""


class ValidationError(ServerApplicationError, ValueError):
    """Raised when application-level input validation fails."""


class NotFoundError(ServerApplicationError):
    """Raised when a requested entity or session does not exist."""
