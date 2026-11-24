"""Domain exceptions for BlueRed central server."""

from __future__ import annotations


class ServerDomainError(Exception):
    """Base exception for server domain invariant violations."""


class DomainValidationError(ServerDomainError, ValueError):
    """Raised when a server domain entity or value object fails invariant validation."""
