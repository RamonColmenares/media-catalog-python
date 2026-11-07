"""Stable application errors shared by the HTTP services."""

from __future__ import annotations

from typing import Any


class DomainError(Exception):
    def __init__(self, status_code: int, code: str, message: str, details: Any = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.details = details


def not_found(resource: str, identifier: object) -> DomainError:
    return DomainError(404, "not_found", f"{resource} was not found", {"id": identifier})


def conflict(code: str, message: str, details: Any = None) -> DomainError:
    return DomainError(409, code, message, details)


def invalid(code: str, message: str, details: Any = None) -> DomainError:
    return DomainError(422, code, message, details)
