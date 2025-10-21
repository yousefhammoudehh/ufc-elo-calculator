from typing import Any, Optional


class DomainException(Exception):
    """Base exception for domain-specific errors."""

    def __init__(self, message: str, data: Optional[dict[str, Any] | list[dict[str, Any]]] = None) -> None:
        self.message = message
        self.data = data
