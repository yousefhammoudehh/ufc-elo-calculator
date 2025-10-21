from typing import Any, Optional


class ApplicationException(Exception):
    """Base exception for application layer errors."""

    def __init__(self, message: str, data: Optional[dict[str, Any] | list[dict[str, Any]]] = None) -> None:
        self.message = message
        self.data = data
