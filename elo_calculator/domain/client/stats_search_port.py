from typing import Protocol


class StatsSearchPort(Protocol):
    """Port for searching a fighter on a stats site to resolve their profile link."""

    def search_fighter(self, name: str) -> str | None:  # pragma: no cover - IO boundary
        """Return a stats profile link for the fighter by name, or None if not found."""
        ...
