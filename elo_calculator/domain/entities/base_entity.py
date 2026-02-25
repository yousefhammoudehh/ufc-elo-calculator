from dataclasses import dataclass, field
from datetime import UTC, datetime


@dataclass(slots=True)
class BaseEntity:
    """Base entity for shared domain fields."""

    id: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, object]:
        return {'id': self.id, 'created_at': self.created_at.isoformat(), 'updated_at': self.updated_at.isoformat()}
