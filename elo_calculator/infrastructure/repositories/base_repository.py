from __future__ import annotations

from collections.abc import Mapping
from typing import Any


class BaseRepository[EntityT]:
    """Base repository abstraction.

    Concrete repositories should inherit this and implement persistence behavior.
    """

    async def get(self, _entity_id: str) -> EntityT | None:
        raise NotImplementedError

    async def list(self, *, limit: int = 100, offset: int = 0) -> list[EntityT]:
        _ = (limit, offset)
        raise NotImplementedError

    async def create(self, payload: Mapping[str, Any]) -> EntityT:
        _ = payload
        raise NotImplementedError

    async def update(self, _entity_id: str, payload: Mapping[str, Any]) -> EntityT:
        _ = (_entity_id, payload)
        raise NotImplementedError

    async def delete(self, _entity_id: str) -> None:
        _ = _entity_id
        raise NotImplementedError
