from collections.abc import Sequence
from typing import Any, TypeVar

from pydantic import BaseModel

from elo_calculator.domain.entities.base_entity import BaseEntityBase

R = TypeVar('R', bound='DataModel')
E = TypeVar('E', bound=BaseEntityBase)
T = TypeVar('T')


class DataModel(BaseModel):
    @classmethod
    def from_entity(cls: type[R], entity: E, exclude: Sequence[str] | None = None) -> R:
        return cls(**entity.to_dict(list(exclude) if exclude is not None else None))

    @classmethod
    def from_entity_list(cls: type[R], entities: Sequence[E], exclude: Sequence[str] | None = None) -> list[R]:
        return [cls.from_entity(entity, exclude) for entity in entities]


class MainResponse[T](BaseModel):
    message: str
    data: T | list[T]
    errors: list[dict[str, Any]] | dict[str, Any] | list[str] | str | None
