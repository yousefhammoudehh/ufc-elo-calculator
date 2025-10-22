from typing import Any

from pydantic import BaseModel

from elo_calculator.domain.entities.base_entity import BaseEntityBase


class DataModel(BaseModel):
    @classmethod
    def from_entity[R: DataModel, E: BaseEntityBase](cls: type[R], entity: E, exclude: list[str] | None = None) -> R:
        return cls(**entity.to_dict(exclude))

    @classmethod
    def from_entity_list[R: DataModel, E: BaseEntityBase](
        cls: type[R], entities: list[E], exclude: list[str] | None = None
    ) -> list[R]:
        return [cls.from_entity(entity, exclude) for entity in entities]


class MainResponse[T](BaseModel):
    message: str
    data: T | list[T]
    errors: list[dict[str, Any]] | dict[str, Any] | list[str] | str | None
