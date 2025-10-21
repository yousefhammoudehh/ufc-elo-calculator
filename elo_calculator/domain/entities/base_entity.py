# ruff: noqa: PLR0911, PLR0912
import uuid
from dataclasses import dataclass, fields
from datetime import date, datetime, timedelta
from enum import Enum
from types import UnionType
from typing import Any, ClassVar, TypeVar, Union, get_args, get_origin
from uuid import UUID

from elo_calculator.utils.date_parser import date_to_iso_str, datetime_to_iso_str, str_to_timedelta

T = TypeVar('T', bound='BaseEntityBase')


def get_field_value(field_type: type[Any] | UnionType | Any, field_data: Any) -> Any:
    if field_data is None:
        return None

    if field_type is UUID or (isinstance(field_type, type) and issubclass(field_type, uuid.UUID)):
        return UUID(field_data) if isinstance(field_data, str) else field_data

    if field_type is datetime or (isinstance(field_type, type) and issubclass(field_type, datetime)):
        if isinstance(field_data, str):
            return datetime.fromisoformat(field_data)
        if isinstance(field_data, datetime):
            return field_data

    if field_type is date or (isinstance(field_type, type) and issubclass(field_type, date)):
        if isinstance(field_data, str):
            return date.fromisoformat(field_data)
        if isinstance(field_data, date):
            return field_data

    if field_type is timedelta or (isinstance(field_type, type) and issubclass(field_type, timedelta)):
        if isinstance(field_data, str):
            return str_to_timedelta(field_data)
        if isinstance(field_data, timedelta):
            return field_data

    if field_type is Enum or (isinstance(field_type, type) and issubclass(field_type, Enum)):
        if isinstance(field_data, str):
            return field_type(field_data)  # type: ignore
        if isinstance(field_data, Enum):
            return field_data

    if isinstance(field_type, type) and issubclass(field_type, BaseEntityBase):
        return field_type.from_dict(field_data)

    origin = get_origin(field_type)
    if origin is Union or origin is UnionType:
        for subtype in get_args(field_type):
            if subtype is type(None):
                continue
            try:
                return get_field_value(subtype, field_data)
            except (ValueError, TypeError):
                continue

        if type(None) in get_args(field_type):
            return None
        raise ValueError(f'Could not parse {field_data} into any of {get_args(field_type)}')

    if origin is list:
        list_args = get_args(field_type)
        if list_args:
            subtype = list_args[0]
            converted_items = []
            for item in field_data:
                converted_item = get_field_value(subtype, item)
                converted_items.append(converted_item)
            return converted_items
    return field_data


def get_attr_value(attr_val: Any, map_primitive: bool = True) -> Any:
    if attr_val is None:
        return None

    if isinstance(attr_val, BaseEntityBase):
        return attr_val.to_dict()

    if isinstance(attr_val, list):
        return [get_attr_value(item) for item in attr_val]

    if not map_primitive:
        return attr_val

    if isinstance(attr_val, UUID):
        return str(attr_val)

    if isinstance(attr_val, datetime):
        return datetime_to_iso_str(attr_val)

    if isinstance(attr_val, date):
        return date_to_iso_str(attr_val)

    if isinstance(attr_val, Enum):
        return attr_val.value

    return attr_val


@dataclass
class BaseEntityBase:
    @classmethod
    def from_dict(cls: type[T], data: dict[str, Any], exclude: list[str] | None = None) -> T:
        """
        Convert a dictionary to an instance of the class.
        Recursively handles nested data classes and lists of data classes.
        """
        excluded_fields = list(cls.config.from_dict_excluded_fields)
        if exclude:
            excluded_fields = excluded_fields + exclude

        instance_data = {}
        entity_fields = {f.name: f.type for f in fields(cls)}
        for field_name, field_type in entity_fields.items():
            field_data = None
            if field_name not in excluded_fields:
                field_data = data.get(field_name)
            instance_data[field_name] = get_field_value(field_type, field_data)

        return cls(**instance_data)

    def to_dict(self, exclude: list[str] | None = None, map_primitive: bool = True) -> dict[str, Any]:
        """
        Convert the current object to a dictionary and handle nested dataclasses.
        Recursively converts all nested dataclasses to dictionaries.
        """
        excluded_fields = list(self.config.to_dict_excluded_fields)
        if exclude:
            excluded_fields = excluded_fields + exclude

        data: dict[str, Any] = {}
        for cls in self.__class__.mro():
            if not hasattr(cls, '__annotations__'):
                continue
            entity_fields = [f.name for f in fields(cls)]
            for field_name in entity_fields:
                if field_name not in excluded_fields:
                    data[field_name] = get_attr_value(getattr(self, field_name, None), map_primitive)

        return data

    def update_from_dict(self, data: dict[str, Any]) -> None:
        entity_fields = {f.name: f.type for f in fields(self)}
        for field_name, field_type in entity_fields.items():
            if field_name not in data:
                continue

            value = data[field_name]
            if not value:
                setattr(self, field_name, value)
                continue
            self._set_field_value(field_name, field_type, value)

    def _set_field_value(self, field_name: str, field_type: type[Any] | str | Any, data: Any) -> None:
        if isinstance(field_type, type) and issubclass(field_type, BaseEntityBase):
            obj = getattr(self, field_name, None)
            if obj:
                obj.update_from_dict(data)
            else:
                setattr(self, field_name, field_type.from_dict(data))
            return
        origin = getattr(field_type, '__origin__', None)
        if origin is list and isinstance(data, list):
            args = getattr(field_type, '__args__', [])
            if args and isinstance(args[0], type) and issubclass(args[0], BaseEntityBase):
                setattr(self, field_name, [args[0].from_dict(item) for item in data])

        setattr(self, field_name, data)

    class config:  # noqa: N801
        db_excluded_fields: ClassVar[tuple[str, ...]] = ()
        to_dict_excluded_fields: ClassVar[tuple[str, ...]] = ()
        from_dict_excluded_fields: ClassVar[tuple[str, ...]] = ()


@dataclass
class BaseEntity(BaseEntityBase):
    id: UUID
    created_at: datetime
    updated_at: datetime
