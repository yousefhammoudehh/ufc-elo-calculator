from dataclasses import dataclass, fields
from datetime import datetime
from typing import Any, Dict, Type, TypeVar
from uuid import UUID

T = TypeVar('T', bound='BaseEntity')


@dataclass
class BaseEntity:
    id: UUID
    created_at: datetime
    updated_at: datetime
    created_by: UUID
    updated_by: UUID

    @classmethod
    def from_dict(cls: Type[T], data: Dict[str, Any]) -> T:
        """
        Convert a dictionary to an instance of the class.
        Recursively handles nested data classes.
        """
        field_types = {f.name: f.type for f in fields(cls)}
        instance_data = {}
        for field, field_type in field_types.items():
            field_data = data.get(field, None)
            if hasattr(field_type, 'from_dict'):
                instance_data[field] = field_type.from_dict(field_data or {})
            else:
                instance_data[field] = field_data

        return cls(**instance_data)

    def to_dict(self) -> dict[str, Any]:
        """
        Convert the current object to a dictionary and print out its fields.
        """
        model_data = {}
        for cls in self.__class__.mro():
            if hasattr(cls, '__annotations__'):
                field_types = {f.name: f.type for f in fields(cls)}
                for field, field_type in field_types.items():
                    field_val = getattr(self, field)
                    if field_val and hasattr(field_type, 'to_dict'):
                        model_data[field] = field_val.to_dict()
                    else:
                        model_data[field] = getattr(self, field)
        return model_data
