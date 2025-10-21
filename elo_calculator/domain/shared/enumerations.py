from enum import Enum


class StrEnum(str, Enum):
    """String-valued Enum base class.

    Inherits from str and Enum so members compare and serialize as plain strings.
    """

    def __str__(self) -> str:  # pragma: no cover - trivial
        return str(self.value)
