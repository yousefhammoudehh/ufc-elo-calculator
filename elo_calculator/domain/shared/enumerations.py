from enum import Enum


class StrEnum(str, Enum):
    """String-valued Enum base class.

    Inherits from str and Enum so members compare and serialize as plain strings.
    """

    def __str__(self) -> str:  # pragma: no cover - trivial
        return str(self.value)


class BoutResult(StrEnum):
    """Bout result types for pre-UFC fights."""

    WIN = 'win'
    LOSS = 'loss'
    DRAW = 'draw'
    NO_CONTEST = 'nc'
