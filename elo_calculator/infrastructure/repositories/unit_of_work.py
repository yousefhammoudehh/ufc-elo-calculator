from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Self

from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from functools import wraps

from elo_calculator.infrastructure.database.engine import engine
from elo_calculator.infrastructure.repositories.fighter_repository import FighterRepository
from elo_calculator.infrastructure.repositories.bout_repository import BoutRepository
from elo_calculator.infrastructure.repositories.event_repository import EventRepository
from elo_calculator.infrastructure.repositories.bout_participant_repository import BoutParticipantRepository
from elo_calculator.infrastructure.repositories.judge_score_repository import JudgeScoreRepository
from elo_calculator.infrastructure.repositories.pre_ufc_bout_repository import PreUfcBoutRepository
from elo_calculator.infrastructure.repositories.promotion_repository import PromotionRepository


class UnitOfWork:
    def __init__(self, engine: AsyncEngine):
        self.engine: AsyncEngine = engine
        self.connection: AsyncConnection

    async def __aenter__(self) -> Self:
        self.connection = await self.engine.connect()
        await self.connection.begin()
        return self

    async def __aexit__(self, exc_type: type | None, exc_val: Exception | None, exc_tb: object | None) -> None:
        if exc_type:
            await self.rollback()
        else:
            await self.commit()
        await self.connection.close()

    # Lazy-loaded repository properties
    @property
    def fighters(self) -> FighterRepository:
        if not hasattr(self, '_fighters'):
            self._fighters = FighterRepository(self.connection)
        return self._fighters  # type: ignore[attr-defined]

    @property
    def bouts(self) -> BoutRepository:
        if not hasattr(self, '_bouts'):
            self._bouts = BoutRepository(self.connection)
        return self._bouts  # type: ignore[attr-defined]

    @property
    def events(self) -> EventRepository:
        if not hasattr(self, '_events'):
            self._events = EventRepository(self.connection)
        return self._events  # type: ignore[attr-defined]

    @property
    def bout_participants(self) -> BoutParticipantRepository:
        if not hasattr(self, '_bout_participants'):
            self._bout_participants = BoutParticipantRepository(self.connection)
        return self._bout_participants  # type: ignore[attr-defined]

    @property
    def judge_scores(self) -> JudgeScoreRepository:
        if not hasattr(self, '_judge_scores'):
            self._judge_scores = JudgeScoreRepository(self.connection)
        return self._judge_scores  # type: ignore[attr-defined]

    @property
    def pre_ufc_bouts(self) -> PreUfcBoutRepository:
        if not hasattr(self, '_pre_ufc_bouts'):
            self._pre_ufc_bouts = PreUfcBoutRepository(self.connection)
        return self._pre_ufc_bouts  # type: ignore[attr-defined]

    @property
    def promotions(self) -> PromotionRepository:
        if not hasattr(self, '_promotions'):
            self._promotions = PromotionRepository(self.connection)
        return self._promotions  # type: ignore[attr-defined]

    async def commit(self) -> None:
        await self.connection.commit()

    async def rollback(self) -> None:
        await self.connection.rollback()


@asynccontextmanager
async def get_uow() -> AsyncGenerator[UnitOfWork]:
    async with UnitOfWork(engine) as uow:
        yield uow


def with_uow(func):
    """Decorator to provide a UnitOfWork instance to service methods transparently."""

    @wraps(func)
    async def wrapper(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        async with UnitOfWork(engine) as uow:
            return await func(self, uow, *args, **kwargs)

    return wrapper
