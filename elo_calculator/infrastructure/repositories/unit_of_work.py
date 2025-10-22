from collections.abc import Awaitable, Callable
from typing import Concatenate, ParamSpec, Self, TypeVar

from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from elo_calculator.application.base_service import BaseService
from elo_calculator.infrastructure.database.engine import engine
from elo_calculator.infrastructure.repositories.bout_participant_repository import BoutParticipantRepository
from elo_calculator.infrastructure.repositories.bout_repository import BoutRepository
from elo_calculator.infrastructure.repositories.event_repository import EventRepository
from elo_calculator.infrastructure.repositories.fighter_repository import FighterRepository
from elo_calculator.infrastructure.repositories.judge_score_repository import JudgeScoreRepository
from elo_calculator.infrastructure.repositories.pre_ufc_bout_repository import PreUfcBoutRepository
from elo_calculator.infrastructure.repositories.promotion_repository import PromotionRepository


class UnitOfWork:
    def __init__(self, engine: AsyncEngine):
        self.engine: AsyncEngine = engine
        self.connection: AsyncConnection
        # Lazily initialized repositories
        self._fighters: FighterRepository | None = None
        self._bouts: BoutRepository | None = None
        self._events: EventRepository | None = None
        self._bout_participants: BoutParticipantRepository | None = None
        self._judge_scores: JudgeScoreRepository | None = None
        self._pre_ufc_bouts: PreUfcBoutRepository | None = None
        self._promotions: PromotionRepository | None = None

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
        if self._fighters is None:
            self._fighters = FighterRepository(self.connection)
        return self._fighters

    @property
    def bouts(self) -> BoutRepository:
        if self._bouts is None:
            self._bouts = BoutRepository(self.connection)
        return self._bouts

    @property
    def events(self) -> EventRepository:
        if self._events is None:
            self._events = EventRepository(self.connection)
        return self._events

    @property
    def bout_participants(self) -> BoutParticipantRepository:
        if self._bout_participants is None:
            self._bout_participants = BoutParticipantRepository(self.connection)
        return self._bout_participants

    @property
    def judge_scores(self) -> JudgeScoreRepository:
        if self._judge_scores is None:
            self._judge_scores = JudgeScoreRepository(self.connection)
        return self._judge_scores

    @property
    def pre_ufc_bouts(self) -> PreUfcBoutRepository:
        if self._pre_ufc_bouts is None:
            self._pre_ufc_bouts = PreUfcBoutRepository(self.connection)
        return self._pre_ufc_bouts

    @property
    def promotions(self) -> PromotionRepository:
        if self._promotions is None:
            self._promotions = PromotionRepository(self.connection)
        return self._promotions

    async def commit(self) -> None:
        await self.connection.commit()

    async def rollback(self) -> None:
        await self.connection.rollback()


T = TypeVar('T', bound=BaseService)
P = ParamSpec('P')
R = TypeVar('R')


def with_uow(  # noqa: UP047 - keep ParamSpec/Concatenate for mypy/py311 compatibility instead of PEP 695 type params
    func: Callable[Concatenate[T, UnitOfWork, P], Awaitable[R]],
) -> Callable[Concatenate[T, P], Awaitable[R]]:
    """Provide a UnitOfWork instance to service methods transparently.

    Methods should be declared as: async def method(self, uow: UnitOfWork, *args) -> R
    Callers invoke: await service.method(*args)  # uow injected by decorator
    """

    async def wrapper(self: T, /, *args: P.args, **kwargs: P.kwargs) -> R:
        async with UnitOfWork(engine) as uow:
            return await func(self, uow, *args, **kwargs)

    return wrapper
