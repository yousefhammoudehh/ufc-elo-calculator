from uuid import UUID
from typing import Any

from elo_calculator.application.base_service import BaseService
from elo_calculator.domain.entities import (
    Fighter,
    Bout,
    Event,
    BoutParticipant,
    JudgeScore,
    PreUfcBout,
    Promotion,
)
from elo_calculator.errors.app_exceptions import DataNotFoundException
from elo_calculator.infrastructure.repositories.unit_of_work import UnitOfWork, with_uow


class FighterService(BaseService):
    @with_uow
    async def get_all(self, uow: UnitOfWork) -> list[Fighter]:
        return await uow.fighters.get_all()

    @with_uow
    async def get(self, uow: UnitOfWork, fighter_id: UUID) -> Fighter:
        fighter = await uow.fighters.get_by_id(fighter_id)
        if not fighter:
            raise DataNotFoundException(f'Fighter id:{fighter_id} not found')
        return fighter

    @with_uow
    async def get_by_fighter_id(self, uow: UnitOfWork, fighter_id: str) -> Fighter | None:
        return await uow.fighters.get_by_fighter_id(fighter_id)

    @with_uow
    async def create(self, uow: UnitOfWork, fighter: Fighter) -> Fighter:
        created = await uow.fighters.add(fighter)
        return created

    @with_uow
    async def update(self, uow: UnitOfWork, fighter_id: UUID, data: dict[str, Any]) -> Fighter:
        existing = await uow.fighters.get_by_id(fighter_id)
        if not existing:
            raise DataNotFoundException(f'Fighter id:{fighter_id} not found')
        updated = await uow.fighters.update(fighter_id, data)
        return updated

    @with_uow
    async def delete(self, uow: UnitOfWork, fighter_id: UUID) -> Fighter:
        existing = await uow.fighters.get_by_id(fighter_id)
        if not existing:
            raise DataNotFoundException(f'Fighter id:{fighter_id} not found')
        return await uow.fighters.delete(fighter_id)


class BoutService(BaseService):
    @with_uow
    async def get(self, uow: UnitOfWork, bout_id: UUID) -> Bout:
        bout = await uow.bouts.get_by_id(bout_id)
        if not bout:
            raise DataNotFoundException(f'Bout id:{bout_id} not found')
        return bout

    @with_uow
    async def get_by_bout_id(self, uow: UnitOfWork, bout_id: str) -> Bout | None:
        return await uow.bouts.get_by_bout_id(bout_id)

    @with_uow
    async def list(self, uow: UnitOfWork) -> list[Bout]:
        return await uow.bouts.get_all()

    @with_uow
    async def create(self, uow: UnitOfWork, bout: Bout) -> Bout:
        return await uow.bouts.add(bout)

    @with_uow
    async def update(self, uow: UnitOfWork, bout_id: UUID, data: dict[str, Any]) -> Bout:
        existing = await uow.bouts.get_by_id(bout_id)
        if not existing:
            raise DataNotFoundException(f'Bout id:{bout_id} not found')
        return await uow.bouts.update(bout_id, data)

    @with_uow
    async def delete(self, uow: UnitOfWork, bout_id: UUID) -> Bout:
        existing = await uow.bouts.get_by_id(bout_id)
        if not existing:
            raise DataNotFoundException(f'Bout id:{bout_id} not found')
        return await uow.bouts.delete(bout_id)


class EventService(BaseService):
    @with_uow
    async def get(self, uow: UnitOfWork, event_id: UUID) -> Event:
        event = await uow.events.get_by_id(event_id)
        if not event:
            raise DataNotFoundException(f'Event id:{event_id} not found')
        return event

    @with_uow
    async def list(self, uow: UnitOfWork) -> list[Event]:
        return await uow.events.get_all()

    @with_uow
    async def create(self, uow: UnitOfWork, event: Event) -> Event:
        return await uow.events.add(event)

    @with_uow
    async def update(self, uow: UnitOfWork, event_id: UUID, data: dict[str, Any]) -> Event:
        existing = await uow.events.get_by_id(event_id)
        if not existing:
            raise DataNotFoundException(f'Event id:{event_id} not found')
        return await uow.events.update(event_id, data)

    @with_uow
    async def delete(self, uow: UnitOfWork, event_id: UUID) -> Event:
        existing = await uow.events.get_by_id(event_id)
        if not existing:
            raise DataNotFoundException(f'Event id:{event_id} not found')
        return await uow.events.delete(event_id)


class BoutParticipantService(BaseService):
    @with_uow
    async def list_by_bout(self, uow: UnitOfWork, bout_id: str) -> list[BoutParticipant]:
        return await uow.bout_participants.get_by_bout_id(bout_id)

    @with_uow
    async def list_by_fighter(self, uow: UnitOfWork, fighter_id: str) -> list[BoutParticipant]:
        return await uow.bout_participants.get_by_fighter_id(fighter_id)

    @with_uow
    async def get(self, uow: UnitOfWork, bout_id: str, fighter_id: str) -> BoutParticipant | None:
        return await uow.bout_participants.get_by_bout_and_fighter(bout_id, fighter_id)

    @with_uow
    async def record(self, uow: UnitOfWork, fighter_id: str) -> dict[str, int]:
        return await uow.bout_participants.get_fighter_record(fighter_id)


class JudgeScoreService(BaseService):
    @with_uow
    async def list_by_bout(self, uow: UnitOfWork, bout_id: str) -> list[JudgeScore]:
        return await uow.judge_scores.get_by_bout_id(bout_id)

    @with_uow
    async def list_by_fighter(self, uow: UnitOfWork, fighter_id: str) -> list[JudgeScore]:
        return await uow.judge_scores.get_by_fighter_id(fighter_id)

    @with_uow
    async def get(self, uow: UnitOfWork, bout_id: str, fighter_id: str) -> JudgeScore | None:
        return await uow.judge_scores.get_by_bout_and_fighter(bout_id, fighter_id)

    @with_uow
    async def total(self, uow: UnitOfWork, bout_id: str, fighter_id: str) -> int | None:
        return await uow.judge_scores.calculate_total_score(bout_id, fighter_id)


class PreUfcBoutService(BaseService):
    @with_uow
    async def list_by_fighter(self, uow: UnitOfWork, fighter_id: str) -> list[PreUfcBout]:
        return await uow.pre_ufc_bouts.get_by_fighter_id(fighter_id)

    @with_uow
    async def list_by_promotion(self, uow: UnitOfWork, promotion_id: UUID) -> list[PreUfcBout]:
        return await uow.pre_ufc_bouts.get_by_promotion(promotion_id)

    @with_uow
    async def list_by_fighter_and_promotion(
        self, uow: UnitOfWork, fighter_id: str, promotion_id: UUID
    ) -> list[PreUfcBout]:
        return await uow.pre_ufc_bouts.get_by_fighter_and_promotion(fighter_id, promotion_id)

    @with_uow
    async def record(self, uow: UnitOfWork, fighter_id: str) -> dict[str, int]:
        return await uow.pre_ufc_bouts.get_fighter_pre_ufc_record(fighter_id)


class PromotionService(BaseService):
    @with_uow
    async def list(self, uow: UnitOfWork) -> list[Promotion]:
        return await uow.promotions.get_all()

    @with_uow
    async def get(self, uow: UnitOfWork, promotion_id: UUID) -> Promotion:
        promotion = await uow.promotions.get_by_id(promotion_id)
        if not promotion:
            raise DataNotFoundException(f'Promotion id:{promotion_id} not found')
        return promotion

    @with_uow
    async def create(self, uow: UnitOfWork, promotion: Promotion) -> Promotion:
        return await uow.promotions.add(promotion)

    @with_uow
    async def update(self, uow: UnitOfWork, promotion_id: UUID, data: dict[str, Any]) -> Promotion:
        existing = await uow.promotions.get_by_id(promotion_id)
        if not existing:
            raise DataNotFoundException(f'Promotion id:{promotion_id} not found')
        return await uow.promotions.update(promotion_id, data)

    @with_uow
    async def delete(self, uow: UnitOfWork, promotion_id: UUID) -> Promotion:
        existing = await uow.promotions.get_by_id(promotion_id)
        if not existing:
            raise DataNotFoundException(f'Promotion id:{promotion_id} not found')
        return await uow.promotions.delete(promotion_id)
