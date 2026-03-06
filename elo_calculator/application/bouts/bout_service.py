"""Bout service — full bout detail with stats, rating changes & PS."""

from elo_calculator.application.base_service import BaseService
from elo_calculator.domain.entities.bout import BoutDetail
from elo_calculator.infrastructure.repositories.bout_participant_repository import BoutParticipantRepository
from elo_calculator.infrastructure.repositories.bout_repository import BoutRepository
from elo_calculator.infrastructure.repositories.fight_ps_repository import FightPSRepository
from elo_calculator.infrastructure.repositories.fight_totals_repository import FightTotalsRepository
from elo_calculator.infrastructure.repositories.rating_delta_repository import RatingDeltaRepository
from elo_calculator.infrastructure.repositories.unit_of_work import UnitOfWork, with_uow


class BoutService(BaseService):
    """Read-only service for individual bout details."""

    @with_uow
    async def get_bout_detail(self, uow: UnitOfWork, *, bout_id: str) -> BoutDetail | None:
        """Return full bout detail with stats, rating deltas & PS."""
        bout_repo = BoutRepository(uow.connection)
        participant_repo = BoutParticipantRepository(uow.connection)
        totals_repo = FightTotalsRepository(uow.connection)
        ps_repo = FightPSRepository(uow.connection)
        delta_repo = RatingDeltaRepository(uow.connection)

        row = await bout_repo.get(bout_id)
        if row is None:
            return None

        participants = await participant_repo.get_by_bout(bout_id)
        stats = await totals_repo.get_by_bout(bout_id)
        deltas = await delta_repo.get_by_bout(bout_id)
        ps_scores = await ps_repo.get_by_bout(bout_id)

        return BoutDetail(
            id=row['bout_id'],
            bout_id=row['bout_id'],
            event_id=row['event_id'],
            event_date=row['event_date'],
            event_name=row['event_name'],
            sport_key=row.get('sport_key', ''),
            division_key=row.get('division_key'),
            weight_class_raw=row.get('weight_class_raw'),
            is_title_fight=bool(row.get('is_title_fight', False)),
            method_group=row.get('method_group'),
            decision_type=row.get('decision_type'),
            finish_round=int(row['finish_round']) if row.get('finish_round') is not None else None,
            finish_time_seconds=(
                int(row['finish_time_seconds']) if row.get('finish_time_seconds') is not None else None
            ),
            scheduled_rounds=int(row['scheduled_rounds']) if row.get('scheduled_rounds') is not None else None,
            referee=row.get('referee'),
            participants=participants,
            fight_stats=stats,
            rating_changes=deltas,
            performance_scores=ps_scores,
        )
