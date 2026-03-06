"""Event service — event listing and detail with fight cards."""

from elo_calculator.application.base_service import BaseService
from elo_calculator.domain.entities.bout import BoutSummary
from elo_calculator.domain.entities.event import Event
from elo_calculator.infrastructure.repositories.bout_participant_repository import BoutParticipantRepository
from elo_calculator.infrastructure.repositories.bout_repository import BoutRepository
from elo_calculator.infrastructure.repositories.event_repository import EventRepository
from elo_calculator.infrastructure.repositories.unit_of_work import UnitOfWork, with_uow


class EventService(BaseService):
    """Read-only service for event data."""

    @with_uow
    async def list_events(self, uow: UnitOfWork, *, limit: int = 25, offset: int = 0) -> tuple[list[Event], int]:
        """Return a paginated list of events (most recent first)."""
        repo = EventRepository(uow.connection)
        events = await repo.list(limit=limit, offset=offset)
        total = await repo.count()
        return events, total

    @with_uow
    async def get_event_detail(self, uow: UnitOfWork, *, event_id: str) -> tuple[Event | None, list[BoutSummary]]:
        """Return event info + full fight card with participants."""
        event_repo = EventRepository(uow.connection)
        bout_repo = BoutRepository(uow.connection)
        participant_repo = BoutParticipantRepository(uow.connection)

        event = await event_repo.get(event_id)
        if event is None:
            return None, []

        bout_rows = await bout_repo.list_by_event(event_id)
        bout_ids = [b['bout_id'] for b in bout_rows]
        participants_map = await participant_repo.get_by_bouts(bout_ids) if bout_ids else {}

        bouts: list[BoutSummary] = []
        for row in bout_rows:
            bid = row['bout_id']
            bouts.append(
                BoutSummary(
                    id=bid,
                    bout_id=bid,
                    event_id=row['event_id'],
                    event_date=row['event_date'],
                    event_name=row['event_name'],
                    division_key=row.get('division_key'),
                    weight_class_raw=row.get('weight_class_raw'),
                    is_title_fight=bool(row.get('is_title_fight', False)),
                    method_group=row.get('method_group'),
                    decision_type=row.get('decision_type'),
                    finish_round=int(row['finish_round']) if row.get('finish_round') is not None else None,
                    finish_time_seconds=(
                        int(row['finish_time_seconds']) if row.get('finish_time_seconds') is not None else None
                    ),
                    participants=participants_map.get(bid, []),
                )
            )
        return event, bouts
