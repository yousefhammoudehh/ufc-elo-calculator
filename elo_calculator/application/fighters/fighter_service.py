"""Fighter service — profile, search, bout history, timeseries."""

from elo_calculator.application.base_service import BaseService
from elo_calculator.domain.entities.bout import BoutParticipant, BoutSummary
from elo_calculator.domain.entities.fighter import Fighter
from elo_calculator.domain.entities.rating import FighterRatingProfile, RatingTimeseriesPoint
from elo_calculator.infrastructure.repositories.bout_participant_repository import BoutParticipantRepository
from elo_calculator.infrastructure.repositories.bout_repository import BoutRepository
from elo_calculator.infrastructure.repositories.fighter_repository import FighterRepository
from elo_calculator.infrastructure.repositories.fighter_timeseries_repository import FighterTimeseriesRepository
from elo_calculator.infrastructure.repositories.rating_snapshot_repository import RatingSnapshotRepository
from elo_calculator.infrastructure.repositories.unit_of_work import UnitOfWork, with_uow


class FighterService(BaseService):
    """Read-only service for fighter data."""

    @with_uow
    async def search_fighters(
        self, uow: UnitOfWork, *, query: str = '', limit: int = 25, offset: int = 0
    ) -> tuple[list[Fighter], int]:
        """Search fighters by name or list all (paginated)."""
        repo = FighterRepository(uow.connection)
        if query:
            fighters = await repo.search(query, limit=limit, offset=offset)
            total = await repo.count_search(query)
        else:
            fighters = await repo.list(limit=limit, offset=offset)
            total = await repo.count()
        return fighters, total

    @with_uow
    async def get_fighter_profile(
        self, uow: UnitOfWork, *, fighter_id: str
    ) -> tuple[Fighter | None, list[FighterRatingProfile]]:
        """Return fighter bio + current ratings across all systems."""
        fighter_repo = FighterRepository(uow.connection)
        snapshot_repo = RatingSnapshotRepository(uow.connection)

        fighter = await fighter_repo.get(fighter_id)
        if fighter is None:
            return None, []

        profiles = await snapshot_repo.get_latest_by_fighter(fighter_id)
        peaks = await snapshot_repo.get_peak_by_fighter(fighter_id)

        # Merge peak into each profile
        for p in profiles:
            p.peak_rating = peaks.get(p.system_key, 0.0)

        return fighter, profiles

    @with_uow
    async def get_fighter_bouts(
        self, uow: UnitOfWork, *, fighter_id: str, limit: int = 25, offset: int = 0
    ) -> tuple[list[BoutSummary], int]:
        """Return paginated bout history for a fighter."""
        bout_repo = BoutRepository(uow.connection)
        participant_repo = BoutParticipantRepository(uow.connection)

        bout_rows = await bout_repo.list_by_fighter(fighter_id, limit=limit, offset=offset)
        total = await bout_repo.count_by_fighter(fighter_id)

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
        return bouts, total

    @with_uow
    async def get_fighter_timeseries(
        self,
        uow: UnitOfWork,
        *,
        fighter_id: str,
        system_key: str = 'unified_composite_elo',
        limit: int = 500,
        offset: int = 0,
    ) -> tuple[list[RatingTimeseriesPoint], int]:
        """Return a paginated rating timeseries for one system."""
        ts_repo = FighterTimeseriesRepository(uow.connection)
        points = await ts_repo.get_timeseries(fighter_id, system_key, limit=limit, offset=offset)
        total = await ts_repo.count(fighter_id, system_key)
        return points, total

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _row_to_participant(r: dict[str, object]) -> BoutParticipant:  # noqa: ARG004 reserved
        return BoutParticipant()
