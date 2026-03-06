"""Reference-data router — divisions and rating systems."""

from fastapi import APIRouter

from elo_calculator.application.base_service import BaseService
from elo_calculator.infrastructure.repositories.division_repository import DivisionRepository
from elo_calculator.infrastructure.repositories.rating_system_repository import RatingSystemRepository
from elo_calculator.infrastructure.repositories.unit_of_work import UnitOfWork, with_uow
from elo_calculator.presentation.models.reference_models import (
    DivisionListResponse,
    DivisionResponse,
    RatingSystemResponse,
    SystemListResponse,
)

reference_router = APIRouter(prefix='/api/v1', tags=['reference'])


class _ReferenceService(BaseService):
    """Thin service for reference data lookups."""

    @with_uow
    async def list_divisions(self, uow: UnitOfWork) -> list[DivisionResponse]:
        repo = DivisionRepository(uow.connection)
        divisions = await repo.list_canonical_mma()
        return [
            DivisionResponse(
                division_id=d.division_id,
                division_key=d.division_key,
                display_name=d.display_name,
                sex=d.sex,
                limit_lbs=d.limit_lbs,
                is_canonical_mma=d.is_canonical_mma,
            )
            for d in divisions
        ]

    @with_uow
    async def list_systems(self, uow: UnitOfWork) -> list[RatingSystemResponse]:
        repo = RatingSystemRepository(uow.connection)
        systems = await repo.list()
        return [
            RatingSystemResponse(system_id=s.system_id, system_key=s.system_key, description=s.description)
            for s in systems
        ]


_service = _ReferenceService()


@reference_router.get('/divisions', response_model=DivisionListResponse)
async def list_divisions() -> DivisionListResponse:
    """List canonical MMA divisions."""
    data = await _service.list_divisions()
    return DivisionListResponse(data=data)


@reference_router.get('/systems', response_model=SystemListResponse)
async def list_systems() -> SystemListResponse:
    """List available rating systems."""
    data = await _service.list_systems()
    return SystemListResponse(data=data)
