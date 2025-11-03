from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, cast

from elo_calculator.application.base_service import BaseService
from elo_calculator.application.services.elo_calculator import EntryEloParams, PromotionsRepoProtocol, compute_entry_elo
from elo_calculator.domain.entities import Fighter, PreUfcBout, Promotion
from elo_calculator.infrastructure.repositories.unit_of_work import UnitOfWork, with_uow


@dataclass
class _SimpleBout:
    result: Any
    promotion: str


CLAMP_MIN_STRENGTH = 0.66
CLAMP_MAX_STRENGTH = 0.74


class _MapPromotionsRepo(PromotionsRepoProtocol):
    def __init__(self, link_to_strength: dict[str, float], default_strength: float):
        self._map = {k or '': float(v) for k, v in link_to_strength.items()}
        self._default = float(default_strength)

    def get_strength_by_link(self, link: str, default: float) -> float:  # pragma: no cover - thin adapter
        val = self._map.get(link)
        if val is None:
            # Clamp unknowns to [CLAMP_MIN_STRENGTH, CLAMP_MAX_STRENGTH]
            return max(
                CLAMP_MIN_STRENGTH,
                min(CLAMP_MAX_STRENGTH, self._default if self._default is not None else float(default)),
            )
        return val


class EntryEloService(BaseService):
    """Service to recalculate fighters' entry/current/peak ELOs from pre-UFC bouts.

    Behavior:
    - If a fighter has no pre-UFC bouts, seed to 1500.
    - Otherwise, compute weighted entry ELO using promotion strengths.
    - Reset entry_elo, current_elo, peak_elo to the computed seed.
    """

    @with_uow
    async def reseed_all(
        self, uow: UnitOfWork, *, default_strength: float | None = None, dry_run: bool = False
    ) -> dict[str, Any]:
        # 1) Load base data
        fighters: list[Fighter] = await uow.fighters.get_all(sort_by='fighter_id', order='asc')
        promos: list[Promotion] = await uow.promotions.get_all()
        pre_bouts: list[PreUfcBout] = await uow.pre_ufc_bouts.get_all()

        # 2) Build lookups
        id_to_link: dict[str, str] = {}
        link_to_strength: dict[str, float] = {}
        for p in promos:
            link = (p.link or '').strip()
            # strength is Numeric -> may be Decimal; cast to float when present
            s = float(p.strength) if p.strength is not None else None
            if link:
                id_to_link[str(p.promotion_id)] = link
                if s is not None:
                    link_to_strength[link] = s

        fighter_to_pre: dict[str, list[_SimpleBout]] = {}
        for b in pre_bouts:
            fid = b.fighter_id or ''
            if not fid:
                continue
            plink = id_to_link.get(str(b.promotion_id) if b.promotion_id is not None else '', '')
            fighter_to_pre.setdefault(fid, []).append(_SimpleBout(result=b.result, promotion=plink))

        # 3) Prepare promotions repo adapter and params
        ds = default_strength if default_strength is not None else 0.4
        repo_adapter = _MapPromotionsRepo(link_to_strength, ds)
        params = EntryEloParams(default_strength=ds)

        # 4) Iterate and compute seeds
        updated = 0
        defaulted = 0
        changes: list[dict[str, Any]] = []
        for f in fighters:
            bouts: Iterable[_SimpleBout] = fighter_to_pre.get(f.fighter_id, [])
            # compute_entry_elo returns (seed, weff, neff)
            seed, _weff, neff = compute_entry_elo(bouts, promotions_repo=repo_adapter, params=params)
            # If no effective bouts, we default to 1500 by definition of weff/neff path
            if not bouts or neff <= 0:
                defaulted += 1
                seed = 1500.0

            # Record change info
            changes.append(
                {'fighter_id': f.fighter_id, 'name': f.name, 'old_entry': f.entry_elo, 'new_entry': float(seed)}
            )

            if dry_run:
                continue

            # Reset entry/current/peak to seed via BaseRepository.update (fighters pk is fighter_id)
            await uow.fighters.update(
                cast(Any, f.fighter_id), {'entry_elo': float(seed), 'current_elo': float(seed), 'peak_elo': float(seed)}
            )
            updated += 1

        return {
            'total_fighters': len(fighters),
            'updated': updated,
            'defaulted_to_1500': defaulted,
            'dry_run': dry_run,
            'params': {'default_strength': ds},
            'sample': changes[:20],  # include a small sample for visibility
        }
