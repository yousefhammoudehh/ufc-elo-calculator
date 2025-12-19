from __future__ import annotations

from elo_calculator.application.base_service import BaseService
from elo_calculator.infrastructure.external_services.caching import CacheManager


class CacheMaintenanceService(BaseService):
    async def flush_all(self) -> dict[str, str]:
        cache = CacheManager()
        await cache.flush_db()
        return {'status': 'flushed'}

    async def invalidate_prefixes(self, prefixes: list[str], batch_size: int = 200) -> dict[str, list[str]]:
        cache = CacheManager()
        for p in prefixes:
            await cache.delete_matching_prefix(p, batch_size=batch_size)
        return {'deleted_prefixes': prefixes}
