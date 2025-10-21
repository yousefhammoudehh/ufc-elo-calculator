import hashlib
import hmac
from enum import Enum
from typing import Any, cast

import cloudpickle  # type: ignore
from redis.asyncio import ConnectionPool, Redis

from elo_calculator.configs.env import CACHING_SECRET, REDIS_HOST, REDIS_MAX_CONNECTIONS, REDIS_PORT
from elo_calculator.configs.log import get_logger

logger = get_logger()
DEFAULT_TTL = 60

pool = ConnectionPool(host=REDIS_HOST, port=REDIS_PORT, max_connections=REDIS_MAX_CONNECTIONS)


class CachePrefix(str, Enum):
    TRANSLATION = 'translations'
    NATIONALITY = 'nationality'
    LANGUAGE = 'language'
    HOLIDAY = 'holiday'
    HOLIDAY_DAYS = 'holiday_days'
    CURRENCY = 'currency'
    COUNTRY = 'country'


def secure_serialize(data: dict[str, Any] | list[dict[str, Any]] | None) -> bytes:
    serialized_data = cloudpickle.dumps(data)
    signature = hmac.new(CACHING_SECRET.encode(), serialized_data, hashlib.sha256).hexdigest().encode()
    return b'.'.join([signature, serialized_data])


def secure_deserialize(data: bytes) -> dict[str, Any] | list[dict[str, Any]] | None:
    try:
        signature, serialized_data = data.split(b'.', 1)
        expected_signature = hmac.new(CACHING_SECRET.encode(), serialized_data, hashlib.sha256).hexdigest().encode()

        if not hmac.compare_digest(signature, expected_signature):
            raise ValueError('Data integrity check failed, possible tampering detected')

        return cast(dict[str, Any], cloudpickle.loads(serialized_data))
    except Exception as e:
        raise ValueError('Deserialization failed') from e


class CacheManager:
    def __init__(self, ttl: int | None = DEFAULT_TTL) -> None:
        self.redis = Redis.from_pool(pool)
        self.ttl = ttl or DEFAULT_TTL

    async def get_json(self, key: str) -> dict[str, Any] | list[dict[str, Any]] | None:
        if data := await self.redis.get(key):
            logger.info(f'Cache hit for key: {key}')
            return secure_deserialize(data)
        logger.info(f'Cache miss for key: {key}')
        return None

    async def set_json(self, key: str, data: dict[str, Any] | list[dict[str, Any]] | None) -> None:
        logger.info(f'Cache set for key: {key}')
        if data:
            await self.redis.setex(key, self.ttl, secure_serialize(data))

    async def delete(self, key: str) -> None:
        logger.info(f'Cache delete for key: {key}')
        await self.redis.delete(key)

    async def delete_matching_prefix(self, prefix: str, batch_size: int = 200) -> None:
        logger.info(f'Deleting cache keys with prefix: {prefix}')
        keys_to_delete = []
        async for key in self.redis.scan_iter(f'{prefix}*', count=batch_size):
            keys_to_delete.append(key)

            if len(keys_to_delete) >= batch_size:
                await self.redis.unlink(*keys_to_delete)
                keys_to_delete = []

        if keys_to_delete:
            await self.redis.unlink(*keys_to_delete)

    async def flush_db(self) -> None:
        logger.info('Flushing all redis keys in the default DB')
        await self.redis.flushdb()
