from collections.abc import Awaitable, Callable

from elo_calculator.application.base_service import BaseService
from elo_calculator.configs.log import get_logger

logger = get_logger()


def get_service[T: BaseService](service_class: type[T]) -> Callable[[], Awaitable[T]]:
    """Factory returning a FastAPI dependency that injects the requested service.

    Behavior:
      * For subclasses of InternalBaseService: no auth required (used by /internal/ routes behind HMAC middleware).
      * For subclasses of BaseService: bearer token is required and decoded into AuthContext.
    """

    async def service_dependency() -> T:
        return service_class()

    return service_dependency
