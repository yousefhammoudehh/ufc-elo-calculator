from typing import Any, Callable, Type, TypeVar

from fastapi import Depends, Request

from elo_calculator.application.base_service import BaseService
from elo_calculator.configs.log import get_logger
from elo_calculator.domain.user.entity import User

T = TypeVar("T", bound=BaseService)

logger = get_logger()


def get_user(request: Request) -> Any:
    return request.state.user


def get_service(service_class: Type[T]) -> Callable[..., Any] | None:
    def service_dependency(user: User = Depends(get_user)) -> T:
        return service_class(user=user)
    return service_dependency
