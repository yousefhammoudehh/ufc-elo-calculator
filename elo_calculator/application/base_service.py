from abc import ABC

from elo_calculator.domain.user.entity import User


class BaseService(ABC):
    def __init__(self, user: User):
        self.user = user
