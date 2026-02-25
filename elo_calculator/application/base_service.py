class BaseService:
    """Base service abstraction.

    Add shared service helpers here (validation, tracing, etc.).
    """

    def __init__(self) -> None:
        self._ready = True
