from string import Template
from typing import Any

from elo_calculator.errors.app_errors import AppError, Errors


class AppBaseException(Exception):  # noqa: N818
    def __init__(
        self,
        error: AppError,
        message: str | None = None,
        field: str | None = None,
        detail: str | None = None,
        inner_errors: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self.error = error
        self.code = error.code
        self.field = field or error.field
        self.message = message or error.message
        self.detail = detail or error.detail
        self.inner_errors = inner_errors

        if self.field:
            kwargs['field'] = self.field

        if self.message:
            self.message = Template(self.message).safe_substitute(kwargs)
        if self.detail:
            self.detail = Template(self.detail).safe_substitute(kwargs)

        super().__init__(self.message)

    def as_dict(self, include_none: bool = False, exclude_fields: set[str] | None = None) -> dict[str, Any]:
        data = {
            'code': self.code,
            'message': self.message,
            'field': self.field,
            'detail': self.detail,
            'errors': self.inner_errors,
        }

        if exclude_fields:
            data = {k: v for k, v in data.items() if k not in exclude_fields}

        if not include_none:
            data = {k: v for k, v in data.items() if v is not None}

        return data


class DatabaseException(AppBaseException):
    def __init__(
        self,
        detail: str | None = None,
        model_cls: str | None = None,
        table: str | None = None,
        message: str | None = None,
        **kwargs: Any,
    ) -> None:
        self.model_cls = model_cls
        self.table = table

        super().__init__(error=Errors.DATABASE_ERROR, message=message, detail=detail, **kwargs)


class ApplicationException(AppBaseException):
    def __init__(self, error: AppError, message: str | None = None, detail: str | None = None, **kwargs: Any) -> None:
        super().__init__(error=error, message=message, detail=detail, **kwargs)


class FieldException(AppBaseException):
    def __init__(self, field: str, detail: str | None = None, message: str | None = None, **kwargs: Any) -> None:
        super().__init__(error=Errors.FIELD_ERROR, message=message, field=f'body.{field}', detail=detail, **kwargs)


class ValidationException(AppBaseException):
    def __init__(
        self, errors: list[AppBaseException], message: str | None = None, detail: str | None = None, **kwargs: Any
    ) -> None:
        self.errors = errors
        super().__init__(error=Errors.VALIDATION_ERROR, message=message, detail=detail, **kwargs)


class DataNotFoundException(AppBaseException):
    def __init__(self, detail: str | None = None, message: str | None = None, **kwargs: Any) -> None:
        super().__init__(error=Errors.RESOURCE_NOT_FOUND_ERROR, message=message, detail=detail, **kwargs)
