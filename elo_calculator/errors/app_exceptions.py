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


class ForbiddenException(AppBaseException):
    def __init__(self, detail: str | None = None, message: str | None = None, **kwargs: Any) -> None:
        super().__init__(error=Errors.FORBIDDEN_ERROR, message=message, detail=detail, **kwargs)


class UnauthorizedException(AppBaseException):
    def __init__(self, message: str | None = None, detail: str | None = None, **kwargs: Any) -> None:
        super().__init__(error=Errors.UNAUTHORIZED_ERROR, message=message, detail=detail, **kwargs)


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


class ExternalServiceException(AppBaseException):
    def __init__(
        self,
        service_name: str,
        status_code: int,
        response_message: str | None = None,
        errors: dict[str, Any] | None = None,
        caller: str | None = None,
        **kwargs: Any,
    ) -> None:
        self.service_name = service_name
        self.status_code = status_code
        self.response_message = response_message
        self.errors = errors
        self.caller = caller

        detail = f'An error occurred while communicating with service: {service_name}, caller: {self.caller}'
        super().__init__(error=Errors.EXTERNAL_SERVICE_ERROR, detail=detail, inner_errors=errors, **kwargs)


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
