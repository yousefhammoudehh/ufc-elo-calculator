# ruff: noqa: ARG001
from string import Template

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from elo_calculator.configs.log import get_logger
from elo_calculator.errors import (
    ApplicationException,
    DatabaseException,
    DataNotFoundException,
    Errors,
    FieldException,
    ValidationException,
)
from elo_calculator.presentation.utils.response import (
    get_bad_request,
    get_method_not_allowed,
    get_not_found,
    get_server_error,
)

logger = get_logger()


async def handle_application_exception(_: Request, exc: ApplicationException) -> JSONResponse:
    return get_bad_request(errors=[exc.as_dict()])


async def handle_field_exception(_: Request, exc: FieldException) -> JSONResponse:
    return get_bad_request(errors=[exc.as_dict()])


async def handle_validation_exception(_: Request, exc: ValidationException) -> JSONResponse:
    return get_bad_request(errors=[err.as_dict() for err in exc.errors])


async def handle_request_validation_error(_: Request, exc: RequestValidationError) -> JSONResponse:
    errors = []
    for error in exc.errors():
        if error['type'] in ['json_invalid', 'json_type']:
            errors.append(
                {
                    'code': Errors.REQUEST_BODY_INVALID_ERROR.code,
                    'message': Errors.REQUEST_BODY_INVALID_ERROR.message,
                    'field': 'body',
                    'detail': f'Location: {error["loc"][-1]}',
                }
            )
        else:
            field = '.'.join(map(str, error['loc']))
            errors.append(
                {
                    'code': Errors.FIELD_ERROR.code,
                    'message': Template(Errors.FIELD_ERROR.message).safe_substitute({'field': field}),
                    'field': field,
                    'detail': error['msg'],
                }
            )
    return get_bad_request(errors=errors)


async def handle_data_not_found_exception(_: Request, exc: DataNotFoundException) -> JSONResponse:
    return get_not_found(errors=[exc.as_dict()])


async def handle_default_not_found_exception(_: Request, exc: HTTPException) -> JSONResponse:
    return get_not_found(
        errors=[{'code': Errors.RESOURCE_NOT_FOUND_ERROR.code, 'message': Errors.RESOURCE_NOT_FOUND_ERROR.message}]
    )


async def handle_default_method_not_allowed_exception(_: Request, exc: HTTPException) -> JSONResponse:
    return get_method_not_allowed(
        errors=[{'code': Errors.METHOD_NOT_ALLOWED_ERROR.code, 'message': Errors.FIELD_ERROR.message}]
    )


async def handle_database_exception(_: Request, exc: DatabaseException) -> JSONResponse:
    logger.exception(exc)
    return get_server_error(errors=[exc.as_dict()])


async def handle_exception(_: Request, exc: Exception) -> JSONResponse:
    logger.exception(exc)
    return get_server_error(
        errors=[{'code': Errors.SERVER_ERROR.code, 'message': Errors.SERVER_ERROR.message, 'detail': str(exc)}]
    )


def register_exception_handlers(app: FastAPI) -> None:
    """Register app exception handlers."""
    app.add_exception_handler(ApplicationException, handle_application_exception)  # type: ignore
    app.add_exception_handler(FieldException, handle_field_exception)  # type: ignore
    app.add_exception_handler(ValidationException, handle_validation_exception)  # type: ignore
    app.add_exception_handler(RequestValidationError, handle_request_validation_error)  # type: ignore
    app.add_exception_handler(DataNotFoundException, handle_data_not_found_exception)  # type: ignore
    app.add_exception_handler(DatabaseException, handle_database_exception)  # type: ignore

    app.add_exception_handler(404, handle_default_not_found_exception)  # type: ignore
    app.add_exception_handler(405, handle_default_method_not_allowed_exception)  # type: ignore
    app.add_exception_handler(500, handle_exception)
    app.add_exception_handler(Exception, handle_exception)
