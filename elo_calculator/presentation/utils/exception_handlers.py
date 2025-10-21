from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from elo_calculator.application.shared.exceptions import ApplicationException
from elo_calculator.configs.log import get_logger
from elo_calculator.domain.shared.exceptions import DomainException
from elo_calculator.infrastructure.shared.exceptions import DatabaseError, ExternalServiceError
from elo_calculator.presentation.utils.response import (
    get_bad_request, get_forbidden, get_method_not_allowed, get_not_found, get_server_error
)

logger = get_logger()


async def handle_domain_error(_: Request, exc: DomainException) -> JSONResponse:
    return get_bad_request(exc.message, exc.data)


async def handle_application_error(_: Request, exc: ApplicationException) -> JSONResponse:
    return get_bad_request(exc.message, exc.data)


async def handle_external_service_error(_: Request, exc: ExternalServiceError) -> JSONResponse:
    logger.exception(exc)
    return get_server_error(exc.message, exc.data)


async def handle_database_error(_: Request, exc: DatabaseError) -> JSONResponse:
    logger.exception(exc)
    return get_server_error(exc.message, exc.data)


async def handle_validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
    errors = exc.errors()
    errors = [{'field': e['loc'][-1], 'location': e['loc'][0], 'message': e['msg']} for e in errors]
    return get_bad_request('Invalid request, check your input and try again', errors)


async def handle_forbidden_error(_: Request, exc: HTTPException) -> JSONResponse:
    return get_forbidden('Forbidden: access denied')


async def handle_not_found_error(request: Request, exc: HTTPException) -> JSONResponse:
    return get_not_found(f"The URL '{request.url.path}' was not found.")


async def handle_method_not_allowed(_: Request, exc: HTTPException) -> JSONResponse:
    return get_method_not_allowed('Method not allowed')


async def handle_error(_: Request, exc: Exception) -> JSONResponse:
    logger.exception(exc)
    return get_server_error(str(exc))


def register_exception_handlers(app: FastAPI) -> None:
    """Register app exception handlers."""
    app.add_exception_handler(DomainException, handle_domain_error)  # type: ignore
    app.add_exception_handler(ApplicationException, handle_application_error)  # type: ignore
    app.add_exception_handler(ExternalServiceError, handle_external_service_error)  # type: ignore
    app.add_exception_handler(DatabaseError, handle_database_error)  # type: ignore
    app.add_exception_handler(RequestValidationError, handle_validation_error)  # type: ignore

    app.add_exception_handler(403, handle_forbidden_error)  # type: ignore
    app.add_exception_handler(404, handle_not_found_error)  # type: ignore
    app.add_exception_handler(405, handle_method_not_allowed)  # type: ignore

    app.add_exception_handler(500, handle_error)
    app.add_exception_handler(Exception, handle_error)
