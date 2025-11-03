# ruff: noqa: ARG001
import json
from collections.abc import Sequence
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, cast
from uuid import UUID

from fastapi import status
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from elo_calculator.utils.date_parser import date_to_iso_str, datetime_to_iso_str, time_to_str, timedelta_to_iso_str


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:  # noqa: PLR0911
        if isinstance(obj, UUID):
            return str(obj)
        if isinstance(obj, datetime):
            return datetime_to_iso_str(obj)
        if isinstance(obj, date):
            return date_to_iso_str(obj)
        if isinstance(obj, time):
            return time_to_str(obj)
        if isinstance(obj, timedelta):
            return timedelta_to_iso_str(obj)
        if isinstance(obj, Decimal):
            # Preserve numeric JSON type for decimals (e.g., promotion.strength)
            return float(obj)

        return super().default(obj)


class CustomJSONResponse(JSONResponse):
    def render(self, content: Any) -> bytes:
        return json.dumps(content, cls=CustomJSONEncoder).encode('utf-8')


DataType = BaseModel | Sequence[BaseModel] | dict[str, Any] | Sequence[dict[str, Any]] | None


def get_response[T, B: DataType](
    status_code: int,
    message: str,
    data: DataType = None,
    errors: dict[str, Any] | list[dict[str, Any]] | str | None = None,
    extra: dict[str, Any] | None = None,
) -> CustomJSONResponse:
    serialized_data: dict[str, Any] | list[dict[str, Any]] | None = None

    def serialize_value(val: Any) -> Any:
        if isinstance(val, BaseModel):
            return val.model_dump()
        if isinstance(val, Sequence) and not isinstance(val, (str, bytes, bytearray)):
            return [serialize_value(x) for x in val]
        if isinstance(val, dict):
            return {k: serialize_value(v) for k, v in val.items()}
        return val

    if data is not None and isinstance(data, (BaseModel, Sequence, dict)):
        serialized = serialize_value(data)
        # Assign, allowing mixed types; JSON encoder will handle primitives
        serialized_data = cast(Any, serialized)
    content: dict[str, Any] = {'status_code': status_code, 'message': message}
    if serialized_data is not None:
        content['data'] = serialized_data
    if errors:
        content['errors'] = errors
    content.update(extra or {})
    return CustomJSONResponse(status_code=status_code, content=content)


def get_ok[T](
    data: DataType, extra: dict[str, Any] | None = None, message: str = 'Ok', return_type: type[T] | None = None
) -> T:
    return cast(T, get_response(status.HTTP_200_OK, message, data, None, extra))


def get_bad_request[T](
    message: str = 'Bad Request',
    errors: dict[str, Any] | list[dict[str, Any]] | str | None = None,
    return_type: type[T] | None = None,
) -> T:
    return cast(T, get_response(status.HTTP_400_BAD_REQUEST, message, None, errors))


def get_unauthorized[T](
    message: str = 'Unauthorized',
    errors: dict[str, Any] | list[dict[str, Any]] | str | None = None,
    return_type: type[T] | None = None,
) -> T:
    return cast(T, get_response(status.HTTP_401_UNAUTHORIZED, message, None, errors))


def get_forbidden[T](
    message: str = 'Forbidden',
    errors: dict[str, Any] | list[dict[str, Any]] | str | None = None,
    return_type: type[T] | None = None,
) -> T:
    return cast(T, get_response(status.HTTP_403_FORBIDDEN, message, None, errors))


def get_not_found[T](
    message: str = 'Not Found',
    errors: dict[str, Any] | list[dict[str, Any]] | str | None = None,
    return_type: type[T] | None = None,
) -> T:
    return cast(T, get_response(status.HTTP_404_NOT_FOUND, message, errors=errors))


def get_method_not_allowed[T](
    message: str = 'Method Not Allowed',
    errors: dict[str, Any] | list[dict[str, Any]] | str | None = None,
    return_type: type[T] | None = None,
) -> T:
    return cast(T, get_response(status.HTTP_405_METHOD_NOT_ALLOWED, message, None, errors))


def get_server_error[T](
    message: str = 'Internal Server Error',
    errors: dict[str, Any] | list[dict[str, Any]] | str | None = None,
    return_type: type[T] | None = None,
) -> T:
    return cast(T, get_response(status.HTTP_500_INTERNAL_SERVER_ERROR, message, None, errors))
