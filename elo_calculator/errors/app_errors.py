from dataclasses import dataclass
from typing import Any


@dataclass
class AppError:
    code: str = ''
    message: str = ''
    field: str | None = None
    detail: str | None = None
    description: str | None = None


class ErrorMeta(type):
    def __new__(cls, name: str, bases: tuple[type, ...], dct: dict[str, Any]) -> type:
        attr_names = set()

        # Iterate over the class attributes
        for attr_name, attr_value in dct.items():
            if isinstance(attr_value, AppError):
                if attr_name in attr_names:
                    raise ValueError(f"Duplicate attribute name '{attr_name}' in class '{name}'")
                attr_names.add(attr_name)

                # Update the `code` field to match the attribute name
                attr_value.code = attr_name
        return super().__new__(cls, name, bases, dct)


class Errors(metaclass=ErrorMeta):
    # Generic errors
    FIELD_ERROR = AppError(
        message='Invalid value for $field',
        description='This error occurs when the provided value for a specific field does not meet the expected format, '
        'type, or constraints',
    )
    VALIDATION_ERROR = AppError(
        message='Invalid request, please check the provided inputs',
        description='This error is triggered when the request contains invalid or missing data, preventing proper '
        'processing',
    )
    REQUEST_BODY_INVALID_ERROR = AppError(
        message='The request body contains invalid JSON',
        description='Occurs when the request body cannot be parsed as valid JSON, often due to syntax errors or '
        'incorrect formatting',
    )
    FORBIDDEN_ERROR = AppError(
        message='You do not have permission to perform this action',
        description='This error is returned when the user attempts to access a resource or perform an action they are '
        'not authorized for',
    )
    RESOURCE_NOT_FOUND_ERROR = AppError(
        message='The requested resource could not be found',
        description='Triggered when a requested resource does not exist or has been removed',
    )
    METHOD_NOT_ALLOWED_ERROR = AppError(
        message='The requested HTTP method is not allowed for this resource',
        description='Occurs when a client attempts to use an HTTP method that is not supported by '
        'the requested endpoint',
    )
    EXTERNAL_SERVICE_ERROR = AppError(
        message='An error occurred while communicating with an external service',
        description='This error occurs when a failure happens while interacting with an external API or service, often '
        'due to network issues or service unavailability',
    )
    DATABASE_ERROR = AppError(
        message='An error occurred while interacting with the database',
        description='Triggered when a database operation fails due to constraints, connection issues, or unexpected '
        'errors in queries',
    )
    SERVER_ERROR = AppError(
        message='An error occurred while processing your request, please try again later',
        description='A generic server side error that occurs when an unexpected issue prevents the request from being '
        'processed successfully',
    )
    UNAUTHORIZED_ERROR = AppError(
        message='Unauthorized',
        description='This error is returned when the user is not authorized to access the requested resource',
    )
