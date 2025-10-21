from typing import Any, Optional


class InfrastructureException(Exception):
    """Base exception for infrastructure-related errors."""

    def __init__(self, message: str, data: Optional[dict[str, Any]] = None) -> None:
        self.message = message
        self.data = data


class DatabaseError(InfrastructureException):
    def __init__(self, message: str, data: Optional[dict[str, Any]] = None) -> None:
        super().__init__(message or 'Database error', data)


class ExternalServiceError(InfrastructureException):
    def __init__(self, service_name: str, service_message: str, service_status_code: int) -> None:
        data = {
            'service_name': service_name,
            'response_status_code': service_status_code,
            'response_message': service_message,
        }
        super().__init__(f'Calling serves {service_name} failed', data)
