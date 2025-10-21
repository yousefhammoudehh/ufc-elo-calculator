import inspect
import random
import time
from datetime import datetime
from functools import partial
from http import HTTPStatus
from typing import Any, Callable, Iterable, Mapping, Optional, Union

from httpx import AsyncBaseTransport, BaseTransport, Client, Headers, HTTPTransport, Request, Response

from olive_template.configs.log import get_logger

logger = get_logger()

HTTPX_RETRY_POLICY: dict[str, Any] = {
    'max_attempts': 5,
    'backoff_factor': 1,
    'retry_status_codes': [424, 429, 500, 502, 503, 504],
    'retryable_methods': ['DELETE', 'GET', 'HEAD', 'POST', 'PUT', 'PATCH'],
    'respect_retry_after_header': True
}

DEFAULT_TIMEOUT = 5


class RetryTransport(AsyncBaseTransport, BaseTransport):
    '''
    A custom HTTP transport that automatically retries requests using an exponential backoff
    strategy for specific HTTP status codes and request methods.
    Args:
        wrapped_transport (Union[BaseTransport, AsyncBaseTransport]): The underlying
            HTTP transport to wrap and use for making requests.
        max_attempts (int, optional): The maximum number of times to retry a request before
            giving up. Defaults to 10.
        max_backoff_wait (float, optional): The maximum time to wait between retries in seconds.
            Defaults to 60.
        backoff_factor (float, optional): The factor by which the wait time increases with each
            retry attempt. Defaults to 1.
        jitter_ratio (float, optional): The amount of jitter to add to the backoff time. Jitter is
            a random value added to the backoff time to avoid a 'thundering herd' effect. The value
            should be between 0 and 0.5. Defaults to 0.1.
        respect_retry_after_header (bool, optional): Whether to respect the Retry-After header in
            HTTP responses when deciding how long to wait before retrying. Defaults to True.
        retryable_methods (Iterable[str], optional): The HTTP methods that can be retried.
            Defaults to ['DELETE', 'GET', 'HEAD', 'POST', 'PUT'].
        retry_status_codes (Iterable[int], optional): The HTTP status codes that can be retried.
            Defaults to [424, 429, 500, 502, 503, 504].
        callback (Callable[..., Any], optional): Plugable function that if provided will be
            called at retrying time.
            Defaults to None.
    Attributes:
        _wrapped_transport (Union[BaseTransport, AsyncBaseTransport]): The underlying
            HTTP transport being wrapped.
        _max_attempts (int): The maximum number of times to retry a request.
        _backoff_factor (float): The factor by which the wait time increases with each retry
            attempt.
        _respect_retry_after_header (bool): Whether to respect the Retry-After header in HTTP
            responses.
        _retryable_methods (frozenset): The HTTP methods that can be retried.
        _retry_status_codes (frozenset): The HTTP status codes that can be retried.
        _jitter_ratio (float): The amount of jitter to add to the backoff time.
        _max_backoff_wait (float): The maximum time to wait between retries in seconds.
        _callback (Callable[..., Any], optional): Plugable function that if provided will be
            called at retrying time.
    '''

    RETRYABLE_METHODS = frozenset(['DELETE', 'GET', 'HEAD', 'POST', 'PUT'])
    RETRYABLE_STATUS_CODES = frozenset(
        [
            HTTPStatus.FAILED_DEPENDENCY,
            HTTPStatus.TOO_MANY_REQUESTS,
            HTTPStatus.INTERNAL_SERVER_ERROR,
            HTTPStatus.BAD_GATEWAY,
            HTTPStatus.SERVICE_UNAVAILABLE,
            HTTPStatus.GATEWAY_TIMEOUT,
        ]
    )
    MAX_BACKOFF_WAIT = 60

    def __init__(
        self,
        wrapped_transport: BaseTransport,
        max_attempts: int = 10,
        max_backoff_wait: float = MAX_BACKOFF_WAIT,
        backoff_factor: float = 1,
        jitter_ratio: float = 0.1,
        respect_retry_after_header: bool = True,
        retryable_methods: Optional[Iterable[str]] = None,
        retry_status_codes: Optional[Iterable[int]] = None,
        callback: Optional[Callable[..., Any]] = None,
    ) -> None:
        '''
        Initializes the instance of RetryTransport class with the given parameters.
        Args:
            wrapped_transport BaseTransport:
                The transport layer that will be wrapped and retried upon failure.
            max_attempts (int, optional):
                The maximum number of times the request can be retried in case of failure.
                Defaults to 10.
            max_backoff_wait (float, optional):
                The maximum amount of time (in seconds) to wait before retrying a request.
                Defaults to 60.
            backoff_factor (float, optional):
                The factor by which the waiting time will be multiplied in each retry attempt.
                Defaults to 1.
            jitter_ratio (float, optional):
                The ratio of randomness added to the waiting time to prevent simultaneous retries.
                Should be between 0 and 0.5. Defaults to 0.1.
            respect_retry_after_header (bool, optional):
                A flag to indicate if the Retry-After header should be respected.
                If True, the waiting time specified in Retry-After header is used for the waiting
                time.
                Defaults to True.
            retryable_methods (str, optional):
                The HTTP methods that can be retried.
                Defaults to 'DELETE', 'GET', 'HEAD', 'POST', 'PUT'.
            retry_status_codes (Iterable[int], optional):
                The HTTP status codes that can be retried.
                Defaults to [424, 429, 500, 502, 503, 504].
            callback (Callable[..., Any], optional):
                Plugable function that if provided will be called at retrying time.
                Defaults to None.
        '''
        self._wrapped_transport = wrapped_transport
        if jitter_ratio < 0 or jitter_ratio > 0.5:
            raise ValueError(
                f'Jitter ratio should be between 0 and 0.5, actual {jitter_ratio}'
            )

        self._max_attempts = max_attempts
        self._backoff_factor = backoff_factor
        self._respect_retry_after_header = respect_retry_after_header
        self._retryable_methods = frozenset(retryable_methods) if retryable_methods \
            else self.RETRYABLE_METHODS
        self._retry_status_codes = frozenset(retry_status_codes) if retry_status_codes \
            else self.RETRYABLE_STATUS_CODES
        self._jitter_ratio = jitter_ratio
        self._max_backoff_wait = max_backoff_wait
        self._callback = callback

    def handle_request(self, request: Request) -> Response:
        '''
        Sends an HTTP request, possibly with retries.
        Args:
            request (Request): The request to send.
        Returns:
            Response: The response received.
        '''
        transport: BaseTransport = self._wrapped_transport
        if request.method in self._retryable_methods:
            send_method = partial(transport.handle_request)
            response = self._retry_operation(request, send_method)
        else:
            response = transport.handle_request(request)
        return response

    def close(self) -> None:
        '''
        Closes the underlying HTTP transport, terminating all outstanding connections and
        rejecting any further requests.
        This should be called before the object is dereferenced, to ensure that connections are
        properly cleaned up.
        '''
        self._wrapped_transport.close()

    def _calculate_sleep(
        self, attempts_made: int, headers: Union[Headers, Mapping[str, str]]
    ) -> Any:
        '''
        Retry-After
        The Retry-After response HTTP header indicates how long the user agent should wait before
        making a follow-up request. There are three main cases this header is used:
        - When sent with a 503 (Service Unavailable) response, this indicates when the resource
            will be available again.
        - When sent with a 429 (Too Many Requests) response, this indicates how long to wait
            before making a new request.
        - When sent with a redirect response, such as 301 (Moved Permanently), this indicates the
            minimum time that the user agent is asked to wait before issuing the redirected request.
        '''
        retry_after_header = (headers.get('Retry-After') or '').strip()
        if self._respect_retry_after_header and retry_after_header:
            if retry_after_header.isdigit():
                return float(retry_after_header)

            try:
                parsed_date = datetime.fromisoformat(
                    retry_after_header
                ).astimezone()  # converts to local time
                diff = (parsed_date - datetime.now().astimezone()).total_seconds()
                if diff > 0:
                    return min(diff, self._max_backoff_wait)
            except ValueError:
                pass

        backoff = self._backoff_factor * (2 ** (attempts_made - 1))
        jitter = (backoff * self._jitter_ratio) * random.SystemRandom().choice([1, -1])
        total_backoff = backoff + jitter
        return min(total_backoff, self._max_backoff_wait)

    def _retry_operation(
        self,
        request: Request,
        send_method: Callable[..., Response],
    ) -> Response:
        remaining_attempts = self._max_attempts
        attempts_made = 0
        response = None
        while True:
            # Retrying starts here
            if response:
                logger.info(
                    f'Retrying the call to url: {request.url} with method: {request.method}. '
                    f'Failed response status code: {response.status_code}'
                )
                if self._callback:
                    if 'headers' and 'status_code' in \
                            inspect.getfullargspec(self._callback).args:
                        self._callback(headers=request.headers,
                                       status_code=response.status_code)
                    else:
                        self._callback()
                time.sleep(self._calculate_sleep(attempts_made, response.headers))
            response = send_method(request)
            if (
                remaining_attempts < 1
                or response.status_code not in self._retry_status_codes
            ):
                return response
            response.close()
            attempts_made += 1
            remaining_attempts -= 1


def get_client(retry_policy: Optional[dict[str, Any]] = None,
               callback: Optional[Callable[..., Any]] = None,
               timeout: Optional[float] = DEFAULT_TIMEOUT) -> Client:
    retry_policy = retry_policy or HTTPX_RETRY_POLICY
    transport = RetryTransport(HTTPTransport(), **retry_policy, callback=callback)
    client = Client(transport=transport, timeout=timeout)
    return client
