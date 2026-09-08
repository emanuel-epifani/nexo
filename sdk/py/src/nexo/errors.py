from __future__ import annotations

from typing import Any, ClassVar, TypedDict

from .protocol.generated import ErrorCode


class ConfigurationDifference(TypedDict):
    path: str
    requested: Any
    actual: Any


class ResourceConfigurationConflictDetails(TypedDict):
    resourceKind: str
    resourceName: str
    requested: dict[str, Any]
    actual: dict[str, Any]
    differences: list[ConfigurationDifference]


class NexoError(Exception):
    def __init__(
        self,
        message: str,
        *,
        code: ErrorCode | int | None = None,
        details: Any = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.details = details


class ServerError(NexoError):
    error_code: ClassVar[ErrorCode]

    def __init__(self, message: str, *, details: Any = None) -> None:
        super().__init__(message, code=self.error_code, details=details)


class InternalError(ServerError):
    error_code = ErrorCode.INTERNAL


class InvalidArgumentError(ServerError):
    error_code = ErrorCode.INVALID_ARGUMENT


class ResourceNotFoundError(ServerError):
    error_code = ErrorCode.RESOURCE_NOT_FOUND


class ResourceConfigurationConflictError(ServerError):
    error_code = ErrorCode.RESOURCE_CONFIG_CONFLICT


class NotAuthorizedError(ServerError):
    error_code = ErrorCode.NOT_AUTHORIZED


class FencedError(ServerError):
    error_code = ErrorCode.FENCED


class NotMemberError(ServerError):
    error_code = ErrorCode.NOT_MEMBER


class SlowConsumerError(ServerError):
    error_code = ErrorCode.SLOW_CONSUMER


class StorageError(ServerError):
    error_code = ErrorCode.STORAGE_ERROR


class ProtocolError(ServerError):
    error_code = ErrorCode.PROTOCOL_ERROR


_ERROR_TYPES: dict[ErrorCode, type[ServerError]] = {
    error_type.error_code: error_type
    for error_type in (
        InternalError,
        InvalidArgumentError,
        ResourceNotFoundError,
        ResourceConfigurationConflictError,
        NotAuthorizedError,
        FencedError,
        NotMemberError,
        SlowConsumerError,
        StorageError,
        ProtocolError,
    )
}


def server_error(
    code: ErrorCode | int,
    message: str,
    *,
    details: Any = None,
) -> NexoError:
    try:
        known_code = ErrorCode(code)
    except ValueError:
        return NexoError(message, code=code, details=details)
    return _ERROR_TYPES[known_code](message, details=details)


class ConnectionClosedError(NexoError):
    def __init__(self) -> None:
        super().__init__("Connection closed")


class RequestTimeoutError(NexoError):
    def __init__(self, timeout_ms: int) -> None:
        super().__init__(f"Request timeout after {timeout_ms}ms")


class RequestCancelledError(NexoError):
    def __init__(self) -> None:
        super().__init__("Request cancelled")


class NotConnectedError(NexoError):
    def __init__(self) -> None:
        super().__init__("Client not connected")
