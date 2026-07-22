class NexoError(Exception):
    pass


class ConnectionClosedError(NexoError):
    def __init__(self) -> None:
        super().__init__("Connection closed")


class RequestTimeoutError(NexoError):
    def __init__(self, timeout_ms: int) -> None:
        super().__init__(f"Request timeout after {timeout_ms}ms")


class NotConnectedError(NexoError):
    def __init__(self) -> None:
        super().__init__("Client not connected")
