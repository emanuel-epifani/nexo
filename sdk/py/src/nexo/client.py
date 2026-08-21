from __future__ import annotations

import signal
from typing import Any, Callable, Optional

from .brokers.pubsub import NexoPubSub, NexoTopic
from .brokers.queue import NexoQueue
from .brokers.store import NexoStore
from .brokers.stream import NexoStream
from .config import DEFAULT_CONFIG, DEFAULT_HOST, DEFAULT_PORT, NexoConnectionConfig
from .transport.tcp.connection import NexoConnection
from .utils.logger import Logger, LogHandler


class NexoOptions:
    def __init__(
        self,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        logger: Optional[LogHandler] = None,
        log_level: Optional[str] = None,
    ) -> None:
        self.host = host
        self.port = port
        self.logger = logger
        self.log_level = log_level


class NexoClient:
    def __init__(self, options: Optional[NexoOptions] = None) -> None:
        opts = options or NexoOptions()
        self._logger = Logger(
            handler=opts.logger,
            level=opts.log_level or DEFAULT_CONFIG.logger.level,
        )

        self._conn = NexoConnection(
            NexoConnectionConfig(
                host=opts.host,
                port=opts.port,
                request_timeout_ms=DEFAULT_CONFIG.connection.request_timeout_ms,
                reconnect_delay_ms=DEFAULT_CONFIG.connection.reconnect_delay_ms,
                sweep_interval_ms=DEFAULT_CONFIG.connection.sweep_interval_ms,
                backoff_short_ms=DEFAULT_CONFIG.connection.backoff_short_ms,
                backoff_long_ms=DEFAULT_CONFIG.connection.backoff_long_ms,
            ),
            self._logger,
        )

        self.store = NexoStore(self._conn)
        self._pubsub_broker = NexoPubSub(self._conn, self._logger)
        self._shutdown_handler: Optional[Callable[[], None]] = None
        self._setup_graceful_shutdown()

    @staticmethod
    async def connect(options: Optional[NexoOptions] = None) -> "NexoClient":
        client = NexoClient(options)
        await client._conn.connect()
        return client

    def disconnect(self) -> None:
        if self._shutdown_handler is not None:
            try:
                signal.signal(signal.SIGINT, signal.SIG_DFL)
                signal.signal(signal.SIGTERM, signal.SIG_DFL)
            except Exception:
                pass
            self._shutdown_handler = None
        self._conn.disconnect()

    def queue(self, name: str) -> NexoQueue[Any]:
        return NexoQueue(self._conn, name, self._logger)

    def stream(self, name: str) -> NexoStream[Any]:
        return NexoStream(self._conn, name, self._logger)

    def pubsub(self, name: str) -> NexoTopic[Any]:
        return NexoTopic(self._pubsub_broker, name)

    def _setup_graceful_shutdown(self) -> None:
        def handler(signum=None, frame=None):
            self.disconnect()

        self._shutdown_handler = handler
        try:
            signal.signal(signal.SIGINT, handler)
            signal.signal(signal.SIGTERM, handler)
        except Exception:
            pass
