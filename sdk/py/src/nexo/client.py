from __future__ import annotations

import signal
from typing import Callable

from .brokers.pubsub import NexoPubSub
from .brokers.queue import NexoQueueFacade
from .brokers.store import NexoStore
from .brokers.stream import NexoStreamFacade
from .config import DEFAULT_CONFIG, DEFAULT_HOST, DEFAULT_PORT, NexoConnectionConfig
from .transport.tcp.connection import NexoConnection
from .utils.logger import Logger, LogHandler


class NexoClient:
    def __init__(
        self,
        *,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        logger: LogHandler | None = None,
        log_level: str | None = None,
    ) -> None:
        self._logger = Logger(
            handler=logger,
            level=log_level or DEFAULT_CONFIG.logger.level,
        )
        self._conn = NexoConnection(
            NexoConnectionConfig(
                host=host,
                port=port,
                request_timeout_ms=DEFAULT_CONFIG.connection.request_timeout_ms,
                reconnect_delay_ms=DEFAULT_CONFIG.connection.reconnect_delay_ms,
                sweep_interval_ms=DEFAULT_CONFIG.connection.sweep_interval_ms,
                backoff_short_ms=DEFAULT_CONFIG.connection.backoff_short_ms,
                backoff_long_ms=DEFAULT_CONFIG.connection.backoff_long_ms,
            ),
            self._logger,
        )
        self.store = NexoStore(self._conn)
        self.queue = NexoQueueFacade(self._conn, self._logger)
        self.stream = NexoStreamFacade(self._conn, self._logger)
        self.pubsub = NexoPubSub(self._conn, self._logger)
        self._shutdown_handler: Callable[[], None] | None = None
        self._setup_graceful_shutdown()

    @classmethod
    async def connect(
        cls,
        *,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        logger: LogHandler | None = None,
        log_level: str | None = None,
    ) -> "NexoClient":
        client = cls(
            host=host,
            port=port,
            logger=logger,
            log_level=log_level,
        )
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

    def _setup_graceful_shutdown(self) -> None:
        def handler(signum=None, frame=None):
            self.disconnect()

        self._shutdown_handler = handler
        try:
            signal.signal(signal.SIGINT, handler)
            signal.signal(signal.SIGTERM, handler)
        except Exception:
            pass
