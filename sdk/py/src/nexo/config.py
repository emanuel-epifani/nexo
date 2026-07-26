from __future__ import annotations
from dataclasses import dataclass, field
from typing import Final

DEFAULT_HOST: Final[str] = "127.0.0.1"
DEFAULT_PORT: Final[int] = 7654


@dataclass(frozen=True)
class NexoConnectionConfig:
    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT
    request_timeout_ms: int = 15000
    reconnect_delay_ms: int = 1500
    sweep_interval_ms: int = 1000
    backoff_short_ms: int = 1000
    backoff_long_ms: int = 2000


@dataclass(frozen=True)
class _QueueDefaults:
    batch_size: int = 50
    wait_ms: int = 20000
    concurrency: int = 5
    peek_limit: int = 10
    peek_offset: int = 0


@dataclass(frozen=True)
class _StreamDefaults:
    batch_size: int = 100
    wait_ms: int = 20000
    concurrency: int = 1
    stop_timeout_ms: int = 30000


@dataclass(frozen=True)
class _LoggerDefaults:
    level: str = "ERROR"


@dataclass(frozen=True)
class _DefaultConfig:
    connection: NexoConnectionConfig = field(default_factory=NexoConnectionConfig)
    queue: _QueueDefaults = field(default_factory=_QueueDefaults)
    stream: _StreamDefaults = field(default_factory=_StreamDefaults)
    logger: _LoggerDefaults = field(default_factory=_LoggerDefaults)


DEFAULT_CONFIG: Final[_DefaultConfig] = _DefaultConfig()
