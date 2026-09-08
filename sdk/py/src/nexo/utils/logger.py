from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from ..config import DEFAULT_CONFIG

LogHandler = Callable[[str, str, Any], None]


class LogLevel:
    TRACE = 0
    DEBUG = 1
    INFO = 2
    WARN = 3
    ERROR = 4
    NONE = 5

    _names = {
        "TRACE": TRACE,
        "DEBUG": DEBUG,
        "INFO": INFO,
        "WARN": WARN,
        "ERROR": ERROR,
        "OFF": NONE,
    }

    @classmethod
    def parse(cls, level: str) -> int:
        return cls._names.get(level.upper(), cls.ERROR)


class Logger:
    def __init__(
        self,
        handler: Optional[LogHandler] = None,
        level: Optional[str] = None,
    ) -> None:
        env_level = level or os.environ.get("NEXO_LOG", "").upper() or DEFAULT_CONFIG.logger.level
        self._level = LogLevel.parse(env_level)
        self._handler: LogHandler = handler or self._default_handler

    def _default_handler(self, level: str, msg: str, *args: Any) -> None:
        timestamp = datetime.now(timezone.utc).isoformat()
        prefix = f"[SDK] [{timestamp}] {level}"
        if level == "ERROR":
            print(prefix, msg, *args, file=sys.stderr)
        elif level == "WARN":
            print(prefix, msg, *args, file=sys.stderr)
        else:
            print(prefix, msg, *args)

    def trace(self, msg: str, *args: Any) -> None:
        if self._level <= LogLevel.TRACE:
            self._handler("TRACE", msg, *args)

    def debug(self, msg: str, *args: Any) -> None:
        if self._level <= LogLevel.DEBUG:
            self._handler("DEBUG", msg, *args)

    def info(self, msg: str, *args: Any) -> None:
        if self._level <= LogLevel.INFO:
            self._handler("INFO", msg, *args)

    def warn(self, msg: str, *args: Any) -> None:
        if self._level <= LogLevel.WARN:
            self._handler("WARN", msg, *args)

    def error(self, msg: str, *args: Any) -> None:
        if self._level <= LogLevel.ERROR:
            self._handler("ERROR", msg, *args)
