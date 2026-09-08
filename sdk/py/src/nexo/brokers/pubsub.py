from __future__ import annotations

import asyncio
import inspect
import itertools
from typing import Any, Callable, Generic, TypeVar, TypedDict

from ..config import DEFAULT_CONFIG
from ..errors import SlowConsumerError
from ..protocol.generated import (
    FLAG_PUBSUB_PUB_CLEAR,
    FLAG_PUBSUB_PUB_HAS_TTL,
    FLAG_PUBSUB_PUB_RETAIN,
    PubSubOpcode,
)
from ..subscription import Subscription
from ..transport.tcp.connection import NexoConnection
from ..utils.logger import Logger


T = TypeVar("T")


class PubSubMessageMeta(TypedDict):
    topic: str


PubSubHandler = (
    Callable[[T], Any]
    | Callable[[T, PubSubMessageMeta], Any]
)


def _callback_accepts_meta(callback: Callable[..., Any]) -> bool:
    try:
        signature = inspect.signature(callback)
    except (TypeError, ValueError):
        return True
    positional = sum(
        parameter.kind in (parameter.POSITIONAL_ONLY, parameter.POSITIONAL_OR_KEYWORD)
        for parameter in signature.parameters.values()
    )
    return positional >= 2 or any(
        parameter.kind == parameter.VAR_POSITIONAL
        for parameter in signature.parameters.values()
    )


class _Listener:
    __slots__ = (
        "id",
        "handler",
        "wants_meta",
        "queue",
        "task",
        "active",
        "processing",
        "failure",
    )

    def __init__(self, listener_id: int, handler: Callable[..., Any], capacity: int) -> None:
        self.id = listener_id
        self.handler = handler
        self.wants_meta = _callback_accepts_meta(handler)
        self.queue: asyncio.Queue[tuple[Any, PubSubMessageMeta]] = asyncio.Queue(
            maxsize=capacity
        )
        self.task: asyncio.Task[None] | None = None
        self.active = True
        self.processing = False
        self.failure: SlowConsumerError | None = None


class _PatternState:
    __slots__ = ("pattern", "wildcard", "listeners")

    def __init__(self, pattern: str, wildcard: bool) -> None:
        self.pattern = pattern
        self.wildcard = wildcard
        self.listeners: dict[int, _Listener] = {}


class _MatcherNode:
    __slots__ = ("children", "single", "terminal", "multi")

    def __init__(self) -> None:
        self.children: dict[str, _MatcherNode] = {}
        self.single: _MatcherNode | None = None
        self.terminal: _PatternState | None = None
        self.multi: _PatternState | None = None


class _PatternMatcher:
    def __init__(self) -> None:
        self._root = _MatcherNode()

    def add(self, state: _PatternState) -> None:
        node = self._root
        for segment in state.pattern.split("/"):
            if segment == "#":
                node.multi = state
                return
            if segment == "+":
                if node.single is None:
                    node.single = _MatcherNode()
                node = node.single
            else:
                node = node.children.setdefault(segment, _MatcherNode())
        node.terminal = state

    def remove(self, state: _PatternState) -> None:
        segments = state.pattern.split("/")

        def remove_from(node: _MatcherNode, index: int) -> bool:
            segment = segments[index]
            if segment == "#":
                if node.multi is state:
                    node.multi = None
            elif segment == "+":
                child = node.single
                if child is not None and index + 1 < len(segments):
                    if remove_from(child, index + 1):
                        node.single = None
                elif child is not None:
                    if child.terminal is state:
                        child.terminal = None
                    if self._empty(child):
                        node.single = None
            else:
                child = node.children.get(segment)
                if child is not None and index + 1 < len(segments):
                    if remove_from(child, index + 1):
                        node.children.pop(segment, None)
                elif child is not None:
                    if child.terminal is state:
                        child.terminal = None
                    if self._empty(child):
                        node.children.pop(segment, None)
            return self._empty(node)

        remove_from(self._root, 0)

    def match(self, topic: str) -> list[_PatternState]:
        frontier = [self._root]
        matches: list[_PatternState] = []
        for segment in topic.split("/"):
            next_frontier: list[_MatcherNode] = []
            for node in frontier:
                if node.multi is not None:
                    matches.append(node.multi)
                literal = node.children.get(segment)
                if literal is not None:
                    next_frontier.append(literal)
                if node.single is not None:
                    next_frontier.append(node.single)
            frontier = next_frontier
            if not frontier:
                break
        for node in frontier:
            if node.terminal is not None:
                matches.append(node.terminal)
            if node.multi is not None:
                matches.append(node.multi)
        return matches

    @staticmethod
    def _empty(node: _MatcherNode) -> bool:
        return (
            not node.children
            and node.single is None
            and node.terminal is None
            and node.multi is None
        )


def _validate_segments(value: str) -> list[str]:
    if not value:
        raise ValueError("PubSub topic cannot be empty")
    segments = value.split("/")
    if any(not segment for segment in segments):
        raise ValueError("PubSub topic cannot contain empty segments")
    return segments


def _validate_topic(topic: str) -> None:
    segments = _validate_segments(topic)
    if any("+" in segment or "#" in segment for segment in segments):
        raise ValueError("Concrete PubSub topic cannot contain wildcards")


def _validate_pattern(pattern: str) -> None:
    segments = _validate_segments(pattern)
    wildcard = False
    for index, segment in enumerate(segments):
        if "+" in segment and segment != "+":
            raise ValueError("+ wildcard must occupy an entire segment")
        if "#" in segment and segment != "#":
            raise ValueError("# wildcard must occupy an entire segment")
        if segment == "#" and index != len(segments) - 1:
            raise ValueError("# wildcard must be the last segment")
        wildcard = wildcard or segment in {"+", "#"}
    if not wildcard:
        raise ValueError("PubSub pattern must contain a wildcard")


class NexoTopic(Generic[T]):
    def __init__(self, broker: "NexoPubSub", name: str) -> None:
        _validate_topic(name)
        self._broker = broker
        self.name = name

    async def publish(
        self,
        data: T,
        *,
        retain: bool = False,
        ttl: int | None = None,
    ) -> None:
        await self._broker._publish(
            self.name,
            data,
            retain=retain,
            ttl=ttl,
        )

    async def subscribe(
        self,
        callback: PubSubHandler[T],
        *,
        queue_capacity: int = DEFAULT_CONFIG.pubsub.listener_queue_capacity,
    ) -> Subscription[T]:
        return await self._broker._subscribe(
            self.name,
            callback,
            wildcard=False,
            queue_capacity=queue_capacity,
        )

    async def clear_retained(self) -> None:
        await self._broker._clear_retained(self.name)


class NexoPattern(Generic[T]):
    def __init__(self, broker: "NexoPubSub", pattern: str) -> None:
        _validate_pattern(pattern)
        self._broker = broker
        self.pattern = pattern

    async def subscribe(
        self,
        callback: PubSubHandler[T],
        *,
        queue_capacity: int = DEFAULT_CONFIG.pubsub.listener_queue_capacity,
    ) -> Subscription[T]:
        return await self._broker._subscribe(
            self.pattern,
            callback,
            wildcard=True,
            queue_capacity=queue_capacity,
        )


class NexoPubSub:
    def __init__(self, conn: NexoConnection, logger: Logger) -> None:
        self._conn = conn
        self._logger = logger
        self._states: dict[str, _PatternState] = {}
        self._exact: dict[str, _PatternState] = {}
        self._matcher = _PatternMatcher()
        self._listener_ids = itertools.count(1)
        self._registry_lock = asyncio.Lock()
        conn.on_push = self._enqueue
        conn.on_reconnect = self._restore_subscriptions

    def topic(self, name: str) -> NexoTopic[Any]:
        return NexoTopic(self, name)

    def pattern(self, wildcard: str) -> NexoPattern[Any]:
        return NexoPattern(self, wildcard)

    async def _publish(
        self,
        topic: str,
        data: Any,
        *,
        retain: bool,
        ttl: int | None,
    ) -> None:
        if ttl is not None and (
            not isinstance(ttl, int)
            or isinstance(ttl, bool)
            or ttl < 0
            or ttl > 0xFFFFFFFF
        ):
            raise ValueError(f"[PubSub] Invalid ttl: {ttl}")
        has_ttl = ttl is not None
        flags = (FLAG_PUBSUB_PUB_RETAIN if retain else 0x00) | (
            FLAG_PUBSUB_PUB_HAS_TTL if has_ttl else 0x00
        )

        def build(writer: Any) -> None:
            writer.string(topic).u8(flags)
            if has_ttl:
                writer.u32(ttl)
            writer.any(data)

        await self._conn.send(PubSubOpcode.PUB, build)

    async def _clear_retained(self, topic: str) -> None:
        await self._conn.send(
            PubSubOpcode.PUB,
            lambda writer: writer.string(topic)
            .u8(FLAG_PUBSUB_PUB_CLEAR)
            .any(b""),
        )

    async def _subscribe(
        self,
        pattern: str,
        callback: Callable[..., Any],
        *,
        wildcard: bool,
        queue_capacity: int,
    ) -> Subscription[Any]:
        if not isinstance(queue_capacity, int) or isinstance(queue_capacity, bool) or queue_capacity < 1:
            raise ValueError("queue_capacity must be a positive integer")
        async with self._registry_lock:
            state = self._states.get(pattern)
            first_listener = state is None
            if state is None:
                state = _PatternState(pattern, wildcard)
                self._register_state(state)
            elif state.wildcard != wildcard:
                raise ValueError("PubSub topic and pattern namespaces cannot overlap")

            listener = _Listener(next(self._listener_ids), callback, queue_capacity)
            state.listeners[listener.id] = listener
            listener.task = asyncio.create_task(self._consume(listener, pattern))
            if first_listener:
                try:
                    await self._conn.send(
                        PubSubOpcode.SUB,
                        lambda writer: writer.string(pattern),
                    )
                except BaseException:
                    state.listeners.pop(listener.id, None)
                    self._unregister_state(state)
                    await self._close_listener(listener)
                    raise

        async def stop_listener() -> None:
            await self._stop_listener(pattern, listener.id)

        return Subscription(
            stop_fn=stop_listener,
            active_fn=lambda: listener.active,
            completion=listener.task,
            error_fn=lambda: listener.failure,
        )

    async def _stop_listener(self, pattern: str, listener_id: int) -> None:
        listener: _Listener | None = None
        wire_error: Exception | None = None
        async with self._registry_lock:
            state = self._states.get(pattern)
            if state is None:
                return
            listener = state.listeners.pop(listener_id, None)
            if listener is None:
                return
            listener.active = False
            if not state.listeners:
                self._unregister_state(state)
                if self._conn.is_connected:
                    try:
                        await self._conn.send(
                            PubSubOpcode.UNSUB,
                            lambda writer: writer.string(pattern),
                        )
                    except Exception as error:
                        wire_error = error
        await self._close_listener(listener)
        if wire_error is not None:
            raise wire_error

    async def _close_listener(self, listener: _Listener) -> None:
        listener.active = False
        if listener.task is None:
            return
        if not listener.processing:
            listener.task.cancel()
        try:
            await listener.task
        except asyncio.CancelledError:
            pass

    async def _consume(self, listener: _Listener, pattern: str) -> None:
        try:
            while listener.active:
                data, meta = await listener.queue.get()
                if not listener.active:
                    break
                listener.processing = True
                try:
                    result = (
                        listener.handler(data, meta)
                        if listener.wants_meta
                        else listener.handler(data)
                    )
                    if inspect.isawaitable(result):
                        await result
                except Exception as error:
                    self._logger.error(
                        f"[PubSub:{pattern}] handler error: {error}"
                    )
                finally:
                    listener.processing = False
        except asyncio.CancelledError:
            pass
        finally:
            listener.active = False

    def _register_state(self, state: _PatternState) -> None:
        self._states[state.pattern] = state
        if state.wildcard:
            self._matcher.add(state)
        else:
            self._exact[state.pattern] = state

    def _unregister_state(self, state: _PatternState) -> None:
        self._states.pop(state.pattern, None)
        if state.wildcard:
            self._matcher.remove(state)
        else:
            self._exact.pop(state.pattern, None)

    def _enqueue(self, topic: str, data: Any) -> None:
        exact = self._exact.get(topic)
        if exact is not None:
            self._enqueue_state(exact, topic, data)
        for state in self._matcher.match(topic):
            self._enqueue_state(state, topic, data)

    def _enqueue_state(self, state: _PatternState, topic: str, data: Any) -> None:
        meta: PubSubMessageMeta = {"topic": topic}
        for listener in tuple(state.listeners.values()):
            if not listener.active:
                continue
            try:
                listener.queue.put_nowait((data, meta))
            except asyncio.QueueFull:
                listener.failure = SlowConsumerError(
                    f"PubSub listener queue exceeded capacity {listener.queue.maxsize}"
                )
                listener.active = False
                self._logger.error(f"[PubSub:{state.pattern}] {listener.failure}")
                asyncio.create_task(
                    self._stop_overflowed_listener(state.pattern, listener.id)
                )

    async def _stop_overflowed_listener(self, pattern: str, listener_id: int) -> None:
        try:
            await self._stop_listener(pattern, listener_id)
        except Exception as error:
            self._logger.error(
                f"[PubSub:{pattern}] Failed to stop slow listener: {error}"
            )

    async def _restore_subscriptions(self) -> None:
        async with self._registry_lock:
            patterns = tuple(self._states)
            if not patterns:
                return
            self._logger.info(
                f"[PubSub] Restoring {len(patterns)} subscription(s)..."
            )
            async def restore(pattern: str) -> None:
                await self._conn.send(
                    PubSubOpcode.SUB,
                    lambda writer: writer.string(pattern),
                )

            results = await asyncio.gather(
                *(restore(pattern) for pattern in patterns),
                return_exceptions=True,
            )
            for pattern, result in zip(patterns, results):
                if isinstance(result, BaseException):
                    self._logger.error(
                        f"[PubSub] Failed to resubscribe to {pattern}",
                        result,
                    )
