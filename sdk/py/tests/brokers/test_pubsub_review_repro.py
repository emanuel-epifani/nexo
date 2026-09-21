from __future__ import annotations

import asyncio
import uuid

import pytest

from nexo import NexoClient
from tests.utils.wait_for import wait_for


@pytest.mark.asyncio
async def test_overlapping_subscriptions_receive_each_retained_value_once():
    client = await NexoClient.connect()
    base = f"review-overlap-{uuid.uuid4()}"
    topic = client.pubsub.topic(f"{base}/value")
    first: list[str] = []
    second: list[str] = []
    first_sub = None
    second_sub = None
    try:
        await topic.publish("retained", retain=True)
        first_sub = await client.pubsub.pattern(f"{base}/#").subscribe(first.append)
        await wait_for(lambda: len(first) == 1)
        second_sub = await client.pubsub.pattern(f"{base}/+").subscribe(second.append)
        await wait_for(lambda: len(second) >= 1)
        await asyncio.sleep(0.2)
        assert first == ["retained"]
        assert second == ["retained"]
    finally:
        if second_sub is not None:
            await second_sub.stop()
        if first_sub is not None:
            await first_sub.stop()
        await topic.clear_retained()
        client.disconnect()


@pytest.mark.asyncio
async def test_callback_can_await_its_own_subscription_stop():
    client = await NexoClient.connect()
    topic = client.pubsub.topic(f"review-self-stop-{uuid.uuid4()}")
    entered = asyncio.Event()
    subscription = None
    callback_stop_task = None

    async def callback(_):
        nonlocal callback_stop_task
        entered.set()
        callback_stop_task = asyncio.create_task(subscription.stop())
        await callback_stop_task

    try:
        subscription = await topic.subscribe(callback)
        await topic.publish("message")
        await entered.wait()
        await asyncio.sleep(0.2)
        assert callback_stop_task is not None
        assert callback_stop_task.done()
    finally:
        client.disconnect()
        if subscription is not None and not subscription.completion.done():
            subscription.completion.cancel()
            try:
                await subscription.completion
            except asyncio.CancelledError:
                pass
        if callback_stop_task is not None and not callback_stop_task.done():
            callback_stop_task.cancel()
            try:
                await callback_stop_task
            except asyncio.CancelledError:
                pass


@pytest.mark.asyncio
async def test_explicit_disconnect_closes_active_subscriptions():
    client = await NexoClient.connect()
    subscription = await client.pubsub.topic(
        f"review-disconnect-{uuid.uuid4()}"
    ).subscribe(lambda _: None)
    client.disconnect()
    await asyncio.sleep(0.2)
    active = subscription.active
    closed = subscription.completion.done()
    await subscription.stop()
    assert active is False
    assert closed is True
