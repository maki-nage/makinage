import asyncio
import pytest
import time

import rx
import rx.operators as ops
from rx.subject import Subject
from rx.scheduler.eventloop import AsyncIOScheduler
import rxsci as rs

from makinage.driver.app_source import make_driver, Sink, Create, create_controllable_source


@pytest.mark.asyncio
async def test_controllable_source():
    actual_result = []
    actual_error = []
    actual_completed = []
    actual_sleep = []

    def source():
        for i in range(3):
            yield i
            time.sleep(0.3)

    def test_sleep(i): actual_sleep.append(i)

    source = rx.from_(source())  # Subject()
    control = Subject()


    #async def test():
    controllable = create_controllable_source(
        source, control,
        loop=asyncio.get_event_loop(), sleep=test_sleep,
    )
    controllable.subscribe(
        on_next=actual_result.append,
        on_error=actual_error.append,
        on_completed=lambda: actual_completed.append(True),
    )

    await asyncio.sleep(0.2)
    assert actual_result == [0]
    control.on_next(1.0)
    await asyncio.sleep(0.2)
    assert actual_sleep == [1.0]
    await asyncio.sleep(0.4)
    assert actual_result == [0, 1, 2]

    control.on_completed()
    await asyncio.sleep(0.5)
    assert actual_completed == [True]


@pytest.mark.asyncio
async def test_driver():
    actual_result = []
    actual_error = []
    actual_completed = []
    actual_sleep = []

    def test_sleep(i): actual_sleep.append(i)

    def source():
        for i in range(3):
            yield i
            time.sleep(0.3)

    source = rx.from_(source())
    control = Subject()
    component = make_driver(loop=asyncio.get_event_loop(), sleep=test_sleep)

    controllable_source = component.call(Sink(
        connector=rx.just(Create('test', source, control))
    ))

    controllable_source.source.pipe(
        rs.ops.assert_(lambda i: i.id == 'test'),
        ops.flat_map(lambda i: i.observable),
    ).subscribe_(
        on_next=actual_result.append,
        on_error=actual_error.append,
        on_completed=lambda: actual_completed.append(True),
    )

    await asyncio.sleep(0.2)
    assert actual_result == [0]
    control.on_next(1.0)
    await asyncio.sleep(0.2)
    assert actual_sleep == [1.0]
    print("fizz")
    await asyncio.sleep(0.5)
    assert actual_result == [0, 1, 2]

    control.on_completed()
    await asyncio.sleep(0.2)
    assert actual_completed == [True]
