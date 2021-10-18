import asyncio
import pytest
import queue
import traceback

import rx
import rx.operators as ops
from rx.subject import Subject
import rxsci as rs
from makinage.driver.app_sink import make_driver, Sink, Create, create_controllable_sink


def on_error(e):
    raise e

@pytest.mark.asyncio
async def test_controllable_sink():
    actual_result = []
    feedback_result = []
    feedback_error = []
    feedback_completed = []

    sink = Subject()
    sink_lock_q = queue.Queue()

    def op(source):
        return source.pipe(
            ops.do_action(lambda i: actual_result.append(i)),
            # block the connector's operator thread
            ops.do_action(lambda i: sink_lock_q.get()),
        )

    feedback = create_controllable_sink('test', op, sink)

    feedback.subscribe(
        on_next=feedback_result.append,
        on_error=feedback_error.append,
        on_completed=feedback_completed.append,
    )

    for i in range(450):
        sink.on_next(i)

    await asyncio.sleep(0.1)
    assert feedback_result == []
    assert actual_result == [0]

    for i in range(10):
        sink_lock_q.put_nowait(i)

    await asyncio.sleep(0.1)
    assert feedback_result == []
    assert actual_result == list(range(11))

    for i in range(50):
        sink.on_next(i)

    assert feedback_result == [('test', 489)]


@pytest.mark.asyncio
async def test_driver():
    sink_lock_q = queue.Queue()
    sink_data = Subject()
    component = make_driver()

    actual_result = []
    feedback_result = []
    feedback_completed = []

    def test_operator():
        return rx.pipe(
            ops.do_action(print),
            ops.do_action(lambda i: actual_result.append(i)),
            # block the connector's operator thread
            ops.do_action(lambda i: sink_lock_q.get()),
        )

    source = component.call(Sink(
        connector=rx.just(Create('test', test_operator(), sink_data))
    ))

    source.feedback.pipe(
        ops.do_action(print),
        rs.ops.assert_(lambda i: i.id is 'test'),
        ops.flat_map(lambda i: i.observable),
    ).subscribe(
        on_next=feedback_result.append,
        on_error=on_error,
        on_completed=feedback_completed.append,
    )

    for i in range(450):
        sink_data.on_next(i)

    await asyncio.sleep(0.1)
    assert feedback_result == []
    assert actual_result == [0]

    for i in range(10):
        sink_lock_q.put_nowait(i)

    await asyncio.sleep(0.1)
    assert feedback_result == []
    assert actual_result == list(range(11))

    for i in range(50):
        sink_data.on_next(i)

    assert feedback_result == [('test', 489)]
