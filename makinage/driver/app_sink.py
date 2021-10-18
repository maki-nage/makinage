from typing import Callable
from collections import namedtuple
import queue
import asyncio

import rx
import rx.operators as ops
from rx.core.notification import OnNext, OnError
from rx.scheduler import NewThreadScheduler
from rx.scheduler.eventloop import AsyncIOThreadSafeScheduler

from cyclotron import Component


def QObservable(q):
    def on_subscribe(observer, scheduler):
        while(True):
            i = q.get()
            if isinstance(i, OnNext):
                observer.on_next(i.value)
                q.task_done()
            elif isinstance(i, OnError):
                observer.on_error(i.exception)
                break
            else:
                observer.on_completed()
                break

    return rx.create(on_subscribe)


def create_controllable_sink(sink_id: str,
                             operator: Callable[[rx.Observable], rx.Observable],
                             sink: rx.Observable,
                             ) -> rx.Observable:
    """Creates a controllable sink from a sink operator

    The sink operator must subscribe to its provided observable. All items are
    emitted on a thread dedicated to this operator. So it can do
    blocking calls without issues on the makinage eventloop.

    Args:
        - sink_id: The observable identifier used in the feedback loop.
        - operator: The connector logic to isolate.
        - sink: A function that subscribes to an observable.
    """
    q = queue.Queue()
    index = 0

    lifecycle = QObservable(q).pipe(
        ops.subscribe_on(NewThreadScheduler()),
        operator,
        ops.ignore_elements(),
        ops.observe_on(AsyncIOThreadSafeScheduler(asyncio.get_event_loop())),
    )

    def push_item(q, sink_id):
        def _push_item(i):
            nonlocal index
            q.put_nowait(i)
            if isinstance(i, OnNext):
                index += 1
                if index == 500:
                    index = 0
                    return (sink_id, q.qsize())
            return None
        return ops.map(_push_item)

    feedback = sink.pipe(
        ops.materialize(),
        push_item(q, sink_id),
        ops.filter(lambda i: i is not None)
    )

    return rx.merge(lifecycle, feedback)


Sink = namedtuple('Sink', ['connector'])
Source = namedtuple('Source', ['feedback'])

# Sink items
Create = namedtuple('Create', ['id', 'operator', 'observable'])
Create.__doc__ = "Creates a new application defined sink."
Create.id.__doc__ = "The id of the sink."
Create.operator.__doc__ = "The function implemententing the sink logic, as an operator."
Create.observable.__doc__ = "The observable that will feed the sink."


# Source items
Feedback = namedtuple('Feedback', ['id', 'observable'])
Feedback.__doc__ = "The feedback loop of a sink."
Feedback.id.__doc__ = "The id of the sink."
Feedback.observable.__doc__ = "The feedback observable, emitting (id, qsize) items."


def make_driver():

    def driver(sink):
        """ Application sink driver.

        This is a driver implementation where the logic is pushed by
        the application. This allows to write connectors in a more flexible way
        than writing a driver per connector.

        The Sink connector stream emits the following items: Create
        The Source feedback stream emits the following items: Feedback
        """
        def on_subscribe(observer, scheduler):
            def on_next(i):
                if type(i) is Create:
                    observer.on_next(
                        Feedback(
                            i.id,
                            create_controllable_sink(
                                i.id,
                                i.operator,
                                i.observable,
                            )
                        )
                    )

                else:
                    observer.on_error("app sink unknown command: {}".format(i))

            disposable = sink.connector.subscribe(
                on_next=on_next,
                on_error=observer.on_error,
                on_completed=observer.on_completed)

            return disposable

        return Source(
            feedback=rx.create(on_subscribe),
        )

    return Component(call=driver, input=Sink)
