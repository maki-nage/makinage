from collections import namedtuple
import time
import asyncio

import rx
import rx.operators as ops
from rx.scheduler import NewThreadScheduler
from rx.scheduler.eventloop import AsyncIOThreadSafeScheduler

from cyclotron import Component


ControlItem = namedtuple('ControlItem', ['delay'])


def control_sync(i, sleep):
    if type(i) is ControlItem:
        sleep(i.delay)
        return ControlItem
    return i


def create_controllable_source(source, control, loop, sleep):
    """Makes an observable controllable to handle backpressure

    This function takes an observable as input makes it controllable by
    executing it in a dedicated worker thread. This allows to regulate
    the emission of the items independently of the asyncio event loop.

    Args:
        - source: An observable emitting the source items.
        - control: The control observable emitting delay items in seconds.
        - sleep: the sleep function to used. Needed only for testing.

    Returns:
        An observable similar to the source observable, with emission being
        controlled by the control observable.
    """
    typed_control = control.pipe(
        ops.observe_on(NewThreadScheduler()),
        ops.map(ControlItem),
    )

    scheduled_source = source.pipe(
        ops.subscribe_on(NewThreadScheduler()),
        ops.merge(typed_control),
        ops.map(lambda i: control_sync(i, sleep)),
        ops.filter(lambda i: i is not ControlItem),
        ops.observe_on(AsyncIOThreadSafeScheduler(loop)),
    )
    return scheduled_source


Sink = namedtuple('Sink', ['connector'])
Source = namedtuple('Source', ['source'])

# Sink items
Create = namedtuple('Create', ['id', 'observable', 'control'])
Create.__doc__ = "Creates a new application defined source."
Create.id.__doc__ = "The id of the source."
Create.observable.__doc__ = "The observable that will feed the source."
Create.control.__doc__ = "The control observable used to regulate data emission."


# Source items
ControllableSource = namedtuple('ControllableSource', ['id', 'observable'])
ControllableSource.__doc__ = "The regulated observable of a source."
ControllableSource.id.__doc__ = "The id of the source."
ControllableSource.observable.__doc__ = "The observable emitting source items."


def make_driver(sleep=time.sleep, loop=None):
    loop = loop or asyncio.get_event_loop()

    def driver(sink):
        def on_subscribe(observer, scheduler):
            _scheduler = scheduler or AsyncIOThreadSafeScheduler(loop)
            print(_scheduler)

            def on_next(i):
                if type(i) is Create:
                    observer.on_next(
                        ControllableSource(
                            i.id,
                            create_controllable_source(
                                source=i.observable,
                                control=i.control,
                                loop=loop,
                                sleep=sleep,
                            )
                        )
                    )

                else:
                    observer.on_error("app source unknown command: {}".format(i))

            disposable = sink.connector.subscribe(
                on_next=on_next,
                on_error=lambda e: _scheduler.schedule(lambda s, t: observer.on_error(e)),
                on_completed=lambda: _scheduler.schedule(lambda s, t: observer.on_completed()),
            )

            return disposable

        return Source(source=rx.create(on_subscribe))

    return Component(call=driver, input=Sink)
