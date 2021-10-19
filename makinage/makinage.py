import asyncio
from collections import namedtuple
from functools import partial

import rx
import rx.operators as ops
from rx.disposable import Disposable
from rx.scheduler.eventloop import AsyncIOScheduler

from cyclotron import Component
from cyclotron.debug import trace_observable
from cyclotron.asyncio.runner import run
import cyclotron_std.sys.argv as argv
import cyclotron_std.io.file as file
import cyclotron_aiohttp.http as http

from .driver import app_sink, app_source

from .config import read_config_from_args
from .operator import create_operators

import cyclotron_aiokafka as kafka

MakiNageSink = namedtuple('MakiNageSink', [
    'kafka', 'app_sink', 'app_source', 'http', 'file',
])
MakiNageSource = namedtuple('MakiNageSource', [
    'kafka', 'app_sink', 'app_source', 'http', 'file', 'argv',
])
MakiNageDrivers = namedtuple('MakiNageDrivers', [
    'kafka', 'app_sink', 'app_source', 'http', 'file', 'argv'
])

Values = namedtuple('Values', ['id', 'observable'])


def ref_count(subscribe_count=1, unsubscribe_count=0):
    connectable_subscription = None
    count = 0

    def _ref_count(source):
        def subscribe(observer, scheduler=None):
            nonlocal connectable_subscription
            nonlocal count
            count += 1
            should_connect = count == subscribe_count
            subscription = source.subscribe(observer, scheduler=scheduler)
            if should_connect:
                connectable_subscription = source.connect(scheduler)

            def dispose():
                nonlocal connectable_subscription
                nonlocal count
                subscription.dispose()
                count -= 1
                if count == unsubscribe_count:
                    connectable_subscription.dispose()

            return Disposable(dispose)

        return rx.create(subscribe)

    return _ref_count


def makinage(aio_scheduler, sources):
    def on_error(e):
        raise e

    config, read_request, http_request = read_config_from_args(
        sources.argv.argv,
        sources.file.response,
        sources.http.response,
        scheduler=aio_scheduler
    )

    first_config = rx.concat(config.pipe(ops.take(1),), rx.never())

    kafka_source = sources.kafka.response.pipe(
        trace_observable("kafka source1"),
        ops.replay(),
        ops.ref_count(),
        trace_observable("kafka source2"),
    )
    kafka_source.subscribe(on_error=on_error)

    app_sources = sources.app_source.source.pipe(
        trace_observable("app source1"),
        ops.replay(),
        ops.ref_count(),
        trace_observable("app source2"),
    )
    app_sources.subscribe(on_error=on_error)

    app_sinks_feedback = sources.app_sink.feedback
    operators_sinks = first_config.pipe(
        ops.flat_map(lambda i: create_operators(
            i, config,
            kafka_source,
            sources.kafka.feedback.pipe(ops.share()),
            app_sources, app_sinks_feedback,
        )),
        ops.subscribe_on(aio_scheduler),
        ops.publish(),
        ref_count(subscribe_count=3),
        #trace_observable("makinage"),
    )

    kafka_request = operators_sinks.pipe(
        ops.flat_map(lambda i: i[0]),
        trace_observable("kafka_request"),
    )
    source_connectors = operators_sinks.pipe(
        ops.flat_map(lambda i: i[1]),
        trace_observable("source_connectors"),
    )
    sink_connectors = operators_sinks.pipe(
        ops.flat_map(lambda i: i[2]),
        trace_observable("sink_connectors"),
    )

    '''
    config.pipe(ops.subscribe_on(aio_scheduler)).subscribe(
        on_next=print,
        on_error=print,
    )
    '''

    return MakiNageSink(
        file=file.Sink(request=read_request),
        http=http.Sink(request=http_request),
        app_source=app_source.Sink(connector=source_connectors),
        app_sink=app_sink.Sink(connector=sink_connectors),
        kafka=kafka.Sink(request=kafka_request),
    )


def main():
    loop = asyncio.get_event_loop()
    # loop.set_debug(True)
    aio_scheduler = AsyncIOScheduler(loop=loop)
    run(
        Component(
            call=partial(makinage, aio_scheduler),
            input=MakiNageSource),
        MakiNageDrivers(
            kafka=kafka.make_driver(),
            app_sink=app_sink.make_driver(),
            app_source=app_source.make_driver(),
            http=http.make_driver(),
            file=file.make_driver(),
            argv=argv.make_driver(),
        ),
        loop=loop,
    )
