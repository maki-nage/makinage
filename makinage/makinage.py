import asyncio
from collections import namedtuple
from functools import partial

import rx
import rx.operators as ops
from rx.scheduler.eventloop import AsyncIOScheduler

from cyclotron import Component
from cyclotron.debug import trace_observable
from cyclotron.asyncio.runner import run
import cyclotron_std.sys.argv as argv
import cyclotron_std.io.file as file
import cyclotron_aiohttp.http as http

from .config import read_config_from_args
from .operator import create_operators

import cyclotron_aiokafka as kafka

MakiNageSink = namedtuple('MakiNageSink', [
    'kafka', 'http', 'file',
])
MakiNageSource = namedtuple('MakiNageSource', [
    'kafka', 'http', 'file', 'argv',
])
MakiNageDrivers = namedtuple('MakiNageDrivers', [
    'kafka', 'http', 'file', 'argv'
])


Values = namedtuple('Values', ['id', 'observable'])


def makinage(aio_scheduler, sources):
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
    kafka_source.subscribe()

    kafka_request = first_config.pipe(
        ops.flat_map(lambda i: create_operators(
            i, config,
            kafka_source,
            sources.kafka.feedback.pipe(ops.share()),
        )),
        ops.subscribe_on(aio_scheduler),
        trace_observable("makinage"),
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
            http=http.make_driver(),
            file=file.make_driver(),
            argv=argv.make_driver(),
        ),
        loop=loop,
    )
