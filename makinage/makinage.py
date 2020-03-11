import asyncio
from collections import namedtuple
from functools import partial

import rx
import rx.operators as ops
from rx.scheduler.eventloop import AsyncIOScheduler
from cyclotron import Component
from cyclotron.debug import trace_observable
from cyclotron.asyncio.runner import run
import cyclotron_std.io.file as file
import cyclotron_std.sys.argv as argv

from .config import read_config_from_args
from .operator import create_operators

import cyclotron_aiokafka as kafka

MakiNageSink = namedtuple('MakiNageSink', [
    'kafka', 'file',
])
MakiNageSource = namedtuple('MakiNageSource', [
    'kafka', 'file', 'argv',
])
MakiNageDrivers = namedtuple('MakiNageDrivers', [
    'kafka', 'file', 'argv'
])


Values = namedtuple('Values', ['id', 'observable'])

def makinage(aio_scheduler, sources):
    config, read_request = read_config_from_args(
        sources.argv.argv,
        sources.file.response,
        scheduler=aio_scheduler
    )
    
    kafka_request = config.pipe(
        ops.take(1),
        ops.flat_map(lambda i: create_operators(i, sources.kafka.response)),
        trace_observable("makinage1"),
        #ops.filter(lambda i: type(i) is kafka.Producer),
        #ops.flat_map(lambda i: i.topics),
        #ops.flat_map(lambda i: i.records),
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
            file=file.make_driver(),
            argv=argv.make_driver(),
        ),
        loop=loop,
    )
