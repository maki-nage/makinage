import asyncio
from collections import namedtuple
from functools import partial

import rx
import rx.operators as ops
from rx.scheduler.eventloop import AsyncIOScheduler
from cyclotron import Component
from cyclotron.asyncio.runner import run
import cyclotron_std.io.file as file
import cyclotron_std.sys.argv as argv

from .config import read_config_from_args
from .operator import create_operators


MakiNageSink = namedtuple('MakiNageSink', [
     'file',
])
MakiNageSource = namedtuple('MakiNageSource', [
    'file', 'argv',
])
MakiNageDrivers = namedtuple('MakiNageDrivers', [
    'file', 'argv'
])


Values = namedtuple('Values', ['id', 'observable'])

def makinage(aio_scheduler, sources):
    config, read_request = read_config_from_args(
        sources.argv.argv,
        sources.file.response,
        scheduler=aio_scheduler
    )
    
    config.pipe(
        ops.take(1),
        ops.map(lambda i: create_operators(i, rx.just(
            Values(id='values', observable=rx.from_([1, 2]))
        )))
    ).subscribe(
        on_next=print,
        on_error=print,
    )

    config.pipe(ops.subscribe_on(aio_scheduler)).subscribe(
        on_next=print,
        on_error=print,
    )

    return MakiNageSink(
        file=file.Sink(request=read_request),
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
            file=file.make_driver(),
            argv=argv.make_driver(),
        ),
        loop=loop,
    )
