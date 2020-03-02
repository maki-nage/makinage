import asyncio
from collections import namedtuple
from functools import partial

from cyclotron import Component
from cyclotron.asyncio.runner import run

from .config import read_config_from_args

MakiNageSink = namedtuple('MakiNageSink', [
     'file',
])
MakiNageSource = namedtuple('MakiNageSource', [
    'file', 'argv',
])
MakiNageDrivers = namedtuple('MakiNageDrivers', [
    'file', 'argv'
])


def makinage(aio_scheduler, sources):
    config, read_request = read_config_from_args(
        sources.argv.argv,
        sources.file.response
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
            call=partial(deepspeech_server, aio_scheduler),
            input=DeepspeechSource),
        DeepspeechDrivers(
            deepspeech=deepspeech.make_driver(),
            httpd=httpd.make_driver(),
            argv=argv.make_driver(),
            logging=logging.make_driver(),
            file=file.make_driver()
        ),
        loop=loop,
    )
