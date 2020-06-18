import asyncio
import functools
from collections import namedtuple


from cyclotron import Component
from cyclotron.asyncio.runner import run
from cyclotron.debug import trace_observable

import cyclotron_std.sys.argv as argv
import cyclotron_std.io.file as file
import cyclotron_aiohttp.http as http
import cyclotron_aiokafka as kafka


import rx
import rx.operators as ops
from rx.subject import Subject
from rx.scheduler.eventloop import AsyncIOScheduler

from makinage.config import read_config_from_args
from makinage.encoding.none import encoder as none_encoder
import makinage

ModelPublisherSource = namedtuple('ModelPublisherSource', ['kafka', 'http', 'file', 'argv'])
ModelPublisherSink = namedtuple('ModelPublisherSink', ['kafka', 'http', 'file'])
ModelPublisherDrivers = namedtuple('ModelPublisherDrivers', ['kafka', 'http', 'file', 'argv'])


def read_file(path):
    '''Temporary side effect until a correct file adapter is writen
    '''
    with open(path, mode="rb") as f:
        return f.read()


def create_model_readers(config):
    readers = {}

    for model in config['models']:
        readers[model['topic']] = rx.just(model['path']).pipe(
            ops.map(read_file),
        )

    return readers


def create_model_topics(config):
    topics = []
    readers = create_model_readers(config)

    encode_none, _ = none_encoder()

    for model in config['models']:
        topics.append(kafka.ProducerTopic(
            topic=model['topic'],
            records=readers[model['topic']],
            map_key=lambda i: None,
            encode=encode_none,
            map_partition=lambda i: i,
        ))

    return kafka.Producer(
        server=config['kafka']['endpoint'],
        topics=rx.from_(topics),
        max_request_size=32*1024*1024,
    )


def model_publisher(scheduler, sources):
    file_source = sources.file.response.pipe(ops.share())

    # kafka driver bootstrap. fixme
    kafka_source = sources.kafka.response.pipe(
        ops.do_action(print),
        ops.replay(),
        ops.ref_count(),
    )
    kafka_source.subscribe()

    config, config_read_request, http_request = read_config_from_args(
        sources.argv.argv,
        file_source,
        sources.http.response,
        scheduler=scheduler
    )

    config = config.pipe(ops.first())

    kafka_request = config.pipe(
        ops.map(lambda c: create_model_topics(c)),
    )

    return ModelPublisherSink(
        file=file.Sink(request=rx.merge(config_read_request)),
        http=http.Sink(request=http_request),
        kafka=kafka.Sink(request=kafka_request),
    )


def main():
    loop = asyncio.get_event_loop()
    aio_scheduler = AsyncIOScheduler(loop=loop)
    run(Component(call=functools.partial(model_publisher, aio_scheduler), input=ModelPublisherSource),
        ModelPublisherDrivers(
            kafka=kafka.make_driver(),
            http=http.make_driver(),
            file=file.make_driver(),
            argv=argv.make_driver(),
        )
    )
