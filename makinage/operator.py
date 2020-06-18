import asyncio
import random
import traceback
from collections import namedtuple
from importlib import import_module

import rx
import rx.operators as ops
from rx.scheduler.eventloop import AsyncIOScheduler

from cyclotron.backpressure import pid
from cyclotron.debug import trace_observable
import cyclotron_aiokafka as kafka

from .util import import_function


def initialize_topics(config_topics):
    ''' initializes the topic objects from a configuration

    If no encoding os provided, then the string encoder is used by default.
    '''
    Topic = namedtuple('Topic', ['encode', 'decode', 'map_partition', 'start_from'])

    topics = {}
    for topic in config_topics:
        if "encoder" in topic:
            module = import_module(topic['encoder'])
        else:
            module = import_module("makinage.encoding.string")
        encoder = getattr(module, "encoder")
        encode, decode = encoder()

        if "partition_selector" in topic:
            map_partition = import_function(topic['partition_selector'])
        else:
            def r(i): return int(random.random() * 1000)
            map_partition = r

        if "start_from" in topic:
            start_from = topic['start_from']
        else:
            start_from = 'end'

        topics[topic['name']] = Topic(
            encode=encode,
            decode=decode,
            map_partition=map_partition,
            start_from=start_from,
        )

    return topics


def initialize_regulators(config, kafka_feedback):
    regulators = {}
    for regulator in config:
        control = kafka_feedback.pipe(
            trace_observable("regulator feedback"),
            ops.filter(lambda i: i[0] == regulator['feedback']),
            ops.map(lambda i: i[1] / 1000),

            pid(rx.concat(rx.just(1.0), rx.never()),
                -0.001, -0.001, 0.0),

            #ops.map(lambda i: 1/i if i != 0 else 1.0),
            ops.map(lambda i: max(min(i, 0.01), 0.0)),
            trace_observable("regulator"),
        )

        regulators[regulator['control']] = control

    return regulators


def create_source_observable(kafka_source, source):
    return kafka_source.pipe(
        trace_observable(prefix="kafka source {}".format(source)),
        ops.filter(lambda i: i.topic == source),  # ConsumerRecords
        trace_observable(prefix="kafka consumer records {}".format(source)),
        ops.flat_map(lambda i: i.records),  # ConsumerRecord
    )

def create_operators(config, config_source, kafka_source, kafka_feedback):
    ''' creates the operators declared in config

    Args:
        config: a dict containing the configuration file. todo: observable.
        kafka_source: The kafka response observable
        kafka_feedback: The kafka backpressure process feedback
    Returns:
        An observable containing tuples of (topic, observable).
    '''
    try:
        topics = initialize_topics(config['topics'])
        if 'regulators' in config:
            regulators = initialize_regulators(
                config['regulators'],
                kafka_feedback)
        else:
            regulators = {}
        producers = []
        consumers = []
        for k, operator in config['operators'].items():
            factory = import_function(operator['factory'])
            sources = []
            if 'sources' in operator:
                for source in operator['sources']:
                    print('create source {}'.format(source))
                    consumers.append(kafka.ConsumerTopic(
                        topic=source,
                        decode=topics[source].decode,
                        control=regulators[source] if source in regulators else None,
                        start_from=topics[source].start_from,
                    ))

                    sources.append(create_source_observable(kafka_source, source))

            print(sources)
            sinks = factory(config_source, *sources)
            print("sinks: {}".format(sinks))
            if 'sinks' in operator:
                for index, sink in enumerate(operator['sinks']):
                    print('create sink {} at {}'.format(sink, index))
                    producers.append(kafka.ProducerTopic(
                        topic=sink,
                        records=sinks[index],
                        map_key=lambda i: None,
                        encode=topics[sink].encode,
                        map_partition=topics[sink].map_partition
                    ))

        kafka_sink = []
        if len(consumers) > 0:
            kafka_sink.append(kafka.Consumer(
                server=config['kafka']['endpoint'],
                group=config['application']['name'],
                topics=rx.from_(set(consumers)),
            ))

        if len(producers) > 0:
            kafka_sink.append(kafka.Producer(
                server=config['kafka']['endpoint'],
                topics=rx.from_(producers)
            ))

        kafka_sink = rx.from_(kafka_sink) if len(kafka_sink) > 0 else rx.never()
        return kafka_sink
    except Exception as e:
        print("Error while creating operators: {}, {}".format(
              e, traceback.format_exc()))
