import asyncio
import random
import traceback
from collections import namedtuple
from importlib import import_module

import rx
import rx.operators as ops
from rx.scheduler.eventloop import AsyncIOScheduler

import rxx

from cyclotron.backpressure import pid
from cyclotron.debug import trace_observable
import cyclotron_aiokafka as kafka

from .driver import app_sink, app_source
from .util import import_function


def initialize_topics(config_topics):
    ''' initializes the topic objects from a configuration

    If no encoding os provided, then the string encoder is used by default.
    '''
    Topic = namedtuple('Topic', [
        'name',
        'encode', 'decode',
        'map_partition',
        'start_from', 'timestamp_mapper',
        'merge_lookup_depth',
    ])

    pull_mode = False
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

        if "timestamp_mapper" in topic:
            timestamp_mapper = import_function(topic['timestamp_mapper'])
            pull_mode = True
        else:
            timestamp_mapper = None

        merge_lookup_depth = 1
        if "merge_lookup_depth" in topic:
            merge_lookup_depth = topic['merge_lookup_depth']

        topics[topic['name']] = Topic(
            name=topic['name'],
            encode=encode,
            decode=decode,
            map_partition=map_partition,
            start_from=start_from,
            timestamp_mapper=timestamp_mapper,
            merge_lookup_depth=merge_lookup_depth,
        )

    return topics, pull_mode


def subscribe_to_non_regulated_sinks(config, sinks_feedback):
    """Ensures that all feedback observable are subscribed

    The app sinks start consuming when their feedback observable is subscribed.
    So we subscribe to all sinks that are not regulated.
    """
    if 'sinks' not in config:
        return

    regulated_sinks = []
    if 'regulators' in config:
        for regulator in config['regulators']:
            regulated_sinks.append(regulator['feedback'])

    sinks_feedback.pipe(
        ops.filter(lambda i: i.id not in regulated_sinks),
        ops.flat_map(lambda i: i.observable),
    ).subscribe()


def initialize_regulators(config, kafka_feedback, app_feedback):
    regulators = {}
    app_regulators = []
    for regulator in config['regulators']:
        app = False
        if 'sinks' in config and regulator['feedback'] in config['sinks']:  # app sink feedback
            app = True
            feedback = app_feedback.pipe(
                ops.filter(lambda i: i.id == regulator['feedback']),
                ops.flat_map(lambda i: i.observable),
            )
        else:  # kafka sink feedback
            feedback = kafka_feedback.pipe(
                ops.filter(lambda i: i[0] == regulator['feedback']),
            )

        control = feedback.pipe(
            ops.map(lambda i: i[1] / 1000),
            pid(rx.concat(rx.just(1.0), rx.never()), -0.001, -0.001, 0.0),
            ops.map(lambda i: max(min(i, 0.01), 0.0)),
            ops.filter(lambda i: i > 0),
            ops.share(),
        )

        regulators[regulator['control']] = control
        if app:
            app_regulators.append(control)

    for app_control in app_regulators:
        app_control.subscribe()

    print(regulators)
    return regulators


def create_kafka_source_observable(kafka_source, topic):
    if topic.timestamp_mapper is not None:  # pull mode
        return kafka_source.pipe(
            ops.filter(lambda i: i.topic == topic.name),  # ConsumerRecords
            ops.flat_map(lambda i: i.records.pipe(
                rxx.pullable.sorted_merge(
                    key_mapper=topic.timestamp_mapper,
                    lookup_size=topic.merge_lookup_depth,
                ),
                rxx.pullable.push(),
            )),
        )
    else:  # push mode
        return kafka_source.pipe(
            trace_observable("kafka tst"),
            ops.filter(lambda i: i.topic == topic.name),  # ConsumerRecords
            ops.flat_map(lambda i: i.records.pipe(
                ops.merge_all(),
            )),
        )


def create_app_source_observable(source, source_id):
    return source.pipe(
        ops.filter(lambda i: i.id == source_id),  # ConsumerRecords
        ops.flat_map(lambda i: i.observable),
    )


def create_operators(config, config_source,
                     kafka_source, kafka_feedback,
                     app_source_data, app_feedback):
    ''' creates the operators declared in config

    Args:
        config: a dict containing the initial configuration file.
        config_source: An observable emitting configuration items.
        kafka_source: The kafka response observable
        kafka_feedback: The kafka backpressure process feedback
    Returns:
        An observable containing tuples of (topic, observable).
    '''
    try:
        source_type = kafka.DataSourceType.STREAM
        if "source_type" in config['application']:
            source_type = kafka.DataSourceType.BATCH if config['application']['source_type'] == "batch" else kafka.DataSourceType.STREAM

        topics, pull_mode = initialize_topics(config['topics'])
        datafeed_mode = kafka.DataFeedMode.PULL if pull_mode is True else kafka.DataFeedMode.PUSH
        if 'regulators' in config:
            regulators = initialize_regulators(
                config,
                kafka_feedback, app_feedback,
            )
        else:
            regulators = {}

        subscribe_to_non_regulated_sinks(config, app_feedback)

        producers = []
        consumers = []
        source_connectors = []
        sink_connectors = []
        for k, operator in config['operators'].items():
            factory = import_function(operator['factory'])
            sources = []
            if 'sources' in operator:
                for source in operator['sources']:
                    print('create source {}'.format(source))
                    if 'sources' in config and source in config['sources']:  # This is an app connector
                        source_config = config['sources'][source]
                        source_kwargs = source_config.get('kwargs', None)
                        source_factory = import_function(source_config['factory'], source_kwargs)
                        source_observable = source_factory()
                        source_connectors.append(app_source.Create(
                            id=source,
                            observable=source_observable,
                            control=regulators[source] if source in regulators else None,
                        ))

                        sources.append(create_app_source_observable(
                            app_source_data, source
                        ))
                    else:  # A kafka source
                        consumers.append(kafka.ConsumerTopic(
                            topic=source,
                            decode=topics[source].decode,
                            control=regulators[source] if source in regulators else None,
                            start_from=topics[source].start_from,
                        ))

                        sources.append(create_kafka_source_observable(
                            kafka_source, topics[source],
                        ))

            print(sources)
            sinks = factory(config_source, *sources)
            print("sinks: {}".format(sinks))
            if 'sinks' in operator:
                for index, sink in enumerate(operator['sinks']):
                    print('create sink {} at {}'.format(sink, index))
                    if 'sinks' in config and sink in config['sinks']:  # This is an app connector
                        sink_config = config['sinks'][sink]
                        sink_kwargs = sink_config.get('kwargs', None)
                        sink_factory = import_function(sink_config['factory'], sink_kwargs)
                        sink_operator = sink_factory()
                        sink_connectors.append(app_sink.Create(
                            id=sink,
                            operator=sink_operator,
                            observable=sinks[index],
                        ))
                    else:  # Kafka
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
                max_partition_fetch_bytes=config['application'].get('max_partition_fetch_bytes', 512000000),
                topics=rx.from_(set(consumers)),
                source_type=source_type,
                feed_mode=datafeed_mode,
            ))

        if len(producers) > 0:
            kafka_sink.append(kafka.Producer(
                server=config['kafka']['endpoint'],
                max_request_size=config['application'].get('max_request_size', 512000000),
                topics=rx.from_(producers)
            ))

        kafka_sink = rx.from_(kafka_sink) if len(kafka_sink) > 0 else rx.never()
        source_connectors = rx.from_(source_connectors) if len(source_connectors) > 0 else rx.never()
        sink_connectors = rx.from_(sink_connectors) if len(sink_connectors) > 0 else rx.never()

        return rx.just((kafka_sink, source_connectors, sink_connectors))

    except Exception as e:
        print("Error while creating operators: {}, {}".format(
              e, traceback.format_exc()))
