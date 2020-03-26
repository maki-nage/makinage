from collections import namedtuple
from importlib import import_module
import rx
import rx.operators as ops

from cyclotron.debug import trace_observable
import cyclotron_aiokafka as kafka


def import_function(spec):
    spec = spec.split(':')
    module = spec[0]
    fn = spec[1]
    module = import_module(module)
    fn = getattr(module, fn)
    return fn


def initialize_topics(config_topics):
    ''' initializes the topic objects from a configuration

    If no encoding os provided, then the string encoder is used by default.
    '''
    Topic = namedtuple('Topic', ['encode', 'decode'])

    topics = {}
    for topic in config_topics:        
        if "encoder" in topic:
            module = import_module(topic['name'])
        else:
            module = import_module("makinage.encoding.string")
        encoder = getattr(module, "encoder")
        encode, decode = encoder()
        topics[topic['name']] = Topic(
            encode=encode,
            decode=decode,
        )

    return topics


def create_operators(config, config_source, kafka_source):
    ''' creates the operators declared in config

    Args:
        config: a dict containing the configuration file. todo: observable.
        kafka_source: The kafka response observable 
    Returns:
        An observable containing tuples of (topic, observable).
    '''
    try:
        topics = initialize_topics(config['topics'])
        producers = []
        consumers = []
        for k, operator in config['operators'].items():
            factory = import_function(operator['factory'])
            sources = []
            if 'sources' in operator:
                for source in operator['sources']:
                    print('create source {}'.format(source))
                    consumers.append(kafka.ConsumerTopic(
                        topic=source
                    ))
                    sources.append(kafka_source.pipe(
                        trace_observable(prefix="kafka source"),
                        ops.filter(lambda i: i.topic == source),  # CoonsumerRecords
                        ops.flat_map(lambda i: i.records),  # CoonsumerRecord
                        ops.map(lambda i: i.value),  # value
                        ops.map(topics[source].decode),
                    ))

            print(sources)
            sinks = factory(config_source, *sources)
            print("sinks: {}".format(sinks))
            for index, sink in enumerate(operator['sinks']):
                print('create sink {} at {}'.format(sink, index))
                producers.append(kafka.ProducerTopic(
                    topic=sink, 
                    records=sinks[index].pipe(
                        ops.map(topics[sink].encode),
                    ),
                    key_mapper=lambda i: i)
                )

        kafka_sink = []
        if len(consumers) > 0:
            kafka_sink.append(kafka.Consumer(
                server=config['kafka']['endpoint'],
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
        print(e)
