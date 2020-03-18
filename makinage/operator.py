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


def create_operators(config, kafka_source):
    ''' creates the operators declared in config

    Args:
        config: a dict containing the configuration file. todo: observable.
        kafka_source: The kafka response observable 
    Returns:
        An observable containing tuples of (topic, observable).
    '''
    try:
        producers = []
        consumers = []
        for k, operator in config['operators'].items():
            factory = import_function(operator['factory'])
            sources = []        
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
                ))
            
            print(sources)
            sinks = factory(config, *sources)
            print("sinks: {}".format(sinks))
            for index, sink in enumerate(operator['sinks']):
                print('create sink {} at {}'.format(sink, index))
                producers.append(kafka.ProducerTopic(
                    topic=sink, 
                    records=sinks[index],
                    key_mapper=lambda i: i))

        consumer = kafka.Consumer(
            server=config['kafka']['endpoint'],
            topics=rx.from_(set(consumers)),
        )

        producer = kafka.Producer(
            server=config['kafka']['endpoint'],
            topics=rx.from_(producers)
        )

        kafka_sink = rx.from_([consumer, producer])
        return kafka_sink
    except Exception as e:
        print(e)
