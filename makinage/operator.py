from importlib import import_module
import rx
import rx.operators as ops

from cyclotron.debug import trace_observable

def import_function(spec):
    spec = spec.split(':')
    module = spec[0]
    fn = spec[1]
    module = import_module(module)
    fn = getattr(module, fn)
    return fn


def create_operators(config, kafka_source):
    ''' creates the operators declared in config

    Returns:
        An observable containing tuples of (topic, observable).
    '''
    try:
        kafka_sink_items = []
        for k, operator in config['operators'].items():
            factory = import_function(operator['factory'])
            sources = []        
            for source in operator['sources']:
                print('create source {}'.format(source))
                sources.append(kafka_source.pipe(
                    ops.filter(lambda i: i.id == source),
                    ops.flat_map(lambda i: i.observable),
                ))
            
            print(sources)
            sinks = factory(config, *sources)
            print("sinks: {}".format(sinks))
            for index, sink in enumerate(operator['sinks']):
                print('create sink {} at {}'.format(sink, index))
                kafka_sink_items.append((sink, sinks[index]))
        
        kafka_sink = rx.from_(kafka_sink_items)
        return kafka_sink
    except Exception as e:
        print(e)
