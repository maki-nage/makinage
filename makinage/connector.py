from collections import namedtuple
from .util import import_function


ConnectorSource = namedtuple('ConnectorSource', [
    'id',
    'source_control',
    'source_observable',
])
ConnectorSink = namedtuple('ConnectorSink', [
    'id',
    'operator',
    'sink_observable',
    'sink_feedback'
])

def create_connectors(config, config_source):
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
        sources = []
        sinks = []
        if 'sources' in config:
            for k, source in config['sources'].items():
                factory = import_function(source['factory'])
                sources.append(ConnectorSource(
                    id=k,
                    observable=factory(config_source),
                ))

        if 'sinks' in config:
            for k, source in config['sources'].items():
                factory = import_function(source['factory'])
                sources.append(factory(config_source))

    except Exception as e:
        print("Error while creating connectors: {}, {}".format(
              e, traceback.format_exc()))