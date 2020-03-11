import rx.operators as ops
from cyclotron.debug import trace_observable

def multiply(config, values):
    sink = values.pipe(
        trace_observable("multiply"),
        ops.map(lambda i: i * 2),
    )
    return sink,