import rx.operators as ops
from cyclotron.debug import trace_observable

def multiply(config, values):
    sink = values.pipe(
        trace_observable("multiply"),
        ops.map(lambda i: int(i.decode('utf-8')) * 2),
        trace_observable("multiplied"),
        ops.map(lambda i: str(i).encode('utf-8'))
    )
    return sink,