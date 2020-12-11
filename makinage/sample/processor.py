import rx.operators as ops
import rxsci as rs
from cyclotron.debug import trace_observable


def multiply(config, values):
    sink = values.pipe(
        trace_observable("multiply 1"),
        rs.ops.with_latest_from(config),
        ops.starmap(lambda i, c: int(i) * c['config']['multiply']),
        trace_observable("multiplied"),
        ops.map(lambda i: str(i))
    )
    return sink,
