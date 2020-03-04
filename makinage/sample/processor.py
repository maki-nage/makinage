import rx.operators as ops


def multiply(config, values):
    sink = values.pipe(
        ops.map(lambda i: i * 2)
    )
    return sink