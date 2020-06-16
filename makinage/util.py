from importlib import import_module


def import_function(spec):
    spec = spec.split(':')
    module = spec[0]
    fn = spec[1]
    module = import_module(module)
    fn = getattr(module, fn)
    return fn
