import functools
from importlib import import_module


def import_function(spec, kwargs=None):
    """Loads a function definition from a string specs

    Args:
        - spec: Function speciciation in the form "module:function"
        - kwargs: [Optional] a dict containing the kwargs

    Returns:
        A function object partialy applied with kwargs
    """
    spec = spec.split(':')
    module = spec[0]
    fn = spec[1]
    module = import_module(module)
    fn = getattr(module, fn)

    if kwargs is not None:
        fn = functools.partial(fn, **kwargs)
    return fn
