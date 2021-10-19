from makinage.util import import_function
from makinage.sample.functions import fn_no_param


def test_import():
    fn = import_function("makinage.sample.functions:fn_no_param")

    assert fn is fn_no_param
    assert fn() == 1


def test_import_kwargs():
    fn = import_function("makinage.sample.functions:fn_add", kwargs={"first": 1, "second": 2})

    assert fn() == 3


def test_import_kwargs_override():
    fn = import_function("makinage.sample.functions:fn_add_with_defaults", kwargs={"first": 3})

    assert fn() == 5
    assert fn(second=5) == 8
    assert fn(first=2) == 4
