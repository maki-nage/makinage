import os
import pytest
from collections import namedtuple

from rx.subject import Subject

pytest.importorskip("makinage.serve.serve")
from makinage.serve.serve import create_transform_functions, \
    create_model_predict, infer, serve
from makinage.sample.serve import ZeroModel, predict_zero

import numpy as np

zero_model_dirname = os.path.join('assets', 'zero_mlflow_pyfunc.zip')


def test_create_transform_default():
    c = {'config': {'serve': {}}}
    t = create_transform_functions(c)
    assert t.pre([1]) == ([1], np.array([1]))
    assert t.post([1], [1]) == [(1, 1)]


def test_create_predict_default():
    c = {'config': {'serve': {}}}
    model = ZeroModel()
    p = create_model_predict(model, c)
    assert p is predict_zero


def test_create_predict_custom():
    c = {'config': {'serve': {
        'predict': 'makinage.sample.serve:predict',
        'ratio': 2,
    }}}

    model = ZeroModel()
    p = create_model_predict(model, c)
    assert p(np.array([2])) == [(0.0, 4)]


def test_predict():
    predict_count = 0

    def _predict(i):
        nonlocal predict_count
        predict_count += len(i)
        return np.array([i]) + 1

    c = {'config': {'serve': {
        'pre_transform': 'makinage.sample.serve:pre_transform',
        'post_transform': 'makinage.sample.serve:post_transform',
        'input_field': 'x', 'output_field': 'pred',
    }}}
    t = create_transform_functions(c)
    data = [
        {'x': 42},
    ]
    actual_result = infer(data, t, _predict)

    assert predict_count == 1
    assert actual_result == [
        {'x': 42, 'pred': [43]}
    ]


def test_serve():
    config = Subject()
    model = Subject()
    data = Subject()
    prediction, = serve(config, model, data)

    actual_predictions = []
    prediction.subscribe(on_next=actual_predictions.append)

    config.on_next({'config': {'serve': {
        'predict': 'makinage.sample.serve:predict',
        'ratio': 2,
    }}})

    with open(zero_model_dirname, 'rb') as fd:
        model_archive_data = fd.read()
    model.on_next(model_archive_data)

    data.on_next(1)
    assert actual_predictions == [
        (1, (0.0, 2))
    ]

    # update config
    actual_predictions.clear()
    config.on_next({'config': {'serve': {
        'predict': 'makinage.sample.serve:predict',
        'ratio': 3,
    }}})

    data.on_next(1)
    assert actual_predictions == [
        (1, (0.0, 3))
    ]


def test_serve_batch():
    config = Subject()
    model = Subject()
    data = Subject()
    prediction, = serve(config, model, data)

    actual_predictions = []
    prediction.subscribe(on_next=actual_predictions.append)

    config.on_next({'config': {'serve': {
        'predict': 'makinage.sample.serve:predict',
        'batch': 3,
        'ratio': 2,
    }}})

    with open(zero_model_dirname, 'rb') as fd:
        model_archive_data = fd.read()
    model.on_next(model_archive_data)

    data.on_next(1)
    data.on_next(1)
    assert actual_predictions == []

    data.on_next(1)
    assert actual_predictions == [
        (1, (0.0, 2)),
        (1, (0.0, 2)),
        (1, (0.0, 2)),
    ]
