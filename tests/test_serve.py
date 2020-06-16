from makinage.serve.serve import create_prepare_function, predict
import makinage.sample.serve as sample_serve

import numpy as np


def test_create_prepare_present():
    c = { 'config': { 'serve': { 'prepare': 'makinage.sample.serve:prepare_function'}}}
    f = create_prepare_function(c)
    assert f == sample_serve.prepare_function


def test_create_prepare_default():
    c = { 'config': { 'serve': {}}}
    f = create_prepare_function(c)
    assert f == np.array


def test_predict():
    predict_count = 0
    def _predict(i):
        nonlocal predict_count
        predict_count += 1
        return np.array([i+1])

    c = { 'config': { 'serve': { 'input_field': 'x', 'output_field': 'pred'}}}
    data = { 'x': 42 }
    actual_result = predict(data, c, _predict)

    assert predict_count == 1
    assert actual_result == { 'x': 42, 'pred': [43] }

    
