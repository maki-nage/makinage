from makinage.serve.serve import create_transform_functions, infer

import numpy as np


def test_create_transform_default():
    c = {'config': {'serve': {}}}
    t = create_transform_functions(c)
    assert t.pre == np.array
    assert t.post == list


def test_predict():
    predict_count = 0

    def _predict(i):
        nonlocal predict_count
        predict_count += 1
        return np.array([i+1])

    c = {'config': {'serve': {
        'pre_transform': 'makinage.sample.serve:pre_transform',
        'post_transform': 'makinage.sample.serve:post_transform',
        'input_field': 'x', 'output_field': 'pred',
    }}}
    t = create_transform_functions(c)
    data = {'x': 42}
    actual_result = infer(data, t, _predict)

    assert predict_count == 1
    assert actual_result == {'x': 42, 'pred': [43]}
