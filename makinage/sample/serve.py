from collections import namedtuple


def pre_transform(config):
    field = config['config']['serve']['input_field']

    def _transform(i):
        return i[field]

    return _transform


def post_transform(config):
    field = config['config']['serve']['output_field']

    def _transform(i, pred):
        i[field] = pred
        return i

    return _transform


def predict_zero(i):
    return [0.0]


ZeroModel = namedtuple('ZeroModel', ['predict'])
ZeroModel.__new__.__defaults__ = (predict_zero,)


def predict(model, config):
    ratio = config['config']['serve']['ratio']

    def _predict(i):
        print(type(model))
        return (model.predict(i)[0], i*ratio)

    return _predict
