from collections import namedtuple


def pre_transform(config):
    field = config['config']['serve']['input_field']

    def _transform(i):
        return i, [ii[field] for ii in i]

    return _transform


def post_transform(config):
    field = config['config']['serve']['output_field']

    def _transform(i, pred):
        for ii, p in zip(i, pred):
            ii[field] = pred
        return i

    return _transform


def predict_zero(i):
    return i*0.0


ZeroModel = namedtuple('ZeroModel', ['predict'])
ZeroModel.__new__.__defaults__ = (predict_zero,)


def predict(model, config):
    ratio = config['config']['serve']['ratio']

    def _predict(i):
        predictions = model.predict(i)
        return [(predictions[index], i[index]*ratio) for index in range(len(i))]

    return _predict
