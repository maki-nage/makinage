

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
