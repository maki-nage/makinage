import pickle


def encoder():
    ''' Pickle encoder

    encodes any object using the pickle format
    '''
    def _encode(i):
        return pickle.dumps(i)

    def _decode(i):
        return pickle.loads(i)

    return _encode, _decode
