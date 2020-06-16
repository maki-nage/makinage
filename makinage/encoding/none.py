import json


def encoder():
    ''' None encoder

    Keep the payload as-is.
    '''
    def _encode(i):
        return i

    def _decode(i):
        return i

    return _encode, _decode
