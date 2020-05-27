import json


def encoder():
    ''' Json encoder

    encodes any object using the JSON format
    '''
    def _encode(i):
        return json.dumps(i).encode()

    def _decode(i):
        return json.loads(i.decode())

    return _encode, _decode
