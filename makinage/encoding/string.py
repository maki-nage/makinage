

def encoder(encoding="utf-8"):
    ''' String encoder

    encodes strings using the encode/decode python functions

    Args:
        encoding: The string encoding to use.
    '''
    def _encode(i):
        return i.encode(encoding)

    def _decode(i):
        return i.decode(encoding)

    return _encode, _decode
