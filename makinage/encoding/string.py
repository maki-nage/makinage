

def encoder(encoding="utf-8"):
    def _encode(i):
        return i.encode(encoding)

    def _decode(i):
        return i.decode(encoding)

    return _encode, _decode
