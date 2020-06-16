from makinage.encoding.none import encoder

def test_none_encoder():
    encode, decode = encoder()

    data = b'azertyuiopqsdfghjll123'
    assert encode(data) == data
    assert decode(data) == data
