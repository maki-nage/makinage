from makinage.encoding.json import encoder

def test_json_encoder():
    encode, decode = encoder()

    assert encode({"foo": "bar", "biz": 42}) == b'{"foo": "bar", "biz": 42}'
    assert decode(b'{"foo": "bar", "biz": 42}') == {"foo": "bar", "biz": 42}
