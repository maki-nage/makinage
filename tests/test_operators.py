from makinage.operator import initialize_topics


def test_init_topics():
    config_topics = [
        {'name': 'foo'},
        {'name': 'bar', 'encoder': 'makinage.encoding.string'},
    ]
    topics, pull_mode = initialize_topics(config_topics)
    assert pull_mode is False
    assert b'test' == topics['foo'].encode('test')
    assert 'test' == topics['foo'].decode(b'test')

    assert b'test' == topics['bar'].encode('test')
    assert 'test' == topics['bar'].decode(b'test')
