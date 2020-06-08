import rx

import cyclotron_std.argparse as argparse
import makinage.config as config


def test_parse_arguments():
    source = rx.from_(['makinage', '--config', 'test.yaml'])
    actual_result = []
    actual_error = []
    config.parse_arguments(source).subscribe(
        on_next=actual_result.append,
        on_error=actual_error.append,
    )

    assert actual_result == [argparse.Argument(key='config', value='test.yaml')]
