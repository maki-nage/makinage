import yaml

import rx.operators as ops

import cyclotron_std.sys.argv as argv
import cyclotron_std.argparse as argparse
import cyclotron_std.io.file as file


def parse_config(config_data):
    ''' takes a stream with the content of the configuration file as input
    and returns a (hot) stream of arguments .
    '''
    config = config_data.pipe(
        ops.filter(lambda i: i.id == "config"),
        ops.flat_map(lambda i: i.data),
        ops.map(lambda i:yaml.load(i, Loader=yaml.FullLoader)),
        ops.replay(buffer_size=1),
        ops.ref_count(),
    )

    return config


def parse_arguments(argv, prog=None):
    parser = argparse.ArgumentParser(prog=prog)
    parser.add_argument(
        '--config', required=True,
        help="Path of the configuration file")

    return argv.pipe(
        ops.skip(1),
        argparse.parse(parser),
    )


def read_config_from_args(argv, file_response, scheduler=None):
    args = parse_arguments(argv)

    read_request, read_response = args.pipe(
        ops.map(lambda i: file.Read(id='config', path=i.value)),
        file.read(file_response),
    )

    if scheduler is not None:
        read_request = read_request.pipe(
            ops.subscribe_on(scheduler),
        )

    config = parse_config(read_response)
    return config, read_request
