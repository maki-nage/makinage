import yaml
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
        ops.map(yaml.load),
        ops.share()
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


def read_config_from_args(argv, file):
    args = parse_arguments(argv)

    read_request, read_response = args.pipe(
        ops.map(lambda i: file.Read(id='config', path=i.value)),
        file.read(sources.file.response),
    )
    read_request = read_request.pipe(
        ops.subscribe_on(aio_scheduler),
    )
    config = parse_config(read_response)
    return config
