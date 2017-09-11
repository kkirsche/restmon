from restmon.client import RestmonClient
from argparse import ArgumentParser


def main():
    parser = ArgumentParser(description='Elasticsearch monitoring client')
    parser.add_argument(
        '--base-uri',
        '-b',
        dest='base_uri',
        nargs='?',
        required=True,
        help='The base URI to use (e.g. http://example.com)')
    parser.add_argument(
        '--endpoints',
        '-e',
        dest='endpoints',
        nargs='+',
        help='API endpoint URIs for monitoring (e.g. api/endpoint1)')
    parser.add_argument(
        '--username',
        '-u',
        dest='username',
        nargs='?',
        help='Basic authentication username (optional)')
    parser.add_argument(
        '--password',
        '-p',
        dest='password',
        nargs='?',
        help='Basic authentication password (optional)')

    parser.add_argument(
        '--environment',
        '-n',
        dest='environment',
        nargs='?',
        default='prod',
        help=
        'the environment in which this tool is running (e.g. prod) (optional)')

    args = parser.parse_args()

    auth = ()

    if args.username and args.password:
        auth = (args.username, args.password)

    client = RestmonClient(
        base_uri=args.base_uri,
        environment=args.environment,
        endpoints=args.endpoints,
        auth=auth)
    client.run()
