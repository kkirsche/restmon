'''Client is responsible for actively monitoring API endpoints.

This module is used to collect a list of REST API endpoints and iterate over
them to log the response content (if it exists), status code, and round trip
time of the request.
'''

from restmon.log import setup_logger
from requests.packages.urllib3.util import Retry
from requests.adapters import HTTPAdapter
from requests import Session
from raven import fetch_package_version
from time import time
from json import dumps
from flatten_json import flatten


class RestmonClient(object):
    '''RestmonClient is used to monitor API endpoints.

    Attributes:
        endpoints (:obj:`list` of :obj:`str`): A list of API URIs to monitor
        logger (:obj:`logging.Logger`): A logging engine
    '''

    def __init__(self, base_uri, endpoints=[], auth=(), environment='prod'):
        '''Initialize a new restmon client.

        Args:
            endpoints (:obj:`list` of :obj:`str`): A list of API URIs to monitor
        '''
        self.logger = setup_logger()
        if base_uri.endswith('/'):
            base_uri = base_uri[:-1]
        self.base_uri = base_uri
        self.auth = auth

        self.api_client = self.setup_api_client()
        self.endpoints = endpoints
        self.logger.debug(
            ('application=restmon environment={env} msg=endpoints configured'
             'endpoints={endpoints}').format(
                 env=environment, endpoints=self.endpoints))
        self.environment = environment
        self.headers = {
            'User-Agent':
            'restmon/{version}'.format(version=fetch_package_version('restmon'))
        }

    def setup_api_client(self):
        api_client = Session()
        adapter = HTTPAdapter(
            max_retries=Retry(
                total=10,
                backoff_factor=3,
                status_forcelist=[404, 408, 409, 500, 502, 503, 504]),
            pool_maxsize=20)
        api_client.auth = self.auth
        api_client.mount(self.base_uri, adapter)

        return api_client

    def run(self):
        self.logger.debug(('application=restmon environment={env} '
                           'msg=start MonitorClient.run').format(
                               env=self.environment))
        for endpoint in self.endpoints:
            self.logger.debug(('application=restmon environment={env} '
                               'endpoint={ep} msg=querying endpoint').format(
                                   ep=endpoint, env=self.environment))
            if endpoint.startswith('/'):
                endpoint = endpoint[1:]
            start = time()
            try:
                r = self.api_client.get(
                    url='{base}/{endpoint}'.format(
                        base=self.base_uri, endpoint=endpoint),
                    timeout=5,
                    headers=self.headers)
                end = time()
                rtt = end - start
                try:
                    j = r.json()
                    line = [
                        'environment={env}'.format(env=self.environment),
                        'application=restmon', 'msg=received response',
                        'time_unit=sec', 'start_time={start}'.format(
                            start=start), 'end_time={end}'.format(end=end),
                        'rtt={rtt}'.format(rtt=rtt), 'response={resp}'.format(
                            resp=dumps(j)), 'endpoint={endpoint}'.format(
                                endpoint=endpoint)
                    ]
                    flat_j = flatten(j)
                    for k, v in list(flat_j.items()):
                        line.append('{k}={v}'.format(k=k, v=v))
                    self.logger.info(' '.join(line))
                except Exception:
                    self.logger.info((
                        'environment={env} msg=received response '
                        'application=restmon time_unit=sec  start_time={start} '
                        'endpoint={endpoint} end_time={end} rtt={rtt} '
                        'response={resp}').format(
                            env=self.environment,
                            rtt=rtt,
                            endpoint=endpoint,
                            resp=r.text,
                            start=start,
                            end=end))
            except Exception as e:
                end = time()
                rtt = end - start
                self.logger.error(
                    ('application=restmon environment={env} '
                     'msg=failed to query endpoint with error error={e} '
                     'start_time={start} end_time={end} rtt={rtt} '
                     'time_unit=sec').format(
                         env=self.environment,
                         e=repr(e),
                         start=start,
                         end=end,
                         rtt=rtt),
                    exc_info=True)

        self.logger.debug(('application=restmon environment={e} '
                           'msg=end MonitorClient.run').format(
                               e=self.environment))
