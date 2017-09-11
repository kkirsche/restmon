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
from urlparse import urlparse
from socket import gethostbyname


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
        api_client.auth = self.auth
        return api_client

    def _dns_resolution(self, uri):
        self.logger.debug(('application=dnsmon environment={env} '
                           'msg=start MonitorClient._dns_resolution').format(
                               env=self.environment))
        parsed = urlparse(uri)
        name = uri[len(parsed.scheme) + 3:].split(':')[0]
        start = time()
        failed = False
        try:
            gethostbyname(name)
        except Exception:
            failed = True
            self.logger.error(('application=dnsmon environment={env} '
                               'msg=failed to perform DNS resolution '
                               'name={name}').format(
                                   env=self.environment, name=name))
        end = time()
        rtt = end - start
        self.logger.info(('application=dnsmon environment={env} '
                          'start_time={start} end_time={end}'
                          'time_unit=sec rtt={rtt} failed={failed}').format(
                              env=self.environment,
                              start=start,
                              end=end,
                              rtt=rtt,
                              failed=failed))
        self.logger.debug(('application=dnsmon environment={env} '
                           'msg=end MonitorClient._dns_resolution').format(
                               env=self.environment))

    def run(self, perform_dns_resolution_time_logging=False):
        self.logger.debug(('application=restmon environment={env} '
                           'msg=start MonitorClient.run').format(
                               env=self.environment))
        if perform_dns_resolution_time_logging:
            self._dns_resolution(uri=self.base_uri)

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
                                endpoint=endpoint),
                        'status_code={status_code}'.format(
                            status_code=r.status_code)
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
                        'response={resp} status_code={status_code}').format(
                            env=self.environment,
                            rtt=rtt,
                            endpoint=endpoint,
                            resp=r.text,
                            start=start,
                            end=end,
                            status_code=r.status_code))
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
