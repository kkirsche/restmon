# -*- coding: utf-8 -*-
'''Create a logger instance.'''
# third party packages
from raven import Client, fetch_package_version
from raven.conf import setup_logging
from raven.handlers.logging import SentryHandler

# python stdlib
from logging import basicConfig, getLogger, INFO, ERROR
from logging.handlers import SysLogHandler


def get_sentry_handler():
    '''Setup the Sentry.io handler for catching errors.

    Returns:
        :obj:`raven.handlers.SentryHandler`
    '''
    client = Client(
        dsn=('https://eb1e68ac56a14e09bfafe3abd7e8ff5e:dcc8d99ffa52459d956a'
             '126085ce8fdf@sentry.io/215308'),
        release=fetch_package_version('restmon'))
    handler = SentryHandler(client)
    handler.setLevel(ERROR)
    return handler


def setup_syslog_handler(address='/dev/log'):
    """Setup the class logging instance.

    Args:
        address (string or tuple): Where to log syslog information to.
    Returns:
        :obj:`logging.handler.SysLogHandler`
    """
    handler = SysLogHandler(address=address)
    return handler


def setup_logger(log_level=INFO):
    '''Setup the logger with Sentry and Syslog handlers.

    Args:
        log_level (int): The logging level (e.g. logging.INFO)
    Returns:
        :obj:`logging.Logger`
    '''
    log_format = '%(message)s'
    basicConfig(format=log_format)
    handler = get_sentry_handler()
    setup_logging(handler)
    logger = getLogger('restmon')
    logger.addHandler(setup_syslog_handler())
    logger.setLevel(log_level)
    return logger
