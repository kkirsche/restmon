RESTmon
=======

A monitoring tool to query REST API endpoints and log the result /
latency

Installation
------------

::

    pip install -U restmon

Usage
-----

::

    #!/bin/sh

    /usr/bin/restmon --dns -p password -u user -b http://service_url -e 'api/health1' 'api/health2'
