#!/usr/bin/env python3
"""Submit metrics for proposed-migration statistics."""
import csv
from io import StringIO
import logging
import urllib

from metrics.helpers import util
from prometheus_client import CollectorRegistry, Gauge


def get_proposed_migration_queue(registry, label, description):
    """Get information about current proposed-migration queue."""
    src = 'https://people.canonical.com/~ubuntu-archive/proposed-migration/' \
          + 'update_excuses.csv'
    logging.info('Pulling proposed-migration stats')
    with urllib.request.urlopen(src) as req:
        code = req.getcode()
        if code != 200:
            logging.error('URL %s failed with code %u', req.geturl(), code)
            return
        csvdata = StringIO(req.read().decode('UTF-8'))

    csv_handle = csv.reader(csvdata)
    latest = list(csv_handle)[-1]
    valid, not_considered = [int(x) for x in latest[1:3]]

    gauge = Gauge(label, description,
                  ['candidate'],
                  registry=registry)
    gauge.labels('Valid Candidates').set(valid)
    gauge.labels('Not Considered').set(not_considered)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    REGISTRY = CollectorRegistry()
    try:
        get_proposed_migration_queue(
            REGISTRY,
            label='foundations_devel_proposed_migration_size',
            description=('Number of packages waiting in devel-proposed'))
    finally:
        util.push2gateway('proposed_migration', REGISTRY)
