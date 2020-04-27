#!/usr/bin/env python3
"""Parse and push ISO proxy logs to InfluxDB."""
import argparse
from collections import defaultdict
from datetime import datetime
import gzip
import logging
import os
import statistics
import urllib.parse

from .iso import ISO
from .util import (
    influxdb_connect, influxdb_insert, influx_search,
    make_dict, recursive_glob, logging_init
)

METRIC = 'iso'
LOG = logging.getLogger(METRIC)


def run(logs_dir, dryrun=False):
    """Set up logging, parse arguments, and determine program execution."""
    for log_path in recursive_glob(logs_dir, r'*ubuntu.com-access.log-*.gz'):
        server = log_path.split('/')[-3].split('-')[0]
        mirror = log_path.split('/')[-2]
        date = log_path.split('/')[-1].split('.')[-3].split('-')[-1]
        rfc3339_date = datetime.strptime(date, '%Y%m%d').isoformat('T') + 'Z'
        LOG.info('%s:%s %s', server, mirror, date)

        if server == 'cdimage':
            print('Skipping cdimage mirror: need better parsing')
            continue

        influx = None
        if not ARGS.dryrun:
            influx = influxdb_connect()
            if influx_search(influx, '%s_%s' % (METRIC, server),
                             mirror, rfc3339_date):
                LOG.info('\tSkipping, data already loaded')
                continue

        proxy_data = parse(log_path)
        data = stats(server, mirror, rfc3339_date, proxy_data)
        push(influx, data)


def parse(log_path):
    """Parse a specific file.

    @param: log_path path to the compressed log to be parsed
    @return: list of log entries
    """
    log_path = os.path.abspath(log_path)

    entries = []
    with gzip.open(log_path, 'rb') as raw_log:
        for raw_line in raw_log:
            try:
                # urllib is used to parse entries like %2b into ascii values
                line = urllib.parse.unquote(raw_line.decode())
            except UnicodeDecodeError:
                LOG.debug('skipping line: bad unicode decode error')

            entry = ISO(line)
            if entry.valid:
                entries.append(entry)

    return entries


def push(influx, data):
    """Push or print the data to stdout.

    @param influx: influxdb connection or None
    @param data: list of dictionary data to push or print
    """
    if influx:
        influxdb_insert(influx, data)
    else:
        print(data)
        print()


def stats(server, mirror, date, proxy_data):
    """Create overall statistics.

    @param server: server where data was collected
    @param mirror: specific mirror looked at
    @param date: RFC3339 date
    @param proxy_data: parsed proxy logs from parse_pollinate_proxy_log
    @return: JSON-like data with statistics calculated for InfluxDB
    """
    data = []

    results = defaultdict(make_dict)
    for log in proxy_data:
        if results[log.release][log.arch][log.flavor]:
            results[log.release][log.arch][log.flavor] += 1
        else:
            results[log.release][log.arch][log.flavor] = 1

    for release, arches in results.items():
        for arch, flavors in arches.items():
            for flavor, downloads in flavors.items():
                entry = {
                    "measurement": '%s_%s' % (METRIC, server),
                    "tags": {
                        "mirror": mirror,
                        "release": release,
                        "arch": arch,
                        "flavor": flavor,
                    },
                    "fields": {
                        "downloads": downloads,
                    },
                    "time": date
                }

                data.append(entry)

    return data


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(prog='pollinate')
    PARSER.add_argument('--dir', help='Directory of logs to parse')
    PARSER.add_argument('--dryrun', action='store_true')
    PARSER.add_argument('--verbose', action='store_true')

    ARGS = PARSER.parse_args()
    logging_init(METRIC, ARGS.verbose)

    run(ARGS.dir, ARGS.dryrun)
