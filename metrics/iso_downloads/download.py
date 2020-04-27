#!/usr/bin/env python3
"""Download ISO proxy logs."""
import argparse
from datetime import datetime, timedelta
import os

from .util import swift_connect, swift_download

BUCKETS = [
    'cdimage-mirror.anonymised',
    'releases-mirror.anonymised'
]


def run(destination_dir, date):
    """Determine data, connect, and download files."""
    if not date:
        date = (datetime.now() - timedelta(days=5)).strftime('%Y%m%d')

    print('downloading logs dated: %s\n' % date)
    connection = swift_connect()
    for bucket in BUCKETS:
        bucket_dir = os.path.join(destination_dir, bucket)
        swift_download(connection, bucket, bucket_dir, date)
        print('')


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser()
    PARSER.add_argument('--dir', help='Destination for logs')
    PARSER.add_argument('--date', required=False,
                        help='Date of logs to download (e.g. 20180112')

    ARGS = PARSER.parse_args()
    run(ARGS.dir, ARGS.date)
