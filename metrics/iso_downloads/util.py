"""Various utility functions."""
import datetime
from collections import defaultdict
import fnmatch
import logging
import os
import pathlib
import sys

from influxdb import InfluxDBClient
import requests
import swiftclient


def influxdb_connect():
    """Connect to an InfluxDB instance."""
    try:
        hostname = os.environ['INFLUXDB_HOSTNAME']
        port = os.environ['INFLUXDB_PORT']
        username = os.environ['INFLUXDB_USERNAME']
        password = os.environ['INFLUXDB_PASSWORD']
        database = os.environ['INFLUXDB_DATABASE']
    except KeyError:
        print('error: please source influx credentials before running')
        sys.exit(1)

    return InfluxDBClient(hostname, port, username, password, database)


def influxdb_insert(client, measurement):
    """Insert measurement into InfluxDB."""
    if measurement:
        client.write_points(measurement)


def influx_search(client, measurement, mirror, date):
    """Search influx for a result matching the mirror and date."""
    if not client:
        return False

    query = (
        "select count(*) from %s where mirror = '%s' and time = '%s';" % (
            measurement, mirror, date
        )
    )

    try:
        return bool(client.query(query))
    except requests.exceptions.ConnectionError:
        print('Unable to connect to database.')
        sys.exit(1)


def make_dict():
    """Initialize a defaultdict of arbitrary depth."""
    return defaultdict(make_dict)


def logging_init(name, debug=False):
    """Set up logging to stdout."""
    log_level = logging.DEBUG if debug else logging.INFO
    log_format = '[%(asctime)s] %(levelname)8s: %(message)s'
    logging.basicConfig(stream=sys.stdout, format=log_format, level=log_level)


def recursive_glob(search_dir, regex):
    """Search recursively for files matching a specified pattern."""
    matches = []
    for root, _, filenames in os.walk(search_dir):
        for filename in fnmatch.filter(filenames, regex):
            matches.append(os.path.join(root, filename))

    return sorted(matches)


def swift_connect():
    """Connect to swift using OS environment variables."""
    try:
        user = os.environ['OS_USERNAME']
        key = os.environ['OS_PASSWORD']
        authurl = os.environ['OS_AUTH_URL']
        tenant_name = os.environ['OS_TENANT_NAME']
        region_name = os.environ['OS_REGION_NAME']
        storage_url = os.environ['OS_STORAGE_URL']
    except KeyError as e:
        print('error: please source swift credentials before running')
        print('missing value: %s' % e)
        sys.exit(1)

    return swiftclient.client.Connection(
        authurl=authurl, user=user, key=key, auth_version='2.0',
        os_options={
            'region_name': region_name,
            'tenant_name': tenant_name,
            'object_storage_url': storage_url,
        }
    )


def swift_download(connection, bucket, bucket_dir, date):
    """Download all items from a bucket."""
    print('%s' % bucket)
    print('---')

    if not os.path.isdir(bucket_dir):
        os.makedirs(bucket_dir)

    for data in connection.get_container(bucket)[1]:
        filename = os.path.join(bucket_dir, data['name'])

        if date not in filename:
            continue

        parent_dir = '/'.join(filename.split('/')[:-1])
        pathlib.Path(parent_dir).mkdir(parents=True, exist_ok=True)

        if os.path.exists(filename):
            print('Skipping %s' % (data['name']))
            continue

        print('%s' % (data['name']))
        _, contents = connection.get_object(bucket, data['name'])
        with open(filename, 'wb') as local:
            local.write(contents)
