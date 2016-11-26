#!/usr/bin/python

import json
import socket
import sys
import urllib2
from datetime import datetime


def get_url():
    date_format = '%Y%m%d'
    today = datetime.utcnow().strftime(date_format)
    base_url = 'http://api.abc.com/usage/table/date/%s?by=1'
    return base_url % today


def get_usage():
    """
    get usage and return exit code

    0: success
    1: fail, total_duration <= 0
    2: fail, timeout
    3: fail, http_status_code != 200
    4: fail, connection failure
    """

    url = get_url()
    payload = '[8699]'
    exit_code = -1

    req = urllib2.Request(url, payload)
    try:
        res = urllib2.urlopen(req, timeout=40)
    except urllib2.HTTPError as e:
        exit_code = 3
    except urllib2.URLError as e:
        exit_code = 4
    except socket.timeout as e:
        exit_code = 2
    else:
        response_json = res.read()
        result = json.loads(response_json)
        res.close()

        total_duration = result['TotalDurationInMinutes']
        if total_duration <= 0:
            exit_code = 1
        else:
            exit_code = 0
    finally:
        return exit_code


def write_file(filename, data):
    with open(filename, 'w') as f:
        f.write(data)


def main():
    exit_code = get_usage()
    data = '%s:%d' % (datetime.utcnow().strftime('%s'), exit_code)
    filename = 'vendor_usage'
    write_file(filename, data)

if __name__ == "__main__":
    main()
