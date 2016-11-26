#!/usr/bin/python

from datetime import datetime


def read_file(filename):
    with open(filename, 'r') as f:
        data = f.read()
    return data


def get_result(data):
    result = data.split(':')

    now = datetime.utcnow().strftime('%s')
    ago = long(now) - long(result[0])

    if (ago > 15 * 60):
        print -1
    else:
        print int(result[1])


if __name__ == "__main__":
    filename = 'vendor_usage'
    get_result(read_file(filename))