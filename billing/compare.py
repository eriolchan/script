#!/usr/bin/python

import os
import sys


def read_file(filename):
    counter = {}
    with open(filename, 'r') as f:
        for line in f:
            process_line(line, counter)
    return counter


def process_line(line, counter):
    if 'complete LogProcess' in line:
        fields = line.split(',')
        table = fields[1].split(':')[1]
        hour = fields[2].split(':')[1]
        files = fields[3].split(':')[1]
        path = get_path(table, hour)
        counter[path] = int(files)
    return counter


def get_path(table, hour):
    category = 'cdn' if table == 'usage_cdn' else 'vos'
    time = hour.replace('-', '/').replace('T', '/')
    return '{}/{}'.format(category, time)


def compare(counter):
    base_path = '/data/2/billing_archive'
    for key in sorted(counter):
        imported = counter[key]
        folder = base_path + '/' + key
        total = len(os.walk(folder).next()[2])
        result = '{}: import {} / {} -> {}'.format(folder, imported, total, 'X' if imported != total else '')
        print result


def main():
    filename = sys.argv[1]
    counter = read_file(filename)
    compare(counter)


if __name__ == '__main__':
    main()