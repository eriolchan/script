#!/usr/bin/python

import os
import sys


def get_data(filename):
    data = {}
    with open(filename, 'r') as f:
        for line in f:
            fields = line.strip().split(',')
            data[fields[0]] = fields[1]
    return data


def get_result(filename, data):
    result = []
    result.append('count:{}'.format(len(data)))
    with open(filename, 'r') as f:
        for line in f:
            fields = line.strip().split(',')
            key = fields[0]
            value = fields[1]
            if key not in data:
                result.append('[+] new:{}'.format(key))
            elif value != data[key]:
                result.append('[=] file:{}, old:{}, new:{}'.format(key, data[key], value))
                del data[key]
            else:
                del data[key]
    result.extend(['[-] old:{}'.format(key) for key in data.keys()])
    return result        


def write_file(filename, result):
    with open(filename, 'w') as f:
        f.write('\n'.join(result))


def main():
    input_old = sys.argv[1]
    input_new = sys.argv[2]
    output = '{}.diff'.format(input_old)

    data = get_data(input_old)
    result = get_result(input_new, data)
    write_file(output, result)


if __name__ == "__main__":
    main()
