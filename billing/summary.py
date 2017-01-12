#!/usr/bin/python

import datetime
import sys


def read_file(input_file):
    counter = {}
    with open(input_file, 'r') as f:
        for line in f:
            process_line(line, counter)
    return counter


def process_line(line, counter):
    if 'complete parsing' in line:
        fields = line.split(',')
        time = fields[-3].split('/')[4:-1]
        hour = datetime.datetime(int(time[1]), int(time[2]), int(time[3]), int(time[4]))
        records = int(fields[-2].split(':')[1])
        if hour not in counter:
            counter[hour] = [0,0,0,0]
        if time[0] == 'cdn':
            counter[hour][0] += 1
            counter[hour][1] += records
        elif time[0] == 'vos':
            counter[hour][2] += 1
            counter[hour][3] += records


def get_result(counter):
    result = []
    for key in sorted(counter):
        result.append('%s,%d,%d,%d,%d' % (key.strftime('%Y-%m-%d %H:%M:%S'), counter[key][0], counter[key][1], counter[key][2], counter[key][3]))
    return result


def write_result(output_file, result):
    with open(output_file, 'w') as f:
        f.write(result)


def main():
    input_file = sys.argv[1]
    print input_file
    output_file = 'result.csv'

    counter = read_file(input_file)
    result = get_result(counter)

    write_result(output_file, '\n'.join(result))
    print 'done!'


if __name__ == '__main__':
    main()
