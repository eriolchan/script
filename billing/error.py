#!/usr/bin/python

import datetime
import sys


def read_file(input_file):
    counter = {}
    files = set()
    broken_files = set()
    with open(input_file, 'r') as f:
        for line in f:
            process_line(line, counter, files, broken_files)
    return counter, files, broken_files


def process_line(line, counter, files, broken_files):
    if 'complete parsing' in line:
        fields = line.split(',')
        time = fields[-3].split('/')[3:-1]
        hour = datetime.datetime(int(time[1]), int(time[2]), int(time[3]), int(time[4]))
        records = int(fields[-2].split(':')[1])
        if hour not in counter:
            # cdn files, cdn records, vos files, vos records, cdn error, vos error
            counter[hour] = [0,0,0,0,0,0]
        if time[0] == 'cdn':
            counter[hour][0] += 1
            counter[hour][1] += records
        elif time[0] == 'vos':
            counter[hour][2] += 1
            counter[hour][3] += records
    elif 'error when parsing' in line:
        fields = line.split('/')
        hour = datetime.datetime(int(fields[4]), int(fields[5]), int(fields[6]), int(fields[7]))
        filename = fields[-1]
        if hour not in counter:
            counter[hour] = [0,0,0,0,0,0]
        if filename not in files:
            files.add(filename)
            if fields[3] == 'cdn':
                counter[hour][4] += 1
            elif fields[3] == 'vos':
                counter[hour][5] += 1
        else:
            broken_files.add(filename)


def get_result(counter):
    result = []
    for key in sorted(counter):
        result.append('%s,%d,%d,%d,%d,%d,%d' % (key.strftime('%Y-%m-%d %H:%M:%S'), counter[key][0], counter[key][1], counter[key][2], counter[key][3], counter[key][4], counter[key][5]))
    return result


def write_result(output_file, result):
    with open(output_file, 'w') as f:
        f.write(result)


def main():
    input_file = sys.argv[1]
    print input_file
    output_file = 'result.csv'

    counter,files,broken_files = read_file(input_file)
    
    result = get_result(counter)
    
    content = '{}\n{}\n{}\n{}\n{}\n{}'.format(
        'cdn files,cdn records,vos files,vos records,cdn error,vos error',
        '\n'.join(result),
        'error files: %d' % len(files),
        '\n'.join(files),
        'broken files: %d' % len(broken_files),
        '\n'.join(broken_files)
    )

    write_result(output_file, content)
    print 'done!'


if __name__ == '__main__':
    main()
