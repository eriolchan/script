#!/usr/bin/python

import os
import sys
import hashlib

def main():
    folder = sys.argv[1]
    date = folder.split('/')
    output_file = '{}.csv'.format(date[-1])
    result = ['{},{}'.format(filename, hashlib.md5(open('{}/{}'.format(folder, filename), 'rb').read()).hexdigest()) for filename in os.listdir(folder)]

    with open(output_file, 'w') as f:
        f.write('\n'.join(result))


if __name__ == "__main__":
    main()