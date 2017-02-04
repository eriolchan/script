#!/usr/bin/python

import os
import sys


def main():
    base_path = '/data/2/billing_archive/cdn'
    date = sys.argv[1]

    for i in range(24):
        folder = '{}/{}/{:02d}'.format(base_path, date, i)
        print folder, len(os.walk(folder).next()[2])


if __name__ == '__main__':
    main()