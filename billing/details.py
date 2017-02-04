#!/usr/bin/python

import os
import sys


def read_file(filename):
    voice = 0
    video = 0
    with open(filename, 'r') as f:
        for line in f:
            fields = line.split()
            voice += int(fields[3])
            video += int(fields[4])
    return voice, video


def main():
    filename = sys.argv[1]
    voice, video = read_file(filename)
    print voice, video, voice + video


if __name__ == '__main__':
    main()