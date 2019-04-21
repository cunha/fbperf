#!/usr/bin/env python3

import argparse
import csv
import sys


def create_parser(): # {{{
    desc = '''Print columns from CSV files'''

    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument('csv',
            type=str,
            help='CSV file')

    return parser
# }}}


def main():
    parser = create_parser()
    opts = parser.parse_args()

    csv.register_dialect('FB')

    fd = open(opts.csv)
    reader = csv.DictReader(fd, dialect='FB')
    writer = csv.writer(sys.stdout, dialect='FB')

    w = writer.writerow
    w(reader.fieldnames + ['hd_capable_frac'])

    for row in reader:
        hd_frac = 0.0
        samples = int(row['num_samples_for_throughput'])
        if samples > 0:
            hd_samples = int(row['num_samples_throughput_hd_capable'])
            hd_frac = hd_samples / samples
        w([row[f] for f in reader.fieldnames] + [hd_frac])

    fd.close()


if __name__ == '__main__':
    sys.exit(main())
