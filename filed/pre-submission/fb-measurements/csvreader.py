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

    parser.add_argument('-c',
            dest='columns',
            metavar='NAME',
            type=str,
            action='append',
            required=False,
            help='Columns to print')

    parser.add_argument('--print-columns',
            dest='print_columns',
            action='store_const',
            const=True,
            default=False,
            help='Print column names and exit [%(default)s]')

    return parser
# }}}


def columnname(fieldnames, c):  # {{{
    try:
        return fieldnames[int(c)-1]
    except ValueError:
        assert c in fieldnames
        return c
# }}}


def main():
    parser = create_parser()
    opts = parser.parse_args()

    csv.register_dialect('FB')

    with open(opts.csv) as fd:
        reader = csv.DictReader(fd, dialect='FB')
        opts.columns = list(columnname(reader.fieldnames, c) for c in opts.columns)

        if opts.print_columns:
            sys.stdout.write('%s\n' %
                             '\n'.join('%d %s' % (k, v) for k, v
                                       in enumerate(reader.fieldnames)))
            sys.exit(0)

        for row in reader:
            sys.stdout.write('%s\n' % ','.join(row[c] for c in opts.columns))


if __name__ == '__main__':
    sys.exit(main())
