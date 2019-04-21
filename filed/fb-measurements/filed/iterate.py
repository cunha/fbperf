#!/usr/bin/env python3

import argparse
import logging
import pickle
import resource
import sys

# import measurements
# from measurements import MetroPrefixMeasurements, Measurement
from pickler import PickleIterator


def create_parser(): # {{{
    desc = '''Process pickled data to correlate BGP attributes with performance'''

    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument('--pickle',
            dest='picklefn',
            metavar='FILE',
            type=str,
            required=True,
            help='File containing pickled data')

    parser.add_argument('--output',
            dest='outfn',
            metavar='FILE',
            type=str,
            required=False,
            help='Output file to contain the summarized data')

    parser.add_argument('--primary',
            dest='primary_rtypes',
            metavar='TYPES',
            type=str,
            required=False,
            help='Comma-separated list of route types to filter primary paths')

    parser.add_argument('--alternate',
            dest='alternate_rtypes',
            metavar='TYPES',
            type=str,
            required=False,
            help='Comma-separated list of route types to filter alternate paths')

    return parser
# }}}


def main():
    resource.setrlimit(resource.RLIMIT_AS, (1 << 30, 1 << 30))
    resource.setrlimit(resource.RLIMIT_FSIZE, (1 << 35, 1 << 35))
    logging.basicConfig(filename='log.txt', format='%(message)s',
            level=logging.NOTSET)

    parser = create_parser()
    opts = parser.parse_args()

    skipped, processed = 0, 0
    for mpm in PickleIterator(opts.picklefn):
        if len(mpm.dscp2meas) != 3:
            skipped += 1
            continue
        processed += 1

    logging.info('skipped %d processed %d', skipped, processed)


if __name__ == '__main__':
    sys.exit(main())
