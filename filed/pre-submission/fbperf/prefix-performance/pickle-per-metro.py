#!/usr/bin/env python3

import argparse
import logging
import pickle
import resource
import sys

from fblib import MetroPrefixPerf


def create_parser():
    desc = '''Pickle CSVs'''
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('--csv',
            dest='csvfn',
            action='store',
            metavar='FILE',
            type=str,
            required=True,
            help='Input CSV with prefix binned data for a metro')
    parser.add_argument('--csvheader',
            dest='csvheaderfn',
            action='store',
            metavar='FILE',
            type=str,
            required=True,
            help='Input CSV with header column of main CSV file')
    parser.add_argument('--pickle',
            dest='picklefn',
            action='store',
            metavar='FILE',
            type=str,
            required=True,
            help='Output pickle file with MetroPrefixPerf pickle')
    parser.add_argument('--logfile',
            dest='logfile',
            action='store',
            metavar='FILE',
            type=str,
            default='log.txt',
            help='Log file location [%(default)s]')
    return parser


def main():
    resource.setrlimit(resource.RLIMIT_AS, (1 << 31, 1 << 31))
    resource.setrlimit(resource.RLIMIT_FSIZE, (1 << 32, 1 << 32))
    parser = create_parser()
    opts = parser.parse_args()
    logging.basicConfig(filename=opts.logfile, format='%(message)s',
                        level=logging.DEBUG)
    logging.info('processing %s -> %s', opts.csvfn, opts.picklefn)
    mpp = MetroPrefixPerf.from_csv(opts.csvfn, opts.csvheaderfn)
    with open(opts.picklefn, 'wb') as fd:
        pickle.dump(mpp, fd)


if __name__ == '__main__':
    sys.exit(main())
