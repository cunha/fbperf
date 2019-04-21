#!/usr/bin/env python3

import argparse
import csv
import logging
import pickle
import resource
import sys

import measurements
from measurements import MetroPrefixMeasurements, Measurement


class FilteringCSVReader(csv.DictReader):  # {{{
    def __init__(self, filter_column, min_value, *args, **kwargs):
        csv.register_dialect('FB')
        self.filter_column = str(filter_column)
        self.min_value = int(min_value)
        super().__init__(dialect='FB', *args, **kwargs)

    def __next__(self):
        cnt = 1
        row = super().__next__()
        while not row[self.filter_column] or int(row[self.filter_column]) < self.min_value:
            cnt += 1
            row = super().__next__()
        logging.info('FilteringCSVReader yielding after %d rows', cnt)
        return row
# }}}


class PickleIterator:  # {{{
    def __init__(self, fn):
        self.fd = open(fn, 'rb')

    def __iter__(self):
        while True:
            try:
                yield pickle.load(self.fd)
            except EOFError:
                break
# }}}


def create_parser(): # {{{
    desc = '''Pickle Hive data for easy manipulation'''

    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument('--csv',
            dest='csv',
            metavar='FILE',
            type=str,
            required=True,
            help='CSV file containing data from Hive')

    parser.add_argument('--output',
            dest='outfn',
            metavar='FILE',
            type=str,
            required=True,
            help='Output file to contain the pickled data')

    parser.add_argument('--samples-column',
            dest='samples_column',
            metavar='COL',
            type=str,
            default=measurements.COL_NUM_SAMPLES_FOR_THROUGHPUT,
            help='Column with number of samples to filter on [%(default)s]')

    parser.add_argument('--min-samples',
            dest='min_samples',
            metavar='INT',
            type=int,
            default=500,
            help='Minimum number of samples per metro/prefix [%(default)s]')

    return parser
# }}}


def main():
    resource.setrlimit(resource.RLIMIT_AS, (1 << 30, 1 << 30))
    resource.setrlimit(resource.RLIMIT_FSIZE, (1 << 35, 1 << 35))
    logging.basicConfig(filename='log.txt', format='%(message)s',
            level=logging.NOTSET)

    parser = create_parser()
    opts = parser.parse_args()
    opts.outfd = open(opts.outfn, 'wb')

    fd = open(opts.csv)
    reader = FilteringCSVReader(opts.samples_column, opts.min_samples, fd)

    # We read the file and build a dscp2row dictionary for each key.
    # When the key changes, we call opts.process to process the
    # current dscp2row, and proceed to the next key.
    dscp2meas = dict()

    # Initialize the last seen key to simplify the loop:
    row = next(reader)
    meas = Measurement(row)
    last = (row[measurements.COL_VIP_METRO_SHA1],
            row[measurements.COL_IP_PREFIX_SHA1])
    dscp2meas[meas.dscp] = meas

    for row in reader:
        meas = Measurement(row)
        key = (row[measurements.COL_VIP_METRO_SHA1],
               row[measurements.COL_IP_PREFIX_SHA1])
        if key == last:
            dscp2meas[meas.dscp] = meas
            continue
        mpm = MetroPrefixMeasurements(last[0], last[1], dscp2meas)
        pickle.dump(mpm, opts.outfd)
        last = key
        dscp2meas = dict()
        dscp2meas[meas.dscp] = meas

    mpm = MetroPrefixMeasurements(last[0], last[1], dscp2meas)
    pickle.dump(mpm, opts.outfd)
    opts.outfd.close()
    fd.close()


if __name__ == '__main__':
    sys.exit(main())
