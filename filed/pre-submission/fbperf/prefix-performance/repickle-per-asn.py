#!/usr/bin/env python3

import argparse
from collections import defaultdict
import pickle
import resource
import sys


def create_parser():
    desc = '''Repickle MetroPrefixData per ASN'''
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('--pickle',
            dest='picklefn',
            action='store',
            metavar='FILE',
            type=str,
            required=True,
            help='Pickle file with metro data')
    parser.add_argument('--asns',
            dest='asnlist',
            action='store',
            metavar='ASNS',
            type=str,
            required=True,
            help='Comma-separated list of ASNs to filter')
    parser.add_argument('--out-pickle',
            dest='outpicklefn',
            action='store',
            metavar='FILE',
            type=str,
            required=True,
            help='Location of output pickle')
    return parser


def main():
    resource.setrlimit(resource.RLIMIT_AS, (1 << 31, 1 << 31))
    resource.setrlimit(resource.RLIMIT_FSIZE, (1 << 32, 1 << 32))

    parser = create_parser()
    opts = parser.parse_args()
    asns = set(int(asn) for asn in opts.asnlist.split(','))

    with open(opts.picklefn, 'rb') as fd:
        mpp = pickle.load(fd)

    asn2key2stats = dict()
    for key, pfxdata in mpp.key2data.items():
        _prefix, dasn, _ipv6 = key
        if dasn not in asns:
            continue
        timestamps = sorted(pfxdata.time2perf.keys())[0:25]
        perflist = list(pfxdata.time2perf[t] for t in timestamps)
        key2stats = asn2key2stats.setdefault(dasn, dict())
        stats = key2stats.setdefault(key, dict())
        stats['minrtt_min'] = list(p['minrtt_min'] for p in perflist)
        stats['minrtt_p5'] = list(p['minrtt_p5'] for p in perflist)
        stats['retrans'] = list(p['retrans_rate'] for p in perflist)
        stats['25mbps'] = list(p['25mbps'] for p in perflist)
        stats['100mbps'] = list(p['100mbps'] for p in perflist)

    with open(opts.outpicklefn, 'wb') as fd:
        pickle.dump(asn2key2stats, fd)

    w = sys.stdout.write
    w('%s: %d asns observed\n' % (opts.picklefn, len(asn2key2stats)))


if __name__ == '__main__':
    sys.exit(main())
