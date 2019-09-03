#!/usr/bin/env python3

import argparse
import csv
import json
import logging
import resource
import sys

COL_VIP_METRO_SHA1 = 'vip_metro_sha1'
COL_IP_PREFIX_SHA1 = 'ip_prefix_sha1'
COL_APM_DSCP_VALUE_TXN_START = 'apm_dscp_value_txn_start'
COL_ALTPATH_ACTIONS_ROUTE_TYPE_CLEANED = 'altpath_actions_route_type_cleaned'
COL_ALTPATH_ACTIONS_ROUTE_AS_PATHS = 'altpath_actions_route_as_paths'
COL_ALTPATH_ACTIONS_ROUTE_BGP_AS_PATH_LEN = 'altpath_actions_route_bgp_as_path_len'
COL_RTT_MS_P50 = 'rtt_ms_p50'
COL_RTT_MS_P95 = 'rtt_ms_p95'
COL_NUM_SAMPLES_FOR_RTT = 'num_samples_for_rtt'
COL_THROUGHPUT_P5 = 'throughput_p5'
COL_THROUGHPUT_P50 = 'throughput_p50'
COL_THROUGHPUT_CAPPED_P5 = 'throughput_capped_p5'
COL_THROUGHPUT_CAPPED_P50 = 'throughput_capped_p50'
COL_NUM_SAMPLES_THROUGHPUT_HD_CAPABLE = 'num_samples_throughput_hd_capable'
COL_NUM_SAMPLES_FOR_THROUGHPUT = 'num_samples_for_throughput'
COL_NUM_SAMPLES_TOTAL = 'num_samples_total'
COL_AVG_PREFIX_BPS = 'avg_prefix_bps'
COL_MAX_PREFIX_BPS = 'max_prefix_bps'
COL_CLIENT_CONN_SPEED = 'client_conn_speed'
COL_CLIENT_COUNTRY = 'client_country'
COL_CLIENT_CONTINENT = 'client_continent'
COL_HD_FRAC = 'hd_capable_frac'

DSCP_PRI = 48
DSCP_SEC = 49
DSCP_TER = 50
DSCP_ALTERNATES = frozenset([DSCP_SEC, DSCP_TER])

RT_TRANSIT = 'transit'
RT_PUBLIC = 'public'
RT_ROUTESERVER = 'route_server'
RT_PRIVATE = 'private'
ROUTE_TYPES = frozenset([RT_TRANSIT, RT_PUBLIC, RT_PRIVATE])
RT_ORDER = ['private', 'public', 'transit']


class RowData:  # {{{
    def __init__(self, row, metric_column, samples_column):
        self.key = (row[COL_VIP_METRO_SHA1], row[COL_IP_PREFIX_SHA1])
        self.dscp = int(row[COL_APM_DSCP_VALUE_TXN_START])
        self.type = str(row[COL_ALTPATH_ACTIONS_ROUTE_TYPE_CLEANED])
        if self.type == RT_ROUTESERVER: self.type = RT_PUBLIC
        assert self.type in ROUTE_TYPES, self.type
        self.aspaths = json.loads(row[COL_ALTPATH_ACTIONS_ROUTE_AS_PATHS])
        self.aspaths = list([int(asn) for asn in path.split(',')]
                            for path in self.aspaths)
        self.peers = set(p[0] for p in self.aspaths)
        self.aspathlen = int(row[COL_ALTPATH_ACTIONS_ROUTE_BGP_AS_PATH_LEN])
        self.uniq_aspathlen = len(set(self.aspaths[0]))
        self.continent = str(row[COL_CLIENT_CONTINENT])
        if not self.continent:
            self.continent = 'XX'
        self.bps = float(row[COL_AVG_PREFIX_BPS])
        self.value = float(row[metric_column])
        self.samples = int(row[samples_column])

    def pref_order(self, other):
        o1 = RT_ORDER.index(self.type)
        o2 = RT_ORDER.index(other.type)
        if o1 < o2: return -1
        if o1 > o2: return 1
        if self.aspathlen < other.aspathlen: return -1
        if self.aspathlen > other.aspathlen: return 1
        return 0
# }}}


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


def create_parser(): # {{{
    desc = '''Process Hive data to correlate BGP attributes with performance'''

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
            help='Output file to contain the summarized data')

    parser.add_argument('--primary',
            dest='primary_rtypes',
            metavar='TYPES',
            type=str,
            required=True,
            help='Comma-separated list of route types to filter primary paths')

    parser.add_argument('--alternate',
            dest='alternate_rtypes',
            metavar='TYPES',
            type=str,
            required=True,
            help='Comma-separated list of route types to filter alternate paths')

    parser.add_argument('--algo',
            dest='algo',
            metavar='CHOICE',
            type=str,
            choices=['rtt', 'throughput', 'rtt-length', 'throughput-length', 'current-rtt', 'current-tput'],
            default='rtt',
            help='Type of graph to generate data for [%(default)s]')

    parser.add_argument('--metric-column',
            dest='metric_column',
            metavar='COL',
            type=str,
            required=False,
            default=COL_RTT_MS_P50,
            help='Column to use compare paths [%(default)s]')

    parser.add_argument('--samples-column',
            dest='samples_column',
            metavar='COL',
            type=str,
            default=COL_NUM_SAMPLES_FOR_RTT,
            help='Column with number of samples to filter on [%(default)s]')

    parser.add_argument('--min-samples',
            dest='min_samples',
            metavar='INT',
            type=int,
            default=500,
            help='Minimum number of samples per metro/prefix [%(default)s]')

    return parser
# }}}


def parse_algo(opts):  # {{{
    if opts.algo == 'rtt':
        opts.process = best_alternate
        opts.mult = 1
        opts.cmp_alternates = min
    if opts.algo == 'throughput':
        opts.process = best_alternate
        opts.mult = -1
        opts.cmp_alternates = max
    if opts.algo == 'rtt-length':
        opts.process = best_length
        opts.mult = 1
        opts.cmp_alternates = min
    if opts.algo == 'throughput-length':
        opts.process = best_length
        opts.mult = -1
        opts.cmp_alternates = max
    if opts.algo == 'current-rtt':
        opts.process = current
        opts.mult = 1
    if opts.algo == 'current-tput':
        opts.process = current
        opts.mult = -1
# }}}


def filter_rows(dscp2row, valid_dscps, valid_rtypes):
    rows = list(row for dscp, row in dscp2row.items()
                if dscp in valid_dscps)
    return list(r for r in rows if r.type in valid_rtypes)


def best_length(dscp2row, opts):
    assert opts.primary_rtypes == opts.alternate_rtypes
    rows = sorted(dscp2row.values(),
                  key=lambda x: (x.aspathlen, x.uniq_aspathlen))
    rows = list(r for r in rows if r.type in opts.primary_rtypes)
    if len(rows) < 2:
        logging.debug('missing alternate routes')
        return
    primary = rows[0]
    alternate = rows[1]
    average = alternate.value

    assert primary.bps == alternate.bps, 'Prefix with different bps rates'
    assert primary.continent == alternate.continent, 'Prefix mapped to multiple continents'

    abs_diff = primary.value - alternate.value
    abs_diff *= opts.mult
    rel_diff = float(abs_diff) / max(primary.value, 1)
    abs_diff_avg = primary.value - average
    abs_diff_avg *= opts.mult
    rel_diff_avg = float(abs_diff_avg) / max(primary.value, 1)

    same_peer_as = 1 if primary.peers == alternate.peers else 0

    opts.outfd.write('%f %f %f %f %d %s %d %d %d %d %d %d %s\n' % (
                     abs_diff, rel_diff,
                     abs_diff_avg, rel_diff_avg,
                     primary.bps,
                     primary.continent,
                     primary.aspathlen,
                     alternate.aspathlen,
                     primary.uniq_aspathlen,
                     alternate.uniq_aspathlen,
                     same_peer_as,
                     primary.pref_order(alternate),
                     '%s,%s' % primary.key))


def best_alternate(dscp2row, opts):
    primary = filter_rows(dscp2row, [DSCP_PRI], opts.primary_rtypes)
    if not primary:
        logging.debug('missing primary route')
        return
    assert len(primary) == 1
    primary = primary[0]

    alternates = filter_rows(dscp2row, DSCP_ALTERNATES, opts.alternate_rtypes)
    if not alternates:
        logging.debug('missing alternate routes')
        return
    assert len(alternates) <= 2
    alternate = opts.cmp_alternates(alternates, key=lambda x: x.value)
    average = float(sum(a.value for a in alternates))/len(alternates)

    assert primary.bps == alternate.bps, 'Prefix with different bps rates'
    assert primary.continent == alternate.continent, 'Prefix mapped to multiple continents'

    abs_diff = primary.value - alternate.value
    abs_diff *= opts.mult
    rel_diff = float(abs_diff) / max(primary.value, 1)
    abs_diff_avg = primary.value - average
    abs_diff_avg *= opts.mult
    rel_diff_avg = float(abs_diff_avg) / max(primary.value, 1)

    same_peer_as = 1 if primary.peers == alternate.peers else 0

    opts.outfd.write('%f %f %f %f %d %s %d %d %d %d %d %d %s\n' % (
                     abs_diff, rel_diff,
                     abs_diff_avg, rel_diff_avg,
                     primary.bps,
                     primary.continent,
                     primary.aspathlen,
                     alternate.aspathlen,
                     primary.uniq_aspathlen,
                     alternate.uniq_aspathlen,
                     same_peer_as,
                     primary.pref_order(alternate),
                     '%s,%s' % primary.key))


def current(dscp2row, opts):
    assert set(opts.primary_rtypes) == set(RT_ORDER)
    current = filter_rows(dscp2row, [DSCP_PRI], opts.primary_rtypes)
    if not current:
        logging.debug('missing primary route')
        return
    assert len(current) == 1
    current = current[0]

    routes = list(dscp2row.values())

    rt_pref = list(rt for rt in routes if rt.type != RT_TRANSIT)
    if not rt_pref:
        rt_pref = list(rt for rt in routes if rt.type == RT_TRANSIT)


    shortest = min(rt_pref, key=lambda x: x.aspathlen)
    rt_shortest = list(rt for rt in rt_pref if rt.aspathlen == shortest.aspathlen)


    best = min(routes, key=lambda x: opts.mult * x.value)
    pref_best = min(rt_pref, key=lambda x: opts.mult*x.value)
    pref_len_best = min(rt_shortest, key=lambda x: opts.mult*x.value)

    opts.outfd.write('%f %d %f %d %f %d %f %d %f %s %s\n' % (
                     current.value, current.aspathlen,
                     pref_len_best.value, pref_len_best.aspathlen,
                     pref_best.value, pref_best.aspathlen,
                     best.value, best.aspathlen,
                     current.bps,
                     current.continent,
                     '%s,%s' % best.key))


def main():
    resource.setrlimit(resource.RLIMIT_AS, (1 << 30, 1 << 30))
    resource.setrlimit(resource.RLIMIT_FSIZE, (1 << 35, 1 << 35))
    logging.basicConfig(filename='log.txt', format='%(message)s',
            level=logging.NOTSET)

    parser = create_parser()
    opts = parser.parse_args()
    opts.primary_rtypes = opts.primary_rtypes.split(',')
    opts.alternate_rtypes = opts.alternate_rtypes.split(',')
    opts.outfd = open(opts.outfn, 'w')
    parse_algo(opts)

    fd = open(opts.csv)
    reader = FilteringCSVReader(opts.samples_column, opts.min_samples, fd)

    # We read the file and build a dscp2row dictionary for each key.
    # When the key changes, we call opts.process to process the
    # current dscp2row, and proceed to the next key.
    dscp2row = dict()

    # Initialize the last seen key to simplify the loop:
    row = next(reader)
    rd = RowData(row, opts.metric_column, opts.samples_column)
    last = rd.key
    dscp2row[rd.dscp] = rd

    for row in reader:
        rd = RowData(row, opts.metric_column, opts.samples_column)
        if rd.key == last:
            dscp2row[rd.dscp] = rd
            continue
        opts.process(dscp2row, opts)
        last = rd.key
        dscp2row = dict()
        dscp2row[rd.dscp] = rd

    opts.process(dscp2row, opts)
    fd.close()
    opts.outfd.close()


if __name__ == '__main__':
    sys.exit(main())
