#!/usr/bin/env python3

import bisect
from collections import defaultdict
import csv
from functools import total_ordering
import glob
import logging
import json
import os
import re
import sys

CONGESTION_FILE_GLOB_FMT = '*-rttthresh%d-cduration%d.txt'
CONGESTION_FILENAME_REGEX = r'([a-z-]+)-rttthresh[01]-cduration[0-9]+\.txt'
CONGESTION_DEFAULT_DIR = '/home/cunha/Dropbox/shared/SIGCOMM-2019-Data/ixping/congestions'

IXPDB_DEFAULT_DIR = '/home/cunha/Dropbox/shared/SIGCOMM-2019-Data/ixp-info'

METRO_CSV_FILENAME_REGEX = r'([0-9]+)-metro\.csv'

V2BOOL = {1: True,
          '1': True,
          'true': True,
          'True': True,
          'ok': True,
          'Ok': True,
          'OK': True,
          'Yes': True,
          'yes': True,
          'Y': True,
          'y': True,
          'T': True,
          't': True,
          0: False,
          '0': False,
          'false': False,
          'False': False,
          'no': False,
          'No': False,
          'NO': False,
          'F': False,
          'f': False}


@total_ordering
class Congestion:
    def __init__(self, metro, rttthresh, cduration, line):
        self.metro = str(metro)
        self.rttthresh = int(rttthresh)
        self.cduration = int(cduration)
        self.key, self.start, self.end = line.split()
        self.start = float(self.start)
        self.end = float(self.end)
        # FB measurement keys are triples: ip,metro,traceroute_svc_dc
        self.ip = self.key if ',' not in self.key else self.key.split(',')[0]

    def __eq__(self, other):
        return (self.start, self.end, self.metro, self.key, self.rttthresh,
                other.cduration) == (other.start, other.end, other.metro,
                other.key, other.rttthresh, other.cduration)

    def __ne__(self, other):
        return (self.start, self.end, self.metro, self.key, self.rttthresh,
                other.cduration) != (other.start, other.end, other.metro,
                other.key, other.rttthresh, other.cduration)

    def __lt__(self, other):
        assert self.rttthresh == other.rttthresh
        assert self.cduration == other.cduration
        return (self.start, self.end, self.metro, self.key) < \
               (other.start, other.end, other.metro, other.key)

    def sql_where_condition(self):
        if not self.metro.startswith('fb-'):
            return None
        vmetro = self.metro.split('-')[1]
        assert len(vmetro) == 3
        condition = "(TIME > %d " % (self.start - 7200)
        condition += "AND TIME < %d " % (self.end + 7200)
        condition += "AND CONTAINS(nexthops, '%s') " % self.ip
        condition += "AND vip_metro = '%s')" % vmetro
        return condition


class CongestionDB:
    def __init__(self, location=CONGESTION_DEFAULT_DIR,
                 rttthresh=1, cduration=3600):
        self.metro2ip2congestions = defaultdict(lambda: defaultdict(list))
        self.location = location
        self.rttthresh = int(rttthresh)
        self.cduration = int(cduration)

        logging.info('CongestionDB location=%s rttthresh=%d cduration=%d',
                     location, rttthresh, cduration)
        fileglob = CONGESTION_FILE_GLOB_FMT % (rttthresh, cduration)
        filelist = glob.glob(os.path.join(location, fileglob))
        logging.info('CongestionDB loading %d files', len(filelist))

        for fn in filelist:
            m = re.match(CONGESTION_FILENAME_REGEX, os.path.basename(fn))
            if not m:
                logging.fatal('ERROR: filename %s does not match regex', fn)
                continue
            logging.info('Loading %s', fn)
            metro = m.group(1)
            with open(fn) as fd:
                for line in fd:
                    self.add(Congestion(metro, rttthresh, cduration, line))

    def add(self, congestion):
        clist = self.metro2ip2congestions[congestion.metro][congestion.ip]
        bisect.insort(clist, congestion)

    def compute_metro2stats(self, ixpdb):
        metro2stats = dict()
        for metro, ip2congestions in self.metro2ip2congestions.items():
            congestions = list()
            for cslist in ip2congestions.values():
                congestions.extend(cslist)
            unmapped = set(ip for ip in ip2congestions
                           if ip not in ixpdb.ip2portinfo)
            stats = {'n_congestions': len(congestions),
                     'n_nexthops': len(ip2congestions),
                     'n_unmapped_nexthops': len(unmapped),
                     'n_asns': len(set(ixpdb.ip2portinfo[ip]['asn']
                                       for ip in ip2congestions
                                       if ip in ixpdb.ip2portinfo)),
                     'unmapped_nexthops': list(unmapped),
                     'sum_congestion_durations': sum(c.end - c.start
                                                     for c in congestions)}
            metro2stats[metro] = stats
        return metro2stats

    def sql_where_clause(self):
        congestions = list()
        for metro, ip2congestions in self.metro2ip2congestions.items():
            if not metro.startswith('fb-'):
                continue
            for clist in ip2congestions.values():
                congestions.extend(clist)
        conditions = list(c.sql_where_condition() for c in congestions)
        return 'WHERE %s' % ' OR\n'.join(conditions)


# time_bucket,
# time_bucket_size_secs,
# ip_prefix_anon_num,
# emetro_anon_num,
# das_anon_num,
# econtinent,
# is_ipv6,
# in_p90,
# in_p95,
# in_p99,
# minrtt_ms_min,
# minrtt_ms_p5,
# minrtt_ms_p50,
# minrtt_ms_p95,
# srtt_ms_min,
# srtt_ms_p5,
# srtt_ms_p50,
# srtt_ms_p95,
# retrans_rate,
# min_bgp_as_path_len,
# max_bgp_as_path_len,
# num_bucket_samples,
# num_bucket_samples_transit,
# num_bucket_samples_peering,
# num_bucket_samples_peering_paid,
# num_bucket_samples_peering_private,
# num_bucket_samples_peering_public,
# num_bucket_samples_peering_route_server,
# weight_bucket_samples_to_tuple_total,
# weight_bucket_samples_to_metro_total,
# weight_bucket_samples_to_global_total,
# weight_tuple_samples_to_metro_total,
# weight_tuple_samples_to_global_total,
# weight_tuple_bps_to_metro_total,
# weight_tuple_bps_to_global_total,
# bucket_avg_bps_gt_25mbps,
# bucket_avg_bps_gt_50mbps,
# bucket_avg_bps_gt_100mbps
class PrefixData:
    def __init__(self):
        self.prefix = -1
        self.dstasn = -1
        # self.continent = 'nowhere'
        self.is_ipv6 = False
        self.key = (self.prefix, self.dstasn, self.is_ipv6)
        self.time2perf = dict()

    def addrow(self, row):
        assert self.prefix == int(row['ip_prefix_anon_num'])
        assert self.dstasn == int(row['das_anon_num'])
        # assert self.continent == str(row['econtinent'])
        assert self.is_ipv6 == V2BOOL[row['is_ipv6']]
        assert int(row['time_bucket']) not in self.time2perf
        # minrtt values in milisseconds
        perf = {'minrtt_min': int(row['minrtt_ms_min']),
                'minrtt_p5': int(row['minrtt_ms_p5']),
                'minrtt_p50': int(row['minrtt_ms_p50']),
                'minrtt_p95': int(row['minrtt_ms_p95']),
                'retrans_rate': float(row['retrans_rate']),
                'samples': int(row['num_bucket_samples']),
                '25mbps': V2BOOL[row['bucket_avg_bps_gt_25mbps']],
                '100mbps': V2BOOL[row['bucket_avg_bps_gt_100mbps']]}
        self.time2perf[int(row['time_bucket'])] = perf

    def merge(self, other):
        assert self.key == other.key
        for time, perf in other.time2perf.items():
            assert time not in self.time2perf, '%s' % str(self.key)
            self.time2perf[time] = perf

    @staticmethod
    def from_row(row):
        pd = PrefixData()
        pd.prefix = int(row['ip_prefix_anon_num'])
        pd.dstasn = int(row['das_anon_num'])
        # pd.continent = str(row['econtinent'])
        pd.is_ipv6 = V2BOOL[row['is_ipv6']]
        pd.key = (pd.prefix, pd.dstasn, pd.is_ipv6)
        # minrtt values in milisseconds
        perf = {'minrtt_min': int(row['minrtt_ms_min']),
                'minrtt_p5': int(row['minrtt_ms_p5']),
                'minrtt_p50': int(row['minrtt_ms_p50']),
                'minrtt_p95': int(row['minrtt_ms_p95']),
                'retrans_rate': float(row['retrans_rate']),
                'samples': int(row['num_bucket_samples']),
                '25mbps': V2BOOL[row['bucket_avg_bps_gt_25mbps']],
                '100mbps': V2BOOL[row['bucket_avg_bps_gt_100mbps']]}
        pd.time2perf[int(row['time_bucket'])] = perf
        return pd

    @staticmethod
    def from_dict_data(data):
        pd = PrefixData()
        assert set(pd.__dict__.keys()) == set(data.keys())
        # pylint: disable=W0201
        pd.__dict__ = data


class MetroPrefixPerf:
    def __init__(self, metro):
        self.metro = int(metro)
        self.sorted_volume1prefix = list()
        self.key2data = dict()

    @staticmethod
    def from_csv(csvfn, csvheaderfn):
        m = re.match(METRO_CSV_FILENAME_REGEX, os.path.basename(csvfn))
        if not m:
            logging.error('ERROR: filename %s does not match regex')
            return None
        mpp = MetroPrefixPerf(int(m.group(1)))
        with open(csvheaderfn) as fd:
            header = fd.readline().strip().split(',')
        fd = open(csvfn)
        reader = csv.DictReader(fd, fieldnames=header)
        for row in reader:
            assert int(row['emetro_anon_num']) == mpp.metro
            pd = PrefixData.from_row(row)
            if pd.key not in mpp.key2data:
                mpp.key2data[pd.key] = pd
            else:
                mpp.key2data[pd.key].merge(pd)
            v1p = (float(row['weight_tuple_bps_to_metro_total']), pd.key)
            mpp.sorted_volume1prefix.append(v1p)
        fd.close()
        mpp.sorted_volume1prefix.sort(reverse=True)
        return mpp


class IXPDB:
    def __init__(self, location=IXPDB_DEFAULT_DIR):
        self._netid2name = dict()
        self.ip2portinfo = dict()
        self.location = location

        with open(os.path.join(location, 'ix.json')) as fd:
            ix_data = json.load(fd)
            ix_data = list(i for i in ix_data['data'] if i['status'] == 'ok')
        with open(os.path.join(location, 'net.json')) as fd:
            net_data = json.load(fd)
            net_data = list(n for n in net_data['data'] if n['status'] == 'ok')
        with open(os.path.join(location, 'netixlan.json')) as fd:
            netixlan_data = json.load(fd)
            netixlan_data = list(n for n in netixlan_data['data']
                                 if n['status'] == 'ok')

        self._netid2name = dict((n['id'], n['name']) for n in net_data)
        for n in netixlan_data:
            stats = {'speed': int(n['speed']),
                     'ipaddr4': n['ipaddr4'],
                     'ipaddr6': n['ipaddr6'],
                     'asn': int(n['asn']),
                     'asn_name': self._netid2name[n['net_id']],
                     'ix_id': int(n['ix_id']),
                     'ix_name': n['name'],
                     'is_rs_peer': int(n['is_rs_peer'])}
            self.ip2portinfo[n['ipaddr4']] = stats
            self.ip2portinfo[n['ipaddr6']] = stats


def main():
    # generate ip2portinfo:
    ixpdb = IXPDB()
    fd = open(os.path.join(IXPDB_DEFAULT_DIR, 'ip2portinfo.json'), 'w')
    json.dump(ixpdb.ip2portinfo, fd, indent=2)
    fd.close()

    congestiondb = CongestionDB()
    # generate metro2stats:
    metro2stats = congestiondb.compute_metro2stats(ixpdb)
    fd = open(os.path.join(CONGESTION_DEFAULT_DIR, 'metro2stats.json'), 'w')
    json.dump(metro2stats, fd, indent=2)
    fd.close()
    # generate where.sql:
    where = congestiondb.sql_where_clause()
    fd = open(os.path.join(CONGESTION_DEFAULT_DIR, 'where.sql'), 'w')
    fd.write(where)
    fd.close()


if __name__ == '__main__':
    sys.exit(main())
