import json


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

DSCP_PRI = 48
DSCP_SEC = 49
DSCP_TER = 50
DSCP_ALTERNATES = frozenset([DSCP_SEC, DSCP_TER])

RT_TRANSIT = 'transit'
RT_PUBLIC = 'public'
RT_PRIVATE = 'private'
RT_ROUTESERVER = 'route_server'
ROUTE_TYPES = frozenset([RT_TRANSIT, RT_PUBLIC, RT_PRIVATE])
RT_ORDER = ['private', 'public', 'transit']


class MetroPrefixMeasurements:
    def __init__(self, metro, prefix, dscp2meas):
        self.metro = str(metro)
        self.prefix = str(prefix)
        self.dscp2meas = dict(dscp2meas)


class Measurement:
    def __init__(self, row):
        self.dscp = int(row[COL_APM_DSCP_VALUE_TXN_START])
        self.type = str(row[COL_ALTPATH_ACTIONS_ROUTE_TYPE_CLEANED])
        if self.type == RT_ROUTESERVER: self.type = RT_PUBLIC
        assert self.type in ROUTE_TYPES, self.type

        self.aspaths = json.loads(row[COL_ALTPATH_ACTIONS_ROUTE_AS_PATHS])
        self.aspaths = list([int(asn) for asn in path.split(',')]
                            for path in self.aspaths)
        self.peers = set(p[0] for p in self.aspaths)
        self.aspathlen = int(row[COL_ALTPATH_ACTIONS_ROUTE_BGP_AS_PATH_LEN])

        self.continent = str(row[COL_CLIENT_CONTINENT])
        if not self.continent:
            self.continent = 'XX'

        self.bps = float(row[COL_AVG_PREFIX_BPS])

        self.rtt50 = int(row[COL_RTT_MS_P50])
        self.rtt95 = int(row[COL_RTT_MS_P95])
        self.rtt_samples = int(row[COL_NUM_SAMPLES_FOR_RTT])
        self.tput5 = float(row[COL_THROUGHPUT_P5])
        self.tput50 = float(row[COL_THROUGHPUT_P50])
        self.tput_samples = float(row[COL_NUM_SAMPLES_FOR_THROUGHPUT])

    def pref_order(self, other):
        o1 = RT_ORDER.index(self.type)
        o2 = RT_ORDER.index(other.type)
        if o1 < o2: return -1
        if o1 > o2: return 1
        if self.aspathlen < other.aspathlen: return -1
        if self.aspathlen > other.aspathlen: return 1
        return 0
