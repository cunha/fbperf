from collections import Counter, defaultdict
import csv
import ipaddress
import json
import math
from typing import Callable, Mapping

# pylint: disable=E1101

def _str_to_bool(string: str):
    return string in ("true", "True", "OK", "ok", "Ok")

def _peer_subtype_map(subtype: str):
    return PEER_SUBTYPE_MAP[subtype]

PEER_SUBTYPE_MAP = {
    "private": "private",
    "": "",
    "paid": "paid",
    "public": "public",
    "route_server": "public",
    "mixed": "private",
}

PEER_TYPE_ORDER = [
    ("peering", "private"),
    ("peering", "public"),
    ("peering", "paid"),
    ("transit", ""),
]


class RowParseError(RuntimeError):
    pass


class RouteInfoParseError(RuntimeError):
    pass


class RouteInfo:
    FIELDS = {
        "num_samples": int,
        "apm_route_num": int,
        "bgp_as_path_len": int,
        "bgp_as_path_min_len_prepending_removed": int,
        "minrtt_ms_p10": int,
        "minrtt_ms_p50": int,
        "minrtt_ms_p50_ci_lb": int,
        "minrtt_ms_p50_ci_ub": int,
        "hdratio_num_samples": int,
        "minrtt_ms_p50_var": float,
        "hdratio": float,
        "hdratio_var": float,
        "bgp_as_path_prepending": _str_to_bool,
        "peer_type": str,
        "peer_subtype": _peer_subtype_map,
        "px_nexthops": json.loads,
    }

    def __init__(self, number: int, csvrow):
        findex = lambda fname: "r%d_%s" % (number, fname)
        if csvrow[findex("num_samples")] == "NULL":
            raise RouteInfoParseError("missing route %d" % number)

        self.number = int(number)
        for fname, fparser in RouteInfo.FIELDS.items():
            if csvrow[findex(fname)] == "NULL":
                setattr(self, fname, None)
            else:
                setattr(self, fname, fparser(csvrow[findex(fname)]))

    @staticmethod
    def minrtt_median_diff_ci(rt1, rt2, z=2) -> (int, int, int):
        assert rt1.number == 0, "Can only compute diff on primary route"
        med1 = rt1.minrtt_ms_p50
        med2 = rt2.minrtt_ms_p50
        var1 = rt1.minrtt_ms_p50_var
        var2 = rt2.minrtt_ms_p50_var
        md = med1 - med2
        interval = z * math.sqrt(var1 + var2)
        return (md - interval, md, md + interval)

    @staticmethod
    def hdratio_diff_ci(rt1, rt2, z=2) -> (float, float, float):
        assert rt1.number == 0, "Can only compute diff on primary route"
        avg1 = rt1.hdratio
        avg2 = rt2.hdratio
        var1 = rt1.hdratio_var
        var2 = rt2.hdratio_var
        n1 = rt1.hdratio_num_samples
        n2 = rt2.hdratio_num_samples
        diff = avg2 - avg1
        interval = z * math.sqrt(var1 / n1 + var2 / n2)
        return (diff - interval, diff, diff + interval)

    @staticmethod
    def compare_median_minrtt(rt1, rt2) -> int:
        rtt1 = rt1.minrtt_ms_p50
        rtt2 = rt2.minrtt_ms_p50
        return (rtt1 > rtt2) - (rtt1 < rtt2)

    @staticmethod
    def compare_hdratio(rt1, rt2) -> int:
        hdr1 = rt1.hdratio
        hdr2 = rt2.hdratio
        return (hdr1 > hdr2) - (hdr1 < hdr2)

    # @staticmethod
    # def generate_min_samples_validator(minrtt_samples: int,
    #         hdratio_samples: int) -> Callable[[RouteInfo], bool]:
    #     def min_samples_validator(route: RouteInfo) -> bool:
    #         return (route.num_samples > minrtt_samples and
    #                 route.hdratio_num_samples > hdratio_samples)
    #     return min_samples_validator

    class MaxMinRttCiSizeValidator:
        def __init__(self, median_minrtt_ci_ms: int):
            self.median_minrtt_ci_ms = median_minrtt_ci_ms

        def __call__(self, route):
            minrtt_ci = route.minrtt_ms_p50_ci_ub - route.minrtt_ms_p50_ci_lb
            return minrtt_ci < self.median_minrtt_ci_ms

        def __str__(self):
            return "max-ci-%d" % self.median_minrtt_ci_ms

    class MaxHdRatioCiSizeValidator:
        def __init__(self, average_hdratio_ci: float, z=2):
            self.average_hdratio_ci = average_hdratio_ci
            self.z = z

        def __call__(self, route):
            hdratio_ci = self.z * math.sqrt(route.hdratio_var / route.hdratio_num_samples)
            return hdratio_ci < self.average_hdratio_ci

        def __str__(self):
            return "max-ci-%0.2f" % self.average_hdratio_ci

    class MaxCiSizeValidator:
        def __init__(self, median_minrtt_ci_ms: int, average_hdratio_ci: float, z=2):
            self.median_minrtt_ci_ms = median_minrtt_ci_ms
            self.average_hdratio_ci = average_hdratio_ci
            self.z = z

        def __call__(self, route):
            minrtt_ci = route.minrtt_ms_p50_ci_ub - route.minrtt_ms_p50_ci_lb
            hdratio_ci = self.z * math.sqrt(route.hdratio_var / route.hdratio_num_samples)
            return minrtt_ci < self.median_minrtt_ci_ms and hdratio_ci < self.average_hdratio_ci

        def __str__(self):
            return "max-ci-%d-%0.2f" % (self.median_minrtt_ci_ms, self.average_hdratio_ci)


class DB:
    def __init__(self, reader: csv.DictReader, validator: Callable[[RouteInfo], bool]):
        self.pid2time2bin = defaultdict(dict)
        self.pid2traffic = Counter()
        self.total_traffic = 0
        self.rows = 0
        self.parse_errors = 0
        for csvrow in reader:
            self.rows += 1
            try:
                pid = PathId(csvrow)
                timebin = TimeBin(csvrow, validator)
            except RowParseError:
                self.parse_errors += 1
                continue
            self.pid2time2bin[pid][timebin.time_bucket] = timebin
            self.pid2traffic[pid] += timebin.bytes_acked_sum
            self.total_traffic += timebin.bytes_acked_sum


class PathId:
    FIELDS = {"bgp_ip_prefix": ipaddress.ip_network, "vip_metro": str, "client_is_ipv6": bool}

    def __init__(self, csvrow: Mapping[str, str]):
        for fname, fparser in PathId.FIELDS.items():
            if csvrow[fname] == "NULL":
                raise RowParseError("column %s is NULL" % fname)
            setattr(self, fname, fparser(csvrow[fname]))

    def __hash__(self):
        return hash((self.bgp_ip_prefix, self.vip_metro))

    def __eq__(self, other):
        return self.bgp_ip_prefix == other.bgp_ip_prefix and self.vip_metro == other.vip_metro


class TimeBin:
    FIELDS = {"bytes_acked_sum": int, "time_bucket": int}
    MAX_ROUTE_NUM = 7

    def __init__(self, csvrow: Mapping[str, str], validator: Callable[[RouteInfo], bool]):
        for fname, fparser in TimeBin.FIELDS.items():
            if csvrow[fname] == "NULL":
                raise RowParseError("column %s is NULL" % fname)
            setattr(self, fname, fparser(csvrow[fname]))
        self.parsing_errors = 0
        self.validation_errors = 0
        self.num2rtinfo: Mapping[int, RouteInfo] = dict()
        for i in range(TimeBin.MAX_ROUTE_NUM + 1):
            try:
                rtinfo = RouteInfo(i, csvrow)
                if validator(rtinfo):
                    self.num2rtinfo[i] = rtinfo
                else:
                    self.validation_errors += 1
            except RouteInfoParseError:
                self.parsing_errors += 1

    def get_primary_route(self):
        return self.num2rtinfo.get(0, None)

    def get_best_alternate(self, compare: Callable[[RouteInfo, RouteInfo], int]):
        bestrt = None
        for rtinfo in self.num2rtinfo.values():
            if rtinfo.apm_route_num == 1:  # Ignoring primary (ECMP'd routes)
                continue
            if bestrt is None:
                bestrt = rtinfo
            elif compare(bestrt, rtinfo) < 0:
                bestrt = rtinfo
        return bestrt
