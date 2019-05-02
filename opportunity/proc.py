#!/usr/bin/env python3

from collections import defaultdict
import csv
import gzip
import logging
import math
import sys
from typing import Callable

from csvhelp import Row, RouteInfo, RowParseError


class ImprovementTracker:
    def __init__(
        self,
        description: str,
        validfunc: Callable[[Row, RouteInfo, RouteInfo], bool],
    ):
        self.pri2alt2prop2bytes = defaultdict(
            lambda: defaultdict(lambda: defaultdict(lambda: 0))
        )
        self.description = description
        self.validfunc = validfunc

    def update(self, row, pri, alt):
        if not self.validfunc(row, pri, alt):
            return
        assert pri.minrtt_ms_p50 - alt.minrtt_ms_p50 > 0, "no opportunity?"
        pritype = (pri.peer_type, pri.peer_subtype)
        alttype = (alt.peer_type, alt.peer_subtype)
        prop2bytes = self.pri2alt2prop2bytes[pritype][alttype]
        prop2bytes["total"] += row.bytes_acked_sum
        for propfunc in PROPERTY_FUNCTIONS:
            label = propfunc(pri, alt)
            prop2bytes[label] += row.bytes_acked_sum

    def dump_latex_string(self, stream):
        stream.write("%% %s\n" % self.description)
        for pri, alt2prop2bytes in self.pri2alt2prop2bytes.items():
            for alt, prop2bytes in alt2prop2bytes.items():
                pristr = "%s/%s" % pri
                altstr = "%s/%s" % alt
                improv = 100 * prop2bytes["total"] / global_bytes_acked_sum
                string = "%s & %s & %.1f%% " % (pristr, altstr, improv)
                string += "[%.1f%%, %.1f%%, %.1f%%] " % (
                        100 * prop2bytes["shorter"] / global_bytes_acked_sum
                        100 * prop2bytes["equal"] / global_bytes_acked_sum
                        100 * prop2bytes["longer"] / global_bytes_acked_sum
                    )
                string += "\\\\\n"
                stream.write(string)

    @staticmethod
    def prop_length(pri, alt):
        prilen = pri.bgp_as_path_len
        altlen = alt.bgp_as_path_len
        if prilen < altlen:
            return "longer"
        if prilen == altlen:
            return "equal"
        return "shorter"

    @staticmethod
    def prop_length_wo_prepend(pri, alt):
        prilen = pri.bgp_as_path_min_len_prepending_removed
        altlen = alt.bgp_as_path_min_len_prepending_removed
        if prilen < altlen:
            return "longer"
        if prilen == altlen:
            return "equal"
        return "shorter"


PROPERTY_FUNCTIONS = [
    ImprovementTracker.prop_length,
    ImprovementTracker.prop_length_wo_prepend,
]


def median_diff_ci(pri: RouteInfo, alt: RouteInfo, z=2) -> (int, int, int):
    med1 = pri.minrtt_ms_p50
    med2 = alt.minrtt_ms_p50
    var1 = pri.minrtt_ms_p50_var
    var2 = alt.minrtt_ms_p50_var
    md = med1 - med2
    interval = z * math.sqrt(var1 + var2)
    return (md - interval, md, md + interval)


DUMP_HEADERS = [
    "is_v6",
    "bytes_acked_sum",
    "has_alternate_route",
    "primary_peer_type",
    "alternate_peer_type",
    "median_improv_lb",
    "median_improv",
    "median_improv_ub",
]


def dump_ci_diff(row, primary, alternate, outfd):
    Qs = median_diff_ci(primary, alternate)
    string = "%d %d %d %s %s %f %f %f\n" % (
        int(row.client_is_ipv6),
        row.bytes_acked_sum,
        int(primary.csv_rt_num != alternate.csv_rt_num),
        primary.peer_type,
        alternate.peer_type,
        Qs[0],
        Qs[1],
        Qs[2],
    )
    outfd.write(string.encode("utf-8"))


IMPROV_PEER_TRACKERS = [
    ImprovementTracker(
        "med minrtt diff ci lower bound > 0ms",
        lambda r, pri, alt: median_diff_ci(pri, alt)[0] > 0,
    ),
    ImprovementTracker(
        "med minrtt diff > 5 ms",
        lambda r, pri, alt: median_diff_ci(pri, alt)[1] > 5,
    ),
    ImprovementTracker(
        "med minrtt diff > 5ms and ci lower bound > 0ms",
        lambda r, pri, alt: median_diff_ci(pri, alt)[0] > 0
        and median_diff_ci(pri, alt)[1] > 5,
    ),
]

global_bytes_acked_sum = 0


def main():
    logging.basicConfig(
        level=logging.DEBUG, format="%(asctime)s:%(levelname)s: %(message)s"
    )
    logging.info("starting up")

    global global_bytes_acked_sum
    outfd = gzip.open("output/bestalt-vs-pri.csv.gz", "w")
    reader = csv.DictReader(sys.stdin, delimiter="\t")
    nrows = 0

    for csvrow in reader:
        nrows += 1
        try:
            row = Row(csvrow)
        except RowParseError:
            continue
        global_bytes_acked_sum += row.bytes_acked_sum
        primary = row.primary_route()
        if primary is None:
            continue
        bestalt = row.best_alternate_route()
        if bestalt is None:
            bestalt = primary
        dump_ci_diff(row, primary, bestalt, outfd)

        for improv in IMPROV_PEER_TRACKERS:
            improv.update(row, primary, bestalt)

    for improv in IMPROV_PEER_TRACKERS:
        improv.dump_latex_string(sys.stdout)

    logging.info("processed %d rows", nrows)
    outfd.close()


if __name__ == "__main__":
    sys.exit(main())
