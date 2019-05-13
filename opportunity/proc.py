#!/usr/bin/env python3

from collections import defaultdict
import csv
import gzip
import logging
import math
import sys
from typing import Callable

from csvhelp import Row, RouteInfo, RowParseError
import csvhelp

CONFIG = {"minrtt_min_samples": 200, "hdratio_min_samples": 200}

PEER_SUBTYPE_MAP = {
    "private": "private",
    "": "",
    "public": "public",
    "paid": "public",
    "route_server": "public",
    "mixed": "private",
}

PEER_TYPE_ORDER = [
    ("peering", "private"),
    ("peering", "public"),
    ("transit", ""),
]

# This gets applied after SUBTYPE_MAP above, so the mixed subtype has been
# replaced by private:
PEER_TYPE_IGNORE = [("mixed", "private")]


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
        # assert pri.minrtt_ms_p50 - alt.minrtt_ms_p50 > 0, "no opportunity?"
        pritype = (pri.peer_type, PEER_SUBTYPE_MAP[pri.peer_subtype])
        alttype = (alt.peer_type, PEER_SUBTYPE_MAP[alt.peer_subtype])
        prop2bytes = self.pri2alt2prop2bytes[pritype][alttype]
        prop2bytes["total"] += row.bytes_acked_sum
        for propfunc in PROPERTY_FUNCTIONS:
            label = propfunc(pri, alt)
            prop2bytes[label] += row.bytes_acked_sum

    def dump_latex_string(self, stream):
        stream.write("%% %s\n" % self.description)
        for pri in PEER_TYPE_ORDER:
            for alt in PEER_TYPE_ORDER:
                prop2bytes = self.pri2alt2prop2bytes[pri][alt]
                if pri in PEER_TYPE_IGNORE or alt in PEER_TYPE_IGNORE:
                    continue
                # if PEER_TYPE_ORDER.index(alt) < PEER_TYPE_ORDER.index(pri):
                #     continue
                pristr = "%s/%s" % pri
                altstr = "%s/%s" % alt
                if pristr.endswith("/"):
                    pristr = pristr[:-1]
                if altstr.endswith("/"):
                    altstr = altstr[:-1]
                improv = 1000 * prop2bytes["total"] / global_bytes_acked_sum
                longer = 1000 * prop2bytes["longer"] / global_bytes_acked_sum
                prepended = (
                    1000
                    * prop2bytes["alt_is_prepended_more"]
                    / global_bytes_acked_sum
                )
                string = (
                    r"%s & %s & %.2f\permil & %.2f\permil & %.2f\permil"
                    % (pristr, altstr, improv, longer, prepended)
                )
                string += "\\\\\n"
                stream.write(string)
                string = "%% %.2f, %.2f, %.2f, %.2f, %.2f, %.2f\n" % (
                    1000
                    * prop2bytes["shorter_wo_prepend"]
                    / global_bytes_acked_sum,
                    1000
                    * prop2bytes["equal_wo_prepend"]
                    / global_bytes_acked_sum,
                    1000
                    * prop2bytes["longer_wo_prepend"]
                    / global_bytes_acked_sum,
                    1000 * prop2bytes["shorter"] / global_bytes_acked_sum,
                    1000 * prop2bytes["equal"] / global_bytes_acked_sum,
                    1000 * prop2bytes["longer"] / global_bytes_acked_sum,
                )
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
            return "longer_wo_prepend"
        if prilen == altlen:
            return "equal_wo_prepend"
        return "shorter_wo_prepend"

    @staticmethod
    def prop_has_more_prepend(pri, alt):
        priprep = (
            pri.bgp_as_path_len - pri.bgp_as_path_min_len_prepending_removed
        )
        altprep = (
            alt.bgp_as_path_len - alt.bgp_as_path_min_len_prepending_removed
        )
        if priprep < altprep:
            return "alt_is_prepended_more"
        else:
            return "alt_is_not_prepended_more"


PROPERTY_FUNCTIONS = [
    ImprovementTracker.prop_length,
    ImprovementTracker.prop_length_wo_prepend,
    ImprovementTracker.prop_has_more_prepend,
]


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


def dump_ci_diffs(row, rttfd, hdrfd):
    primary, bestalt = row.get_primary_bestalt_minrtt(
        CONFIG["minrtt_min_samples"]
    )
    if primary is not None:
        bestalt = primary if bestalt is None else bestalt
        for improv in MINRTT_IMPROV_PEER_TRACKERS:
            improv.update(row, primary, bestalt)
        Qs = csvhelp.rtt_median_diff_ci(primary, bestalt)
        string = "%d %d %d %s %s %f %f %f\n" % (
            int(row.client_is_ipv6),
            row.bytes_acked_sum,
            int(primary.csv_rt_num != bestalt.csv_rt_num),
            primary.peer_type,
            bestalt.peer_type,
            Qs[0],
            Qs[1],
            Qs[2],
        )
        rttfd.write(string.encode("utf-8"))

    primary, bestalt = row.get_primary_bestalt_hdratio(
        CONFIG["hdratio_min_samples"]
    )
    if primary is not None:
        bestalt = primary if bestalt is None else bestalt
        for improv in HDRATIO_IMPROV_PEER_TRACKERS:
            improv.update(row, primary, bestalt)
        Qs = csvhelp.hdr_mean_diff_ci(primary, bestalt)
        string = "%d %d %d %s %s %f %f %f\n" % (
            int(row.client_is_ipv6),
            row.bytes_acked_sum,
            int(primary.csv_rt_num != bestalt.csv_rt_num),
            primary.peer_type,
            bestalt.peer_type,
            Qs[0],
            Qs[1],
            Qs[2],
        )
        hdrfd.write(string.encode("utf-8"))


MINRTT_IMPROV_PEER_TRACKERS = [
    ImprovementTracker(
        "med minrtt diff ci lower bound > 0ms",
        lambda r, pri, alt: csvhelp.rtt_median_diff_ci(pri, alt)[0] > 0,
    ),
    ImprovementTracker(
        "med minrtt diff ci lower bound > 5 ms",
        lambda r, pri, alt: csvhelp.rtt_median_diff_ci(pri, alt)[0] > 5,
    ),
    ImprovementTracker(
        "med minrtt diff ci lower bound > 10 ms",
        lambda r, pri, alt: csvhelp.rtt_median_diff_ci(pri, alt)[0] > 10,
    ),
]

HDRATIO_IMPROV_PEER_TRACKERS = [
    ImprovementTracker(
        "mean hdratio diff ci lower bound > 0",
        lambda r, pri, alt: csvhelp.hdr_mean_diff_ci(pri, alt)[0] > 0.0,
    ),
    ImprovementTracker(
        "mean hdratio diff ci lower bound > 0.05",
        lambda r, pri, alt: csvhelp.hdr_mean_diff_ci(pri, alt)[0] > 0.05,
    ),
    ImprovementTracker(
        "mean hdratio diff ci lower bound > 0.1",
        lambda r, pri, alt: csvhelp.hdr_mean_diff_ci(pri, alt)[0] > 0.1,
    ),
]

global_bytes_acked_sum = 0


def main():
    logging.basicConfig(
        level=logging.DEBUG, format="%(asctime)s:%(levelname)s: %(message)s"
    )
    logging.info("starting up")

    global global_bytes_acked_sum
    rttfd = gzip.open("output/bestalt-vs-pri-rtt.csv.gz", "w")
    hdrfd = gzip.open("output/bestalt-vs-pri-hdr.csv.gz", "w")
    reader = csv.DictReader(sys.stdin, delimiter="\t")
    nrows = 0

    for csvrow in reader:
        nrows += 1
        try:
            row = Row(csvrow)
        except RowParseError:
            continue
        global_bytes_acked_sum += row.bytes_acked_sum  # pylint: disable=E1101
        dump_ci_diffs(row, rttfd, hdrfd)

    sys.stdout.write("% MINRTT\n")
    for improv in MINRTT_IMPROV_PEER_TRACKERS:
        improv.dump_latex_string(sys.stdout)

    sys.stdout.write("% HDRATIO\n")
    for improv in HDRATIO_IMPROV_PEER_TRACKERS:
        improv.dump_latex_string(sys.stdout)

    logging.info("processed %d rows", nrows)
    rttfd.close()
    hdrfd.close()


if __name__ == "__main__":
    sys.exit(main())
