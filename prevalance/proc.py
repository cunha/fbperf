#!/usr/bin/env python3

from collections import defaultdict
import csv
import gzip
import logging
import math
import os
import sys

import scipy.stats
import numpy

from csvhelp import Row, RouteInfo, RowParseError


BIN_SIZE = 900
MAX_MEASUREMENT_GAP_SIZE = 2
global_bytes_acked_sum = 0


def median_diff_ci(pri: RouteInfo, alt: RouteInfo, z=2) -> (int, int, int):
    med1 = pri.minrtt_ms_p50
    med2 = alt.minrtt_ms_p50
    var1 = pri.minrtt_ms_p50_var
    var2 = alt.minrtt_ms_p50_var
    md = med1 - med2
    interval = z * math.sqrt(var1 + var2)
    return (md - interval, md, md + interval)


class PrevalenceTracker:
    class Stats:
        def __init__(self, row):
            self.bytes_acked_sum = row.bytes_acked_sum
            primary, bestalt = row.get_primary_bestalt()
            self.pri_type = primary.peer_type
            self.pri_subtype = primary.peer_subtype
            self.alt_type = bestalt.peer_type
            self.alt_subtype = bestalt.peer_subtype
            self.median_diff_ci = median_diff_ci(primary, bestalt)

    class Summary:
        def __init__(self, time2stats, improvfunc):
            def get_nbins(tstamp1, tstamp2):
                return (tstamp2 - tstamp1) // BIN_SIZE

            series = sorted(time2stats.items())
            times, stats = zip(*series)

            self.bins = len(series)
            self.bins_improv = 0
            self.total_bytes = sum(s.bytes_acked_sum for s in stats)
            self.total_bytes_improv = 0
            self.num_shifts = 0

            max_streak, curr_streak = 0, 0
            is_moved = False

            # w = sys.stdout.write
            # w("################################\n")
            for i, s in enumerate(stats):
                should_move = improvfunc(s)
                if not should_move:
                    max_streak = max(max_streak, curr_streak)
                    curr_streak = 0
                else:
                    self.bins_improv += 1
                    self.total_bytes_improv += s.bytes_acked_sum
                    if not is_moved:
                        self.num_shifts += 1
                        nbins = 1
                    else:
                        nbins = (
                            1 if i == 0 else get_nbins(times[i - 1], times[i])
                        )
                    if nbins <= MAX_MEASUREMENT_GAP_SIZE + 1:
                        curr_streak += nbins
                    else:
                        max_streak = max(max_streak, curr_streak)
                        curr_streak = 0
                is_moved = should_move
                # w(
                #     "%d %d %d %d %d %d\n"
                #     % (
                #         times[i],
                #         int(should_move),
                #         self.bins_improv,
                #         self.total_bytes_improv,
                #         max_streak,
                #         curr_streak,
                #     )
                # )

            max(max_streak, curr_streak)
            self.longest_shifted_bin_streak = max_streak

    def __init__(self):
        self.key2time2stats = defaultdict(dict)

    def update(self, row):
        t = row.time_bucket
        self.key2time2stats[row.key()][t] = PrevalenceTracker.Stats(row)

    def dump_cdfs(self, outdir, improvfunc=lambda s: s.median_diff_ci[0] > 0):
        summaries = list(
            PrevalenceTracker.Summary(t2s, improvfunc)
            for t2s in self.key2time2stats.values()
        )
        summaries = list(s for s in summaries if s.bins > 24)
        with open(os.path.join(outdir, "summaries-nbins.cdf"), "w") as fd:
            data = sorted(s.bins_improv / s.bins for s in summaries)
            xs, ys = makecdf(data)
            dumpcdf(xs, ys, fd)


def makecdf(data):
    counts, edges = numpy.histogram(data, bins=1000, density=True)
    ys = numpy.cumsum(counts)
    ys /= ys[-1]
    return edges[1:], ys

    # res = scipy.stats.relfreq(data, numbins=10000)
    # assert res.frequency.size == 10000
    # ys = np.cumsum(res.numpy.zeros(10000)
    # for i in range(1, 10000):
    #     ys[i] = ys[i - 1] + res.frequency[i]
    # xs = res.lowerlimit + numpy.linspace(0, res.binsize * 10000, 10000)
    # return xs, ys


def dumpcdf(xs, ys, fd):
    for x, y in zip(xs, ys):
        fd.write("%f %f\n" % (x, y))


def main():
    logging.basicConfig(
        level=logging.DEBUG, format="%(asctime)s:%(levelname)s: %(message)s"
    )
    logging.info("starting up")

    tracker = PrevalenceTracker()
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
        tracker.update(row)

    tracker.dump_cdfs("output/")

    logging.info("processed %d rows", nrows)
    outfd.close()


if __name__ == "__main__":
    sys.exit(main())
