#!/usr/bin/env python3

from collections import defaultdict
import csv
import json
import logging
import math
import os
import sys

# import scipy.stats
import numpy

from csvhelp import Row, RouteInfo, RowParseError


CONFIG = {
        # This is just so we can compute the number of bins between two
        # time stamps:
        'bin_duration_secs': 900,

        # This is the number of bins we require to consider a <POP, prefix>
        # tuple has enough data for prevalence analysis.
        #
        # Update 20190507.1442: Brandon found that most of the traffic is
        # concentrated in prefixes that have enough samples for most bins
        # (expected), so we require that prefixes have 80 (15-min) bins (20h).
        'min_number_of_bins': 80,

        # A *shift period* is defined as a contiguous period of time where the
        # best alternate path is better than the primary.  This period can
        # contain gaps, i.e., bins where we did not have enough samples.  The
        # size of each limited by shift_period_max_measurement_gap_in_bins;
        # gaps with more bins terminate a shift period.
        'shift_period_max_measurement_gap_in_bins': 2,

        # For some of the analysis we want to filter for prefixes where
        # "significant" improvement can be achieved.  The absolute number of
        # bins that is considered "significant":
        'significant_improv_min_bins': 8
}

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
                return (tstamp2 - tstamp1) // CONFIG['bin_duration_secs']

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
                    if nbins <= CONFIG['shift_period_max_measurement_gap_in_bins'] + 1:
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

        summaries = list(s for s in summaries if s.bins > CONFIG['min_number_of_bins'])
        summaries_improv = list(s for s in summaries if s.bins_improv > 0)
        summaries_sig = list(s for s in summaries
                if s.bins_improv >= CONFIG['significant_improv_min_bins'])

        total = sum(s.total_bytes for s in summaries)
        ratio = total / global_bytes_acked_sum
        total_improv = sum(s.total_bytes for s in summaries_improv)
        ratio_improv = total / global_bytes_acked_sum
        total_sig = sum(s.total_bytes for s in summaries_sig)
        ratio_sig = total / global_bytes_acked_sum

        sys.stdout.write('total traffic %d %f\n' % (total, ratio))
        sys.stdout.write('total traffic in prefixes with improvement %d %f\n' % (total_improv, ratio_improv))
        sys.stdout.write('total traffic in prefixes with significant improvement %d %f\n' % (total_sig, ratio_sig))

        data = sorted(s.bins_improv / s.bins for s in summaries)
        xs, ys = makecdf(data)
        with open(os.path.join(outdir, "frac-improved-bins.cdf"), "w") as fd:
            dumpcdf(xs, ys, fd)

        with open('output/test.txt', 'w') as fd:
            for d in data:
                fd.write('%f\n' % d)

        data = sorted(s.total_bytes_improv / s.total_bytes for s in summaries)
        xs, ys = makecdf(data)
        with open(os.path.join(outdir, "frac-improved-bytes.cdf"), "w") as fd:
            dumpcdf(xs, ys, fd)

        data = sorted(s.num_shifts / s.bins for s in summaries)
        xs, ys = makecdf(data)
        with open(os.path.join(outdir, "ratio-shifts-to-nbins.cdf"), "w") as fd:
            dumpcdf(xs, ys, fd)

        data = sorted(s.num_shifts / s.bins for s in summaries_sig)
        xs, ys = makecdf(data)
        with open(os.path.join(outdir, "ratio-shifts-to-nbins-significant.cdf"), "w") as fd:
            dumpcdf(xs, ys, fd)

        data = sorted(s.longest_shifted_bin_streak / s.bins_improv
                for s in summaries_improv)
        xs, ys = makecdf(data)
        with open(os.path.join(outdir, "frac-improved-bins-in-longest-streak.cdf"), "w") as fd:
            dumpcdf(xs, ys, fd)

        data = sorted(s.longest_shifted_bin_streak / s.bins_improv
                for s in summaries_sig)
        xs, ys = makecdf(data)
        with open(os.path.join(outdir, "frac-improved-bins-in-longest-streak-significant.cdf"), "w") as fd:
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
    logging.info("configuration:")
    logging.info(json.dumps(CONFIG, indent=2))

    tracker = PrevalenceTracker()
    global global_bytes_acked_sum
    global_bytes_acked_sum = 0
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


if __name__ == "__main__":
    sys.exit(main())
