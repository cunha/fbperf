#!/usr/bin/env python3

from collections import defaultdict
import csv
import json
import logging
import math
import os
import sys

from csvhelp import Row, RouteInfo, RowParseError
from buildcdf import buildcdf


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

        # Minimum number of samples required; we do not need this for MinRTT
        # because Brandon only exports if we have enough MinRTT samples.
        'hdratio_min_samples': 100,
        'minrtt_min_samples': 200,

        # For some of the analysis we want to filter for prefixes where
        # "significant" improvement can be achieved.  The absolute number of
        # bins that is considered "significant":
        'significant_improv_min_bins': 8,

        # This is in addition to the lower bound of the median difference
        # confidence interval being larger than 0.
        'deprecated_min_median_rtt_improvement_ms': 5,

        # A *shift period* is defined as a contiguous period of time where the
        # best alternate path is better than the primary.  This period can
        # contain gaps, i.e., bins where we did not have enough samples.  The
        # size of each limited by shift_period_max_measurement_gap_in_bins;
        # gaps with more bins terminate a shift period.
        'deprecated_shift_period_max_measurement_gap_in_bins': 2,
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


def mean_diff_ci(pri: RouteInfo, alt: RouteInfo, z=2) -> (int, int, int):
    avg1 = pri.hdratio
    avg2 = alt.hdratio
    var1 = pri.hdratio_var
    var2 = alt.hdratio_var
    n1 = pri.hdratio_num_samples
    n2 = alt.hdratio_num_samples
    diff = avg1 - avg2
    interval = z * math.sqrt(var1/n1 + var2/n2)
    return (diff - interval, diff, diff + interval)


class NotEnoughSamplesError(ValueError):
    pass


class PrevalenceTracker:
    class MinRttStats:
        def __init__(self, row):
            self.bytes_acked_sum = row.bytes_acked_sum
            primary, bestalt = row.get_primary_bestalt_minrtt(CONFIG['minrtt_min_samples'])
            if primary is None:
                raise NotEnoughSamplesError()
            self.pri_type = primary.peer_type
            self.pri_subtype = primary.peer_subtype
            self.alt_type = bestalt.peer_type
            self.alt_subtype = bestalt.peer_subtype
            self.diff_ci = median_diff_ci(primary, bestalt)

    class HdRatioStats:
        def __init__(self, row):
            self.bytes_acked_sum = row.bytes_acked_sum
            primary, bestalt = row.get_primary_bestalt_hdratio(CONFIG['hdratio_min_samples'])
            if primary is None:
                raise NotEnoughSamplesError()
            self.pri_type = primary.peer_type
            self.pri_subtype = primary.peer_subtype
            self.alt_type = bestalt.peer_type
            self.alt_subtype = bestalt.peer_subtype
            self.diff_ci = mean_diff_ci(primary, bestalt)

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
                nbins = 1 if i == 0 else get_nbins(times[i - 1], times[i])

                # We may want to change the code if nbins is large. We
                # currently do nothing special because we only consider
                # prefixes with at least 80 bins (see min_number_of_bins and
                # shift_period_max_measurement_gap_in_bins).

                should_move = improvfunc(s)
                if should_move == -1:
                    max_streak = max(max_streak, curr_streak)
                    curr_streak = 0
                    is_moved = False
                elif should_move == 0:
                    if is_moved:
                        self.bins_improv += nbins
                        # We multiply the amount of bytes by nbins:
                        self.total_bytes_improv += s.bytes_acked_sum*nbins
                        curr_streak += nbins
                elif should_move == 1:
                    if not is_moved:
                        self.num_shifts += 1
                        nbins = 1
                    self.bins_improv += nbins
                    self.total_bytes_improv += s.bytes_acked_sum*nbins
                    curr_streak += nbins
                    is_moved = True
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

            max_streak = max(max_streak, curr_streak)
            self.longest_shifted_bin_streak = max_streak

    def __init__(self):
        self.key2time2rttstats = defaultdict(dict)
        self.key2time2hdrstats = defaultdict(dict)

    def update(self, row):
        t = row.time_bucket
        k = row.key()
        try:
            self.key2time2rttstats[k][t] = PrevalenceTracker.MinRttStats(row)
        except NotEnoughSamplesError as se:
            pass
        try:
            self.key2time2hdrstats[k][t] = PrevalenceTracker.HdRatioStats(row)
        except NotEnoughSamplesError as se:
            pass

    def dump_cdfs_minrtt(self, outdir, improvfunc):
        key2summary = dict(
            (key, PrevalenceTracker.Summary(t2s, improvfunc))
            for key, t2s in self.key2time2rttstats.items()
        )
        dump_cdfs_key2sum(outdir, key2summary)

    def dump_cdfs_hdratio(self, outdir, improvfunc):
        key2summary = dict(
            (key, PrevalenceTracker.Summary(t2s, improvfunc))
            for key, t2s in self.key2time2hdrstats.items()
        )
        dump_cdfs_key2sum(outdir, key2summary)


def dump_cdfs_key2sum(outdir, key2sum):
    summaries = list(key2sum.values())
    summaries = list(s for s in summaries
            if s.bins > CONFIG['min_number_of_bins'])
    out_subdir = os.path.join(outdir, 'min_number_of_bins')
    dump_cdfs_sum(out_subdir, summaries)

    summaries_sig = list(s for s in summaries
            if s.bins_improv >= CONFIG['significant_improv_min_bins'])
    out_subdir = os.path.join(outdir, 'significant_improv_min_bins')
    dump_cdfs_sum(out_subdir, summaries_sig)

    total = sum(s.total_bytes for s in summaries)
    ratio = total / global_bytes_acked_sum
    total_sig = sum(s.total_bytes for s in summaries_sig)
    ratio_sig = total_sig / global_bytes_acked_sum

    continuous_improv_prefixes = list(k for k, s in key2sum.items()
            if s.bins > CONFIG['min_number_of_bins'] and
            s.bins_improv == s.bins)
    outfn = os.path.join(outdir, 'continuous-improv-prefixes.data')
    dump_keys(continuous_improv_prefixes, outfn)

    sys.stdout.write('total traffic %d %f\n' % (total, ratio))
    sys.stdout.write('total traffic in prefixes with significant improvement %d %f\n' % (total_sig, ratio_sig))


def make_default_weight_iterator(seq):
    for x in seq:
        yield x, 1.0
    yield None, None

def make_weighted_iterator(seq):
    for x, y in seq:
        yield x, y
    yield None, None


def dump_cdfs_sum(outdir, summaries):
    os.makedirs(outdir, exist_ok=True)

    data = sorted(s.bins_improv / s.bins for s in summaries)
    xs, ys = zip(*buildcdf(make_default_weight_iterator(data)))
    outfn = os.path.join(outdir, "frac-improved-bins.cdf")
    with open(outfn, "w") as fd:
        dumpcdf(xs, ys, fd)

    data = sorted((s.bins_improv / s.bins, s.total_bytes) for s in summaries)
    xs, ys = zip(*buildcdf(make_weighted_iterator(data)))
    outfn = os.path.join(outdir, "frac-improved-bins-weighted.cdf")
    with open(outfn, "w") as fd:
        dumpcdf(xs, ys, fd)

    data = sorted(s.total_bytes_improv / s.total_bytes for s in summaries)
    xs, ys = zip(*buildcdf(make_default_weight_iterator(data)))
    outfn = os.path.join(outdir, "frac-improved-bytes.cdf")
    with open(outfn, "w") as fd:
        dumpcdf(xs, ys, fd)

    data = sorted((s.total_bytes_improv / s.total_bytes, s.total_bytes)
            for s in summaries)
    xs, ys = zip(*buildcdf(make_weighted_iterator(data)))
    outfn = os.path.join(outdir, "frac-improved-bytes-weighted.cdf")
    with open(outfn, "w") as fd:
        dumpcdf(xs, ys, fd)

    data = sorted(s.num_shifts for s in summaries)
    xs, ys = zip(*buildcdf(make_default_weight_iterator(data)))
    outfn = os.path.join(outdir, "num-shifts.cdf")
    with open(outfn, "w") as fd:
        dumpcdf(xs, ys, fd)

    data = sorted(s.num_shifts / s.bins for s in summaries)
    xs, ys = zip(*buildcdf(make_default_weight_iterator(data)))
    outfn = os.path.join(outdir, "ratio-shifts-to-nbins.cdf")
    with open(outfn, "w") as fd:
        dumpcdf(xs, ys, fd)

    bins_per_day = 86400 / CONFIG['bin_duration_secs']
    data = sorted(s.num_shifts * (bins_per_day/s.bins) for s in summaries)
    xs, ys = zip(*buildcdf(make_default_weight_iterator(data)))
    outfn = os.path.join(outdir, "num-shifts-per-day.cdf")
    with open(outfn, "w") as fd:
        dumpcdf(xs, ys, fd)

    data = sorted((s.num_shifts * (bins_per_day/s.bins), s.total_bytes)
            for s in summaries)
    xs, ys = zip(*buildcdf(make_weighted_iterator(data)))
    outfn = os.path.join(outdir, "num-shifts-per-day-weighted.cdf")
    with open(outfn, "w") as fd:
        dumpcdf(xs, ys, fd)

    data = sorted(s.longest_shifted_bin_streak / s.bins_improv
            for s in summaries if s.bins_improv > 0)
    xs, ys = zip(*buildcdf(make_default_weight_iterator(data)))
    outfn = os.path.join(outdir, "frac-improved-bins-in-longest-streak.cdf")
    with open(outfn, "w") as fd:
        dumpcdf(xs, ys, fd)


def dump_keys(keys, outfn):
    with open(outfn, 'w') as fd:
        for vip_metro, bgp_ip_prefix in keys:
            fd.write("%s %s\n" % (vip_metro, str(bgp_ip_prefix)))


def dumpcdf(xs, ys, fd):
    for x, y in zip(xs, ys):
        fd.write("%f %f\n" % (x, y))


def make_nonsticky(improvfunc, limit):
    def inner_nonsticky(stats):
        return -1 if improvfunc(stats, limit) <= 0 else 1
    return inner_nonsticky

def make_sticky(improvfunc, limit):
    def inner_sticky(stats):
        return improvfunc(stats, limit)
    return inner_sticky


def is_improv_ci_lower_bound(stats, limit):
    if stats.diff_ci[0] > limit:
        return +1
    if stats.diff_ci[1] < limit:
        return -1
    return 0


# CONFIG['min_median_rtt_improvement_ms']:
def is_median_improv(stats, limit):
    if stats.diff_ci[1] > limit:
        return +1
    if stats.diff_ci[1] < limit:
        return -1
    return 0


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

    NAME2FUNC = {
            "nonsticky": make_nonsticky,
            "sticky": make_sticky,
    }
    logging.info("processed %d rows", nrows)

    for name, func in NAME2FUNC.items():
        for limit in [0, 5, 10]:
            basedir = 'minrtt_ci_lower_bound_%d' % limit
            dirname = os.path.join('output', name, basedir)
            tracker.dump_cdfs_minrtt(dirname, func(is_improv_ci_lower_bound, limit))
            basedir = 'minrtt_median_improv_%d' % limit
            dirname = os.path.join('output', name, basedir)
            tracker.dump_cdfs_minrtt(dirname, func(is_median_improv, limit))
        for limit in [0, 0.05, 0.1]:
            basedir = 'hdratio_ci_lower_bound_%.2f' % limit
            dirname = os.path.join('output', name, basedir)
            tracker.dump_cdfs_hdratio(dirname, func(is_improv_ci_lower_bound, limit))
            basedir = 'hdratio_median_improv_%.2f' % limit
            dirname = os.path.join('output', name, basedir)
            tracker.dump_cdfs_hdratio(dirname, func(is_median_improv, limit))


if __name__ == '__main__':
    sys.exit(main())
