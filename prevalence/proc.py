#!/usr/bin/env python3

import bisect
from collections import defaultdict
import csv
import gzip
import json
import logging
import os
import sys

from csvhelp import Row, RowParseError
import csvhelp
from buildcdf import buildcdf


CONFIG = {
    # This is just so we can compute the number of bins between two
    # time stamps:
    "bin_duration_secs": 900,
    # This is the number of bins we require to consider a <POP, prefix>
    # tuple has enough data for prevalence analysis.
    #
    # Update 20190507.1442: Brandon found that most of the traffic is
    # concentrated in prefixes that have enough samples for most bins
    # (expected), so we require that prefixes have 80 (15-min) bins (20h).
    "min_number_of_bins_per_day_rtt": 72,
    # We still require 200 samples for HD-ratio, but allow more bins to be
    # missing to compensate for the reduced number of samples:
    "min_number_of_bins_per_day_hdr": 48,
    # Minimum number of samples required; we do not need this for MinRTT
    # because Brandon only exports if we have enough MinRTT samples.
    "hdratio_min_samples": 200,
    "minrtt_min_samples": 200,
    # For some of the analysis we want to filter for prefixes where
    # "significant" improvement can be achieved.  The absolute number of
    # bins that is considered "significant":
    "significant_improv_min_bins": 8,
    # This is in addition to the lower bound of the median difference
    # confidence interval being larger than 0.
    "deprecated_min_median_rtt_improvement_ms": 5,
    # A *shift period* is defined as a contiguous period of time where the
    # best alternate path is better than the primary.  This period can
    # contain gaps, i.e., bins where we did not have enough samples.  The
    # size of each limited by shift_period_max_measurement_gap_in_bins;
    # gaps with more bins terminate a shift period.
    "deprecated_shift_period_max_measurement_gap_in_bins": 2,
    # This is used to split the time series into days by finding the valley
    # with the least amount of samples.
    "daybreak_num_bins_in_valley": 7200 // 900,
    "classify_continuous_min_frac_improv": 0.8,
    "classify_event_duration": 3600 * 2,
    "classify_min_number_of_days": 2,
    "classify_multiple_days_cnt": 2,
}

global_bytes_acked_sum = 0


def find_lowest_valley_midpoint(seq, itemfunc, valley_width_cnt):
    minidx = -1
    minavg = float("inf")
    nbins = valley_width_cnt
    for i in range(len(seq) - nbins):
        avg = sum(itemfunc(v) for v in seq[i : i + nbins]) / nbins
        if avg < minavg:
            minidx = i
            minavg = avg
    return minidx + nbins // 2


def compute_day_breakpoint_indexes(seq, breakidx, bin_duration):
    breakidxs = list()
    assert ((24 * 3600) % bin_duration) == 0
    bpd = 24 * 3600 // bin_duration  # bins per day
    c = breakidx % bpd
    while c + bpd < len(seq):
        breakidxs.append(c)
        c += bpd
    return breakidxs


class NotEnoughSamplesError(ValueError):
    pass


class SummaryStats:
    def __init__(self):
        self.bins = 0
        self.bins_improv = 0
        self.total_bytes = 0
        self.total_bytes_improv = 0
        self.num_shifts = 0
        self.max_streak = 0
        self.curr_streak = 0

    def update_improv(self, nbins, total_bytes):
        self.bins_improv += nbins
        self.total_bytes_improv += total_bytes * nbins
        self.curr_streak += nbins
        self.max_streak = max(self.max_streak, self.curr_streak)

    def update_totals(self, seq):
        self.bins = len(seq)
        self.total_bytes = sum(s.bytes_acked_sum for s in seq)


class PrevalenceTracker:
    class MinRttStats:
        def __init__(self, row):
            self.bytes_acked_sum = row.bytes_acked_sum
            primary, bestalt = row.get_primary_bestalt_minrtt(CONFIG["minrtt_min_samples"])
            if primary is None:
                raise NotEnoughSamplesError()
            if primary == bestalt:
                raise NotEnoughSamplesError()
            self.pri_type = primary.peer_type
            self.pri_subtype = primary.peer_subtype
            self.alt_type = bestalt.peer_type
            self.alt_subtype = bestalt.peer_subtype
            self.diff_ci = csvhelp.rtt_median_diff_ci(primary, bestalt)

    class HdRatioStats:
        def __init__(self, row):
            self.bytes_acked_sum = row.bytes_acked_sum
            primary, bestalt = row.get_primary_bestalt_hdratio(CONFIG["hdratio_min_samples"])
            if primary is None:
                raise NotEnoughSamplesError()
            if primary == bestalt:
                raise NotEnoughSamplesError()
            self.pri_type = primary.peer_type
            self.pri_subtype = primary.peer_subtype
            self.alt_type = bestalt.peer_type
            self.alt_subtype = bestalt.peer_subtype
            self.diff_ci = csvhelp.hdr_mean_diff_ci(primary, bestalt)

    class Summary:
        def __init__(self, time2stats, improvfunc):
            def get_nbins(tstamp1, tstamp2):
                return (tstamp2 - tstamp1) // CONFIG["bin_duration_secs"]

            def get_nsamples(stats):
                return stats.bytes_acked_sum

            series = sorted(time2stats.items())
            times, stats = zip(*series)

            breakidx = find_lowest_valley_midpoint(stats, get_nsamples, CONFIG["daybreak_num_bins_in_valley"])
            breakidxs = compute_day_breakpoint_indexes(stats, breakidx, CONFIG["bin_duration_secs"])

            gs = SummaryStats()
            gs.bins = len(stats)
            gs.total_bytes = sum(s.bytes_acked_sum for s in stats)
            self.global_stats = gs

            self.day2stats = defaultdict(SummaryStats)

            is_moved = False
            for i, s in enumerate(stats):
                nbins = 1 if i == 0 else get_nbins(times[i - 1], times[i])
                day = bisect.bisect(breakidxs, i)
                ds = self.day2stats[day]

                # We may want to change the code if nbins is large. We
                # currently do nothing special because we only consider
                # prefixes with at least 80 bins (see min_number_of_bins and
                # shift_period_max_measurement_gap_in_bins).

                should_move = improvfunc(s)
                if should_move == -1:
                    gs.curr_streak = 0
                    ds.curr_streak = 0
                    is_moved = False
                elif should_move == 0:
                    if is_moved:
                        gs.update_improv(nbins, s.bytes_acked_sum)
                        ds.update_improv(nbins, s.bytes_acked_sum)
                elif should_move == 1:
                    if not is_moved:
                        gs.num_shifts += 1
                        ds.num_shifts += 1
                        nbins = 1
                    gs.update_improv(nbins, s.bytes_acked_sum)
                    ds.update_improv(nbins, s.bytes_acked_sum)
                    is_moved = True

            # Drop first and last days, they may be incomplete:
            self.day2stats.pop(0, None)
            self.day2stats.pop(len(breakidxs), None)
            for dnum, dstats in self.day2stats.items():
                seq = stats[breakidxs[dnum - 1] : breakidxs[dnum]]
                dstats.update_totals(seq)

        def has_enough_bins_per_day(self, min_bins):
            if not self.day2stats:
                return False
            return min(ds.bins >= min_bins for ds in self.day2stats.values())

        def has_enough_days(self, min_bins, days):
            values = self.day2stats.values()
            return len(list(ds for ds in values if ds.bins > min_bins)) >= days

        def classify(self):
            days_with_improv = len(list(ds for ds in self.day2stats.values() if ds.bins_improv > 0))
            event_bins = CONFIG["classify_event_duration"] // CONFIG["bin_duration_secs"]
            days_with_event = len(list(ds for ds in self.day2stats.values() if ds.max_streak >= event_bins))
            days_without_opp = len(list(ds for ds in self.day2stats.values() if ds.bins - ds.bins_improv > event_bins))
            days = len(self.day2stats)
            data = (
                self.global_stats.bins,
                self.global_stats.bins_improv,
                self.global_stats.total_bytes,
                self.global_stats.total_bytes_improv,
                days,
                days_with_improv,
                days_with_event,
                days_without_opp,
            )
            if self.global_stats.bins_improv >= (self.global_stats.bins * CONFIG["classify_continuous_min_frac_improv"]):
                return "continuous", data
            if days_with_improv == 1 and days_with_event == 1:
                return "one-off", data
            if days_with_improv == days and days_with_event == days and days_without_opp == days:
                return "diurnal", data
            if days_with_event >= CONFIG["classify_multiple_days_cnt"]:
                return "multiday", data
            return "unknown", data

    def __init__(self):
        self.key2time2rttstats = defaultdict(dict)
        self.key2time2hdrstats = defaultdict(dict)

    def update(self, row):
        t = row.time_bucket
        k = row.key()
        try:
            self.key2time2rttstats[k][t] = PrevalenceTracker.MinRttStats(row)
        except NotEnoughSamplesError:
            pass
        try:
            self.key2time2hdrstats[k][t] = PrevalenceTracker.HdRatioStats(row)
        except NotEnoughSamplesError:
            pass

    def dump_cdfs_minrtt(self, outdir, improvfunc):
        key2summary = dict((key, PrevalenceTracker.Summary(t2s, improvfunc)) for key, t2s in self.key2time2rttstats.items())
        # import pdb
        # pdb.set_trace()
        # dump_cdfs_key2sum(outdir, key2summary, CONFIG["min_number_of_bins_per_day_rtt"])
        classify_summaries(outdir, key2summary, CONFIG["min_number_of_bins_per_day_rtt"])

    def dump_cdfs_hdratio(self, outdir, improvfunc):
        key2summary = dict((key, PrevalenceTracker.Summary(t2s, improvfunc)) for key, t2s in self.key2time2hdrstats.items())
        # dump_cdfs_key2sum(outdir, key2summary, CONFIG["min_number_of_bins_per_day_hdr"])
        classify_summaries(outdir, key2summary, CONFIG["min_number_of_bins_per_day_hdr"])


def dump_cdfs_key2sum(outdir, key2sum, bins_per_day):
    summaries = list(key2sum.values())
    summaries = list(s.global_stats for s in summaries if s.has_enough_bins_per_day(bins_per_day))
    out_subdir = os.path.join(outdir, "min_number_of_bins")
    dump_cdfs_sum(out_subdir, summaries)

    summaries_sig = list(s for s in summaries if s.bins_improv >= CONFIG["significant_improv_min_bins"])
    out_subdir = os.path.join(outdir, "significant_improv_min_bins")
    dump_cdfs_sum(out_subdir, summaries_sig)

    total = sum(s.total_bytes for s in summaries)
    ratio = total / global_bytes_acked_sum
    total_sig = sum(s.total_bytes for s in summaries_sig)
    ratio_sig = total_sig / global_bytes_acked_sum
    sys.stdout.write("outdir: %s\n" % outdir)
    sys.stdout.write("total traffic: %d %f\n" % (total, ratio))
    sys.stdout.write("total traffic in prefixes with significant improvement: %d %f\n" % (total_sig, ratio_sig))


class ClassificationStats:
    def __init__(self):
        self.key2bytes = dict()
        self.key2data = dict()
        self.improv_bytes = 0
        self.total_bytes = 0

    def update(self, key, summary, data):
        self.key2data[key] = data
        self.improv_bytes += summary.global_stats.total_bytes_improv
        self.total_bytes += summary.global_stats.total_bytes

    def dump(self, total_bytes, total_valid_bytes, total_improv_bytes, fn):
        with open(fn, "w") as fd:
            fd.write("global_bytes %d\n" % total_bytes)
            f = total_valid_bytes / total_bytes
            fd.write("global_valid_bytes %d %f\n" % (total_valid_bytes, f))
            f = total_improv_bytes / total_bytes
            fd.write("global_improv_bytes %d %f\n" % (total_improv_bytes, f))
            f = self.total_bytes / total_bytes
            fd.write("class_total_bytes %d %f\n" % (self.total_bytes, f))
            f = self.improv_bytes / total_bytes
            fd.write("class_improv_bytes %d %f\n" % (self.improv_bytes, f))
            fd.write("bins bins_improv total_bytes total_bytes_improv days days_with_improv days_with_event days_without_opp\n")
            for key, data in self.key2data.items():
                vip_metro, bgp_ip_prefix = key
                fd.write("%s %s " % (vip_metro, str(bgp_ip_prefix)))
                fd.write("%d %d %d %d %d %d %d %d\n" % data)


def classify_summaries(outdir, key2sum, bins_per_day):
    cls2stats = defaultdict(ClassificationStats)
    total_bytes = 0
    total_valid_bytes = 0
    total_improv_bytes = 0
    for k, s in key2sum.items():
        total_bytes += s.global_stats.total_bytes
        total_improv_bytes += s.global_stats.total_bytes_improv
        # if not s.has_enough_bins_per_day(CONFIG["min_number_of_bins_per_day"]):
        #     continue
        if not s.has_enough_days(bins_per_day, CONFIG["classify_min_number_of_days"]):
            continue
        total_valid_bytes += s.global_stats.total_bytes
        cls, data = s.classify()
        cls2stats[cls].update(k, s, data)

    for cls, cstats in cls2stats.items():
        outfn = os.path.join(outdir, "%s.data" % cls)
        cstats.dump(total_bytes, total_valid_bytes, total_improv_bytes, outfn)


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

    data = sorted((s.total_bytes_improv / s.total_bytes, s.total_bytes) for s in summaries)
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

    bins_per_day = 86400 / CONFIG["bin_duration_secs"]
    data = sorted(s.num_shifts * (bins_per_day / s.bins) for s in summaries)
    xs, ys = zip(*buildcdf(make_default_weight_iterator(data)))
    outfn = os.path.join(outdir, "num-shifts-per-day.cdf")
    with open(outfn, "w") as fd:
        dumpcdf(xs, ys, fd)

    data = sorted((s.num_shifts * (bins_per_day / s.bins), s.total_bytes) for s in summaries)
    xs, ys = zip(*buildcdf(make_weighted_iterator(data)))
    outfn = os.path.join(outdir, "num-shifts-per-day-weighted.cdf")
    with open(outfn, "w") as fd:
        dumpcdf(xs, ys, fd)

    data = sorted(s.max_streak / s.bins_improv for s in summaries if s.bins_improv > 0)
    xs, ys = zip(*buildcdf(make_default_weight_iterator(data)))
    outfn = os.path.join(outdir, "frac-improved-bins-in-longest-streak.cdf")
    with open(outfn, "w") as fd:
        dumpcdf(xs, ys, fd)


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
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s:%(levelname)s: %(message)s")
    logging.info("configuration:")
    logging.info(json.dumps(CONFIG, indent=2))

    tracker = PrevalenceTracker()
    global global_bytes_acked_sum
    global_bytes_acked_sum = 0
    fd = gzip.open(sys.argv[1], "rt")
    reader = csv.DictReader(fd, delimiter="\t")
    nrows = 0
    row_parse_errors_cnt = 0

    for csvrow in reader:
        nrows += 1
        try:
            row = Row(csvrow)
        except RowParseError:
            row_parse_errors_cnt += 1
            continue
        global_bytes_acked_sum += row.bytes_acked_sum  # pylint: disable=E1101
        # if row.key() == ('mrs', ipaddress.ip_network('105.144.0.0/16')):
        #     import pdb
        #     pdb.set_trace()
        tracker.update(row)

    NAME2FUNC = {"nonsticky": make_nonsticky, "sticky": make_sticky}
    logging.info("processed %d rows", nrows)
    logging.info("row_parse_errors %d", row_parse_errors_cnt)

    for name, func in NAME2FUNC.items():
        for limit in [0, 5, 10]:
            basedir = "minrtt_ci_lower_bound_%d" % limit
            dirname = os.path.join("output", name, basedir)
            tracker.dump_cdfs_minrtt(dirname, func(is_improv_ci_lower_bound, limit))
            basedir = "minrtt_median_improv_%d" % limit
            dirname = os.path.join("output", name, basedir)
            tracker.dump_cdfs_minrtt(dirname, func(is_median_improv, limit))
        for limit in [0, 0.05, 0.1]:
            basedir = "hdratio_ci_lower_bound_%.2f" % limit
            dirname = os.path.join("output", name, basedir)
            tracker.dump_cdfs_hdratio(dirname, func(is_improv_ci_lower_bound, limit))
            basedir = "hdratio_median_improv_%.2f" % limit
            dirname = os.path.join("output", name, basedir)
            tracker.dump_cdfs_hdratio(dirname, func(is_median_improv, limit))


if __name__ == "__main__":
    sys.exit(main())
