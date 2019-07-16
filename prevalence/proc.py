#!/usr/bin/env python3

import argparse
import csv
import gzip
import logging
import os
import resource
import sys

# from typing import Any, Mapping

import configs
import csvhelp
import perf

# import temporal

# from buildcdf import buildcdf

# pylint: disable=E1101


# def dump_cdfs_minrtt(self, outdir, improvfunc):
#     key2summary = dict((key, PrevalenceTracker.Summary(t2s, improvfunc)) for key, t2s in self.key2time2rttstats.items())
#     # import pdb
#     # pdb.set_trace()
#     dump_cdfs_key2sum(outdir, key2summary, CONFIG["min_number_of_bins_per_day_rtt"])
#     classify_summaries(outdir, key2summary, CONFIG["min_number_of_bins_per_day_rtt"])

# def dump_cdfs_hdratio(self, outdir, improvfunc):
#     key2summary = dict((key, PrevalenceTracker.Summary(t2s, improvfunc)) for key, t2s in self.key2time2hdrstats.items())
#     dump_cdfs_key2sum(outdir, key2summary, CONFIG["min_number_of_bins_per_day_hdr"])
#     classify_summaries(outdir, key2summary, CONFIG["min_number_of_bins_per_day_hdr"])


# def dump_cdfs_key2sum(outdir, key2sum, bins_per_day):
#     summaries = list(key2sum.values())
#     summaries = list(s.global_stats for s in summaries if s.has_enough_bins_per_day(bins_per_day))
#     out_subdir = os.path.join(outdir, "min_number_of_bins")
#     dump_cdfs_sum(out_subdir, summaries)

#     summaries_sig = list(s for s in summaries if s.bins_improv >= CONFIG["significant_improv_min_bins"])
#     out_subdir = os.path.join(outdir, "significant_improv_min_bins")
#     dump_cdfs_sum(out_subdir, summaries_sig)

#     total = sum(s.total_bytes for s in summaries)
#     ratio = total / global_bytes_acked_sum
#     total_sig = sum(s.total_bytes for s in summaries_sig)
#     ratio_sig = total_sig / global_bytes_acked_sum
#     sys.stdout.write("outdir: %s\n" % outdir)
#     sys.stdout.write("total traffic: %d %f\n" % (total, ratio))
#     sys.stdout.write("total traffic in prefixes with significant improvement: %d %f\n" % (total_sig, ratio_sig))

# class ClassificationStats:
#     def __init__(self):
#         self.key2bytes = dict()
#         self.key2data = dict()
#         self.improv_bytes = 0
#         self.total_bytes = 0

#     def update(self, key, summary, data):
#         self.key2data[key] = data
#         self.improv_bytes += summary.global_stats.total_bytes_improv
#         self.total_bytes += summary.global_stats.total_bytes

#     def dump(self, total_bytes, total_valid_bytes, total_improv_bytes, fn):
#         dirname = os.path.split(fn)[0]
#         os.makedirs(dirname, exist_ok=True)
#         with open(fn, "w") as fd:
#             fd.write("global_bytes %d\n" % total_bytes)
#             f = total_valid_bytes / total_bytes
#             fd.write("global_valid_bytes %d %f\n" % (total_valid_bytes, f))
#             f = total_improv_bytes / total_bytes
#             fd.write("global_improv_bytes %d %f\n" % (total_improv_bytes, f))
#             f = self.total_bytes / total_bytes
#             fd.write("class_total_bytes %d %f\n" % (self.total_bytes, f))
#             f = self.improv_bytes / total_bytes
#             fd.write("class_improv_bytes %d %f\n" % (self.improv_bytes, f))
#             fd.write(
#                 "bins bins_improv total_bytes total_bytes_improv days days_with_improv days_with_event days_without_opp\n"
#             )
#             for key, data in self.key2data.items():
#                 vip_metro, bgp_ip_prefix = key
#                 fd.write("%s %s " % (vip_metro, str(bgp_ip_prefix)))
#                 fd.write("%d %d %d %d %d %d %d %d\n" % data)


# def classify_summaries(outdir, key2sum, bins_per_day):
#     cls2stats = defaultdict(ClassificationStats)
#     total_bytes = 0
#     total_valid_bytes = 0
#     total_improv_bytes = 0
#     for k, s in key2sum.items():
#         total_bytes += s.global_stats.total_bytes
#         total_improv_bytes += s.global_stats.total_bytes_improv
#         # if not s.has_enough_bins_per_day(CONFIG["min_number_of_bins_per_day"]):
#         #     continue
#         if not s.has_enough_days(bins_per_day, CONFIG["classify_min_number_of_days"]):
#             continue
#         total_valid_bytes += s.global_stats.total_bytes
#         cls, data = s.classify()
#         cls2stats[cls].update(k, s, data)

#     for cls, cstats in cls2stats.items():
#         outfn = os.path.join(outdir, "%s.data" % cls)
#         cstats.dump(total_bytes, total_valid_bytes, total_improv_bytes, outfn)


# def make_default_weight_iterator(seq):
#     for x in seq:
#         yield x, 1.0
#     yield None, None


# def make_weighted_iterator(seq):
#     for x, y in seq:
#         yield x, y
#     yield None, None


# def dump_cdfs_sum(outdir, summaries):
#     os.makedirs(outdir, exist_ok=True)

#     data = sorted(s.bins_improv / s.bins for s in summaries)
#     xs, ys = zip(*buildcdf(make_default_weight_iterator(data)))
#     outfn = os.path.join(outdir, "frac-improved-bins.cdf")
#     with open(outfn, "w") as fd:
#         dumpcdf(xs, ys, fd)

#     data = sorted((s.bins_improv / s.bins, s.total_bytes) for s in summaries)
#     xs, ys = zip(*buildcdf(make_weighted_iterator(data)))
#     outfn = os.path.join(outdir, "frac-improved-bins-weighted.cdf")
#     with open(outfn, "w") as fd:
#         dumpcdf(xs, ys, fd)

#     data = sorted(s.total_bytes_improv / s.total_bytes for s in summaries)
#     xs, ys = zip(*buildcdf(make_default_weight_iterator(data)))
#     outfn = os.path.join(outdir, "frac-improved-bytes.cdf")
#     with open(outfn, "w") as fd:
#         dumpcdf(xs, ys, fd)

#     data = sorted((s.total_bytes_improv / s.total_bytes, s.total_bytes) for s in summaries)
#     xs, ys = zip(*buildcdf(make_weighted_iterator(data)))
#     outfn = os.path.join(outdir, "frac-improved-bytes-weighted.cdf")
#     with open(outfn, "w") as fd:
#         dumpcdf(xs, ys, fd)

#     data = sorted(s.num_shifts for s in summaries)
#     xs, ys = zip(*buildcdf(make_default_weight_iterator(data)))
#     outfn = os.path.join(outdir, "num-shifts.cdf")
#     with open(outfn, "w") as fd:
#         dumpcdf(xs, ys, fd)

#     data = sorted(s.num_shifts / s.bins for s in summaries)
#     xs, ys = zip(*buildcdf(make_default_weight_iterator(data)))
#     outfn = os.path.join(outdir, "ratio-shifts-to-nbins.cdf")
#     with open(outfn, "w") as fd:
#         dumpcdf(xs, ys, fd)

#     bins_per_day = 86400 / CONFIG["bin_duration_secs"]
#     data = sorted(s.num_shifts * (bins_per_day / s.bins) for s in summaries)
#     xs, ys = zip(*buildcdf(make_default_weight_iterator(data)))
#     outfn = os.path.join(outdir, "num-shifts-per-day.cdf")
#     with open(outfn, "w") as fd:
#         dumpcdf(xs, ys, fd)

#     data = sorted((s.num_shifts * (bins_per_day / s.bins), s.total_bytes) for s in summaries)
#     xs, ys = zip(*buildcdf(make_weighted_iterator(data)))
#     outfn = os.path.join(outdir, "num-shifts-per-day-weighted.cdf")
#     with open(outfn, "w") as fd:
#         dumpcdf(xs, ys, fd)

#     data = sorted(s.max_streak / s.bins_improv for s in summaries if s.bins_improv > 0)
#     xs, ys = zip(*buildcdf(make_default_weight_iterator(data)))
#     outfn = os.path.join(outdir, "frac-improved-bins-in-longest-streak.cdf")
#     with open(outfn, "w") as fd:
#         dumpcdf(xs, ys, fd)

# def dumpcdf(xs, ys, fd):
#     for x, y in zip(xs, ys):
#         fd.write("%f %f\n" % (x, y))


def create_parser():
    desc = """Process FB performance data"""
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument(
        "--csv",
        dest="inputfn",
        action="store",
        metavar="FILE",
        type=str,
        required=True,
        help="File containing a DB with route dumps",
    )
    return parser


def main():
    resource.setrlimit(resource.RLIMIT_AS, (1 << 34, 1 << 34))
    resource.setrlimit(resource.RLIMIT_FSIZE, (1 << 35, 1 << 35))
    logging.basicConfig(level=logging.DEBUG, filename="log.txt", format="%(message)s")

    parser = create_parser()
    opts = parser.parse_args()

    config = configs.Config(configs.CONFIGS[0])

    fd = gzip.open(opts.inputfn, "rt")
    reader = csv.DictReader(fd, delimiter="\t")
    db = csvhelp.DB(reader, config.routeinfo_validator)
    fd.close()
    logging.info("loaded DB (%d rows, %d errors)", db.counters["rows"], db.counters["parse_errors"])

    while True:
        sys.stdin.readline()

    # perfdb = perf.PerfDB(db, config)

    # NAME2FUNC = {"nonsticky": make_nonsticky, "sticky": make_sticky}
    # for name, func in NAME2FUNC.items():
    # for limit in opts.rtt_thresholds:
    #     basedir = "minrtt_ci_lower_bound_%d" % limit
    #     dirname = os.path.join("output", name, basedir)
    #     tracker.dump_cdfs_minrtt(dirname, func(is_improv_ci_lower_bound, limit))
    #     basedir = "minrtt_median_improv_%d" % limit
    #     dirname = os.path.join("output", name, basedir)
    #     tracker.dump_cdfs_minrtt(dirname, func(is_median_improv, limit))
    # for limit in opts.hdratio_thresholds:
    #     basedir = "hdratio_ci_lower_bound_%.2f" % limit
    #     dirname = os.path.join("output", name, basedir)
    #     tracker.dump_cdfs_hdratio(dirname, func(is_improv_ci_lower_bound, limit))
    #     basedir = "hdratio_median_improv_%.2f" % limit
    #     dirname = os.path.join("output", name, basedir)
    #     tracker.dump_cdfs_hdratio(dirname, func(is_median_improv, limit))


if __name__ == "__main__":
    sys.exit(main())


# def find_lowest_valley_midpoint(seq, itemfunc, valley_width_cnt):
#     minidx = -1
#     minavg = float("inf")
#     nbins = valley_width_cnt
#     for i in range(len(seq) - nbins):
#         avg = sum(itemfunc(v) for v in seq[i : i + nbins]) / nbins
#         if avg < minavg:
#             minidx = i
#             minavg = avg
#     return minidx + nbins // 2

# def compute_day_breakpoint_indexes(seq, breakidx, bin_duration):
#     breakidxs = list()
#     assert ((24 * 3600) % bin_duration) == 0
#     bpd = 24 * 3600 // bin_duration  # bins per day
#     c = breakidx % bpd
#     while c + bpd < len(seq):
#         breakidxs.append(c)
#         c += bpd
#     return breakidxs

# def apply_filters(self):
#     # Filters sorted by (guessed) complexity:
#     # self.__drop_keys_with_insufficient_bins_per_day_or_gaps()
#     # self.__drop_keys_with_path_changes()
#     pass

# def has_enough_bins_per_day(self, min_bins):
#     if not self.day2stats:
#         return False
#     return min(ds.bins >= min_bins for ds in self.day2stats.values())

# def has_enough_days(self, min_bins, days):
#     values = self.day2stats.values()
#     return len(list(ds for ds in values if ds.bins > min_bins)) >= days


# class SummaryStats:
#     def __init__(self):
#         self.bins = 0
#         self.bins_improv = 0
#         self.total_bytes = 0
#         self.total_bytes_improv = 0
#         self.num_shifts = 0
#         self.max_streak = 0
#         self.curr_streak = 0

#     def update_improv(self, nbins, total_bytes):
#         self.bins_improv += nbins
#         self.total_bytes_improv += total_bytes * nbins
#         self.curr_streak += nbins
#         self.max_streak = max(self.max_streak, self.curr_streak)

#     def update_totals(self, seq):
#         self.bins = len(seq)
#         self.total_bytes = sum(s.bytes_acked_sum for s in seq)
