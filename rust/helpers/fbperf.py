import bisect
from collections import Counter, namedtuple
from itertools import cycle
import logging
import pathlib
import pickle
import re
from typing import List, Mapping, Tuple

import matplotlib.pyplot as plt
import numpy as np
import matplotlib
matplotlib.rcParams['text.usetex'] = True

# These headers match from `imc2019/0916`, and have since been updated.
# We keep the old version here because they still match code in
# `make-test-csv` and is compatible with `perfstats`
HEADERS = [
    "time_bucket",
    "vip_metro",
    "bgp_ip_prefix",
    "bgp_ip_prefix_len",
    "client_is_ipv6",
    "client_continent",
    "client_country",
    "conn_speed_majority",
    "conn_type_from_liger",
    "conn_type_from_liger_score",
    "bytes_acked",
    "apm_route_num_1_multiple_paths",
    "apm_route_num_2_multiple_paths",
    "apm_route_num_3_multiple_paths",
    "apm_route_num_1_changed",
    "apm_route_num_2_changed",
    "apm_route_num_3_changed",
    "num_pivots",
    "r0_num_samples",
    "r0_num_samples_with_hdratio",
    "r0_apm_route_num",
    "r0_peer_type",
    "r0_peer_subtype",
    "r0_bgp_as_path_len",
    "r0_bgp_as_path_strings",
    "r0_bgp_as_path_min_len_prepending_removed",
    "r0_bgp_as_path_prepending",
    "r0_px_nexthops",
    "r0_minrtt_ms_p25",
    "r0_minrtt_ms_p25_ci_lb",
    "r0_minrtt_ms_p25_ci_ub",
    "r0_minrtt_ms_p50",
    "r0_minrtt_ms_p50_ci_lb",
    "r0_minrtt_ms_p50_ci_ub",
    "r0_minrtt_ms_p50_var",
    "r0_hdratio_p50",
    "r0_hdratio_p50_ci_lb",
    "r0_hdratio_p50_ci_ub",
    "r0_hdratio_p50_var",
    "r0_hdratio_avg",
    "r0_hdratio_avg_bootstrapped",
    "r0_hdratio_avg_bootstrapped_ci_lb",
    "r0_hdratio_avg_bootstrapped_ci_ub",
    "r0_hdratio_normal_var",
    "r1_num_samples",
    "r1_num_samples_with_hdratio",
    "r1_apm_route_num",
    "r1_peer_type",
    "r1_peer_subtype",
    "r1_bgp_as_path_len",
    "r1_bgp_as_path_strings",
    "r1_bgp_as_path_min_len_prepending_removed",
    "r1_bgp_as_path_prepending",
    "r1_px_nexthops",
    "r1_minrtt_ms_p25",
    "r1_minrtt_ms_p25_ci_lb",
    "r1_minrtt_ms_p25_ci_ub",
    "r1_minrtt_ms_p50",
    "r1_minrtt_ms_p50_ci_lb",
    "r1_minrtt_ms_p50_ci_ub",
    "r1_minrtt_ms_p50_var",
    "r1_hdratio_p50",
    "r1_hdratio_p50_ci_lb",
    "r1_hdratio_p50_ci_ub",
    "r1_hdratio_p50_var",
    "r1_hdratio_avg",
    "r1_hdratio_avg_bootstrapped",
    "r1_hdratio_avg_bootstrapped_ci_lb",
    "r1_hdratio_avg_bootstrapped_ci_ub",
    "r1_hdratio_normal_var",
    "r2_num_samples",
    "r2_num_samples_with_hdratio",
    "r2_apm_route_num",
    "r2_peer_type",
    "r2_peer_subtype",
    "r2_bgp_as_path_len",
    "r2_bgp_as_path_strings",
    "r2_bgp_as_path_min_len_prepending_removed",
    "r2_bgp_as_path_prepending",
    "r2_px_nexthops",
    "r2_minrtt_ms_p25",
    "r2_minrtt_ms_p25_ci_lb",
    "r2_minrtt_ms_p25_ci_ub",
    "r2_minrtt_ms_p50",
    "r2_minrtt_ms_p50_ci_lb",
    "r2_minrtt_ms_p50_ci_ub",
    "r2_minrtt_ms_p50_var",
    "r2_hdratio_p50",
    "r2_hdratio_p50_ci_lb",
    "r2_hdratio_p50_ci_ub",
    "r2_hdratio_p50_var",
    "r2_hdratio_avg",
    "r2_hdratio_avg_bootstrapped",
    "r2_hdratio_avg_bootstrapped_ci_lb",
    "r2_hdratio_avg_bootstrapped_ci_ub",
    "r2_hdratio_normal_var",
    "r3_num_samples",
    "r3_num_samples_with_hdratio",
    "r3_apm_route_num",
    "r3_peer_type",
    "r3_peer_subtype",
    "r3_bgp_as_path_len",
    "r3_bgp_as_path_strings",
    "r3_bgp_as_path_min_len_prepending_removed",
    "r3_bgp_as_path_prepending",
    "r3_px_nexthops",
    "r3_minrtt_ms_p25",
    "r3_minrtt_ms_p25_ci_lb",
    "r3_minrtt_ms_p25_ci_ub",
    "r3_minrtt_ms_p50",
    "r3_minrtt_ms_p50_ci_lb",
    "r3_minrtt_ms_p50_ci_ub",
    "r3_minrtt_ms_p50_var",
    "r3_hdratio_p50",
    "r3_hdratio_p50_ci_lb",
    "r3_hdratio_p50_ci_ub",
    "r3_hdratio_p50_var",
    "r3_hdratio_avg",
    "r3_hdratio_avg_bootstrapped",
    "r3_hdratio_avg_bootstrapped_ci_lb",
    "r3_hdratio_avg_bootstrapped_ci_ub",
    "r3_hdratio_normal_var",
    "r4_num_samples",
    "r4_num_samples_with_hdratio",
    "r4_apm_route_num",
    "r4_peer_type",
    "r4_peer_subtype",
    "r4_bgp_as_path_len",
    "r4_bgp_as_path_strings",
    "r4_bgp_as_path_min_len_prepending_removed",
    "r4_bgp_as_path_prepending",
    "r4_px_nexthops",
    "r4_minrtt_ms_p25",
    "r4_minrtt_ms_p25_ci_lb",
    "r4_minrtt_ms_p25_ci_ub",
    "r4_minrtt_ms_p50",
    "r4_minrtt_ms_p50_ci_lb",
    "r4_minrtt_ms_p50_ci_ub",
    "r4_minrtt_ms_p50_var",
    "r4_hdratio_p50",
    "r4_hdratio_p50_ci_lb",
    "r4_hdratio_p50_ci_ub",
    "r4_hdratio_p50_var",
    "r4_hdratio_avg",
    "r4_hdratio_avg_bootstrapped",
    "r4_hdratio_avg_bootstrapped_ci_lb",
    "r4_hdratio_avg_bootstrapped_ci_ub",
    "r4_hdratio_normal_var",
    "r5_num_samples",
    "r5_num_samples_with_hdratio",
    "r5_apm_route_num",
    "r5_peer_type",
    "r5_peer_subtype",
    "r5_bgp_as_path_len",
    "r5_bgp_as_path_strings",
    "r5_bgp_as_path_min_len_prepending_removed",
    "r5_bgp_as_path_prepending",
    "r5_px_nexthops",
    "r5_minrtt_ms_p25",
    "r5_minrtt_ms_p25_ci_lb",
    "r5_minrtt_ms_p25_ci_ub",
    "r5_minrtt_ms_p50",
    "r5_minrtt_ms_p50_ci_lb",
    "r5_minrtt_ms_p50_ci_ub",
    "r5_minrtt_ms_p50_var",
    "r5_hdratio_p50",
    "r5_hdratio_p50_ci_lb",
    "r5_hdratio_p50_ci_ub",
    "r5_hdratio_p50_var",
    "r5_hdratio_avg",
    "r5_hdratio_avg_bootstrapped",
    "r5_hdratio_avg_bootstrapped_ci_lb",
    "r5_hdratio_avg_bootstrapped_ci_ub",
    "r5_hdratio_normal_var",
    "r6_num_samples",
    "r6_num_samples_with_hdratio",
    "r6_apm_route_num",
    "r6_peer_type",
    "r6_peer_subtype",
    "r6_bgp_as_path_len",
    "r6_bgp_as_path_strings",
    "r6_bgp_as_path_min_len_prepending_removed",
    "r6_bgp_as_path_prepending",
    "r6_px_nexthops",
    "r6_minrtt_ms_p25",
    "r6_minrtt_ms_p25_ci_lb",
    "r6_minrtt_ms_p25_ci_ub",
    "r6_minrtt_ms_p50",
    "r6_minrtt_ms_p50_ci_lb",
    "r6_minrtt_ms_p50_ci_ub",
    "r6_minrtt_ms_p50_var",
    "r6_hdratio_p50",
    "r6_hdratio_p50_ci_lb",
    "r6_hdratio_p50_ci_ub",
    "r6_hdratio_p50_var",
    "r6_hdratio_avg",
    "r6_hdratio_avg_bootstrapped",
    "r6_hdratio_avg_bootstrapped_ci_lb",
    "r6_hdratio_avg_bootstrapped_ci_ub",
    "r6_hdratio_normal_var",
    "r7_num_samples",
    "r7_num_samples_with_hdratio",
    "r7_apm_route_num",
    "r7_peer_type",
    "r7_peer_subtype",
    "r7_bgp_as_path_len",
    "r7_bgp_as_path_strings",
    "r7_bgp_as_path_min_len_prepending_removed",
    "r7_bgp_as_path_prepending",
    "r7_px_nexthops",
    "r7_minrtt_ms_p25",
    "r7_minrtt_ms_p25_ci_lb",
    "r7_minrtt_ms_p25_ci_ub",
    "r7_minrtt_ms_p50",
    "r7_minrtt_ms_p50_ci_lb",
    "r7_minrtt_ms_p50_ci_ub",
    "r7_minrtt_ms_p50_var",
    "r7_hdratio_p50",
    "r7_hdratio_p50_ci_lb",
    "r7_hdratio_p50_ci_ub",
    "r7_hdratio_p50_var",
    "r7_hdratio_avg",
    "r7_hdratio_avg_bootstrapped",
    "r7_hdratio_avg_bootstrapped_ci_lb",
    "r7_hdratio_avg_bootstrapped_ci_ub",
    "r7_hdratio_normal_var",
    "r1_r0_diff_minrtt_ms_p50",
    "r1_r0_diff_minrtt_ms_p50_ci_ub",
    "r1_r0_diff_minrtt_ms_p50_ci_lb",
    "r1_r0_diff_hdratio_p50",
    "r1_r0_diff_hdratio_p50_ci_ub",
    "r1_r0_diff_hdratio_p50_ci_lb",
    "r2_r0_diff_minrtt_ms_p50",
    "r2_r0_diff_minrtt_ms_p50_ci_ub",
    "r2_r0_diff_minrtt_ms_p50_ci_lb",
    "r2_r0_diff_hdratio_p50",
    "r2_r0_diff_hdratio_p50_ci_ub",
    "r2_r0_diff_hdratio_p50_ci_lb",
    "r3_r0_diff_minrtt_ms_p50",
    "r3_r0_diff_minrtt_ms_p50_ci_ub",
    "r3_r0_diff_minrtt_ms_p50_ci_lb",
    "r3_r0_diff_hdratio_p50",
    "r3_r0_diff_hdratio_p50_ci_ub",
    "r3_r0_diff_hdratio_p50_ci_lb",
    "r4_r0_diff_minrtt_ms_p50",
    "r4_r0_diff_minrtt_ms_p50_ci_ub",
    "r4_r0_diff_minrtt_ms_p50_ci_lb",
    "r4_r0_diff_hdratio_p50",
    "r4_r0_diff_hdratio_p50_ci_ub",
    "r4_r0_diff_hdratio_p50_ci_lb",
    "r5_r0_diff_minrtt_ms_p50",
    "r5_r0_diff_minrtt_ms_p50_ci_ub",
    "r5_r0_diff_minrtt_ms_p50_ci_lb",
    "r5_r0_diff_hdratio_p50",
    "r5_r0_diff_hdratio_p50_ci_ub",
    "r5_r0_diff_hdratio_p50_ci_lb",
    "r6_r0_diff_minrtt_ms_p50",
    "r6_r0_diff_minrtt_ms_p50_ci_ub",
    "r6_r0_diff_minrtt_ms_p50_ci_lb",
    "r6_r0_diff_hdratio_p50",
    "r6_r0_diff_hdratio_p50_ci_ub",
    "r6_r0_diff_hdratio_p50_ci_lb",
    "r7_r0_diff_minrtt_ms_p50",
    "r7_r0_diff_minrtt_ms_p50_ci_ub",
    "r7_r0_diff_minrtt_ms_p50_ci_lb",
    "r7_r0_diff_hdratio_p50",
    "r7_r0_diff_hdratio_p50_ci_ub",
    "r7_r0_diff_hdratio_p50_ci_lb",
    "r1_r0_diff_hdratio_avg_bootstrapped_ci_lb",
    "r1_r0_diff_hdratio_avg_bootstrapped_ci_ub",
    "r2_r0_diff_hdratio_avg_bootstrapped_ci_lb",
    "r2_r0_diff_hdratio_avg_bootstrapped_ci_ub",
    "r3_r0_diff_hdratio_avg_bootstrapped_ci_lb",
    "r3_r0_diff_hdratio_avg_bootstrapped_ci_ub",
    "r4_r0_diff_hdratio_avg_bootstrapped_ci_lb",
    "r4_r0_diff_hdratio_avg_bootstrapped_ci_ub",
    "r5_r0_diff_hdratio_avg_bootstrapped_ci_lb",
    "r5_r0_diff_hdratio_avg_bootstrapped_ci_ub",
    "r6_r0_diff_hdratio_avg_bootstrapped_ci_lb",
    "r6_r0_diff_hdratio_avg_bootstrapped_ci_ub",
    "r7_r0_diff_hdratio_avg_bootstrapped_ci_lb",
    "r7_r0_diff_hdratio_avg_bootstrapped_ci_ub",
]


class TemporalConfig:
    def __init__(
        self,
        bin_duration_secs=900,
        min_days=2,
        min_frac_existing_bins=0.6,
        min_frac_bins_with_alternate=0.6,
        min_frac_valid_bins=0.6,
        continuous_min_frac_shifted_bins=0.75,
        diurnal_min_bad_bins=4,
        diurnal_bad_bin_min_prob_shift=0.5,
        uneventful_max_frac_shifted_bins=0.0,
    ):
        self.bin_duration_secs = bin_duration_secs
        self.min_days = min_days
        self.min_frac_existing_bins = min_frac_existing_bins
        self.min_frac_bins_with_alternate = min_frac_bins_with_alternate
        self.min_frac_valid_bins = min_frac_valid_bins
        self.continuous_min_frac_shifted_bins = continuous_min_frac_shifted_bins
        self.diurnal_min_bad_bins = diurnal_min_bad_bins
        self.diurnal_bad_bin_min_prob_shift = diurnal_bad_bin_min_prob_shift
        self.uneventful_max_frac_shifted_bins = uneventful_max_frac_shifted_bins

    def __str__(self):
        return "tempconfig--bin-{}--days-{}--fracExisting-{:0.2f}--fracWithAlternate-{:0.2f}--fracValid-{:0.2f}--cont-{:0.2f}--minBadBins-{}--badBinPrev-{:0.2f}--uneventful-{:0.2f}".format(
            self.bin_duration_secs,
            self.min_days,
            self.min_frac_existing_bins,
            self.min_frac_bins_with_alternate,
            self.min_frac_valid_bins,
            self.continuous_min_frac_shifted_bins,
            self.diurnal_min_bad_bins,
            self.diurnal_bad_bin_min_prob_shift,
            self.uneventful_max_frac_shifted_bins,
        )

    @staticmethod
    def parse(string):
        regex_string = r"tempconfig--bin-(\d+)--days-(\d+)--fracExisting-([0-9.]+)--fracWithAlternate-([0-9.]+)--fracValid-([0-9.]+)--cont-([0-9.]+)--minBadBins-([0-9.]+)--badBinPrev-([0-9.]+)--uneventful-([0-9.]+)"
        m = re.match(regex_string, string)
        assert m is not None, f"string={string}"
        bin_duration_secs = int(m.group(1))
        min_days = int(m.group(2))
        min_frac_existing_bins = float(m.group(3))
        min_frac_bins_with_alternate = float(m.group(4))
        min_frac_valid_bins = float(m.group(5))
        continuous_min_frac_shifted_bins = float(m.group(6))
        diurnal_min_bad_bins = int(m.group(7))
        diurnal_bad_bin_min_prob_shift = float(m.group(8))
        uneventful_max_frac_shifted_bins = float(m.group(9))
        return TemporalConfig(
            bin_duration_secs,
            min_days,
            min_frac_existing_bins,
            min_frac_bins_with_alternate,
            min_frac_valid_bins,
            continuous_min_frac_shifted_bins,
            diurnal_min_bad_bins,
            diurnal_bad_bin_min_prob_shift,
            uneventful_max_frac_shifted_bins,
        )


class Summarizer:
    DEFAULT_DEG_BASE_CI = {
        "minrtt50": {"deg": 25.0, "opp": 0.0, "relationships": 0.0},
        "hdratio50": {"deg": 0.2, "opp": 0.0, "relationships": 0.0},
        "hdratioboot": {"deg": 0.2, "opp": 0.0, "relationships": 0.0},
    }
    RELATIONSHIP_PAIR_NAMES = {
        (7, 8): "Peering vs Transit",
        (8, 8): "Transit vs Transit",
    }

    def __init__(
        self,
        metric,
        summary,
        lb,
        diff_thresh,
        diff_ci,
        base_ci,
        pri_relationship,
        alt_relationship,
        hdratio_diff_ci,
    ):
        self.metric = str(metric)
        self.summary = str(summary)
        self.lb = bool(lb)
        self.diff_thresh = float(diff_thresh)
        self.diff_ci = float(diff_ci)
        # Only for DegradationSummarizer:
        self.base_ci = float(base_ci)
        # Only for RelationshipSummarizer:
        self.pri_relationship = int(pri_relationship)
        self.alt_relationship = int(alt_relationship)
        # Only for MinRtt50ImprovementSummarizer:
        self.hdratio_diff_ci = float(hdratio_diff_ci)

    def __hash__(self):
        return hash(
            (
                self.metric,
                self.summary,
                self.lb,
                self.diff_thresh,
                self.diff_ci,
                self.base_ci,
                self.pri_relationship,
                self.alt_relationship,
                self.hdratio_diff_ci,
            )
        )

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __str__(self):
        lb = "true" if self.lb else "false"
        if self.summary == "deg":
            return f"{self.metric}--{self.summary}--bound-{lb}--diff-thresh-{self.diff_thresh:0.2f}--diff-ci-{self.diff_ci:0.2f}--base-ci-{self.base_ci:0.2f}"
        if self.summary == "opp":
            return f"{self.metric}--{self.summary}--bound-{lb}--diff-thresh-{self.diff_thresh:0.2f}--diff-ci-{self.diff_ci:0.2f}"
        if self.summary == "relationships":
            return f"{self.metric}--{self.summary}-{self.pri_relationship}-{self.alt_relationship}--bound-{lb}--diff-thresh-{self.diff_thresh:0.2f}--diff-ci-{self.diff_ci:0.2f}"

    @staticmethod
    def parse(string):
        deg_string = r"^(.+)--deg--bound-(.+)--diff-thresh-([0-9.]+)--diff-ci-([0-9.]+)--base-ci-([0-9.]+)$"
        opp_string = r"^(.+)--opp--bound-(.+)--diff-thresh-([0-9.]+)--diff-ci-([0-9.]+)(?-([0-9.]+))?$"
        rel_string = r"^(.+)--relationships-(.+)-(.+)--bound-(.+)--diff-thresh-([0-9.]+)--diff-ci-([0-9.]+)$"
        summary = "deg"
        m = re.match(deg_string, string)
        if m is not None:
            metric = m.group(1)
            lb = m.group(2) == "true"
            diff_thresh = float(m.group(3))
            diff_ci = float(m.group(4))
            base_ci = float(m.group(5))
            pri = 0
            alt = 0
            hdratio_diff_ci = 0.0
            return Summarizer(
                metric,
                summary,
                lb,
                diff_thresh,
                diff_ci,
                base_ci,
                pri,
                alt,
                hdratio_diff_ci,
            )
        summary = "opp"
        m = re.match(opp_string, string)
        if m is not None:
            metric = m.group(1)
            lb = m.group(2) == "true"
            diff_thresh = float(m.group(3))
            diff_ci = float(m.group(4))
            base_ci = 0.0
            pri = 0
            alt = 0
            hdratio_diff_ci = float(m.group(5)) if metric == "minrtt50" else 0.0
            return Summarizer(
                metric,
                summary,
                lb,
                diff_thresh,
                diff_ci,
                base_ci,
                pri,
                alt,
                hdratio_diff_ci,
            )
        summary = "relationships"
        m = re.match(rel_string, string)
        if m is not None:
            metric = m.group(1)
            pri = int(m.group(2))
            alt = int(m.group(3))
            lb = m.group(4) == "true"
            diff_thresh = float(m.group(5))
            diff_ci = float(m.group(6))
            base_ci = 0.0
            hdratio_diff_ci = 0.0
            return Summarizer(
                metric,
                summary,
                lb,
                diff_thresh,
                diff_ci,
                base_ci,
                pri,
                alt,
                hdratio_diff_ci,
            )
        raise RuntimeError(f"could not parse Summarizer spec {string}")

    @staticmethod
    def get_metric(sumstr):
        m = re.match(r"^([^-]+)--", sumstr)
        assert m, sumstr
        return m.group(1)

    @staticmethod
    def get_summary(sumstr):
        m = re.match(r"^[^-]+--([^-]+)-", sumstr)
        assert m, sumstr
        return m.group(1)

    @staticmethod
    def filter_sumstr(sumstr_iterable, metric, summary, thresh, diffci):
        for sumstr in sumstr_iterable:
            m = re.search(f"^{metric}--", sumstr)
            if not m:
                continue
            m = re.search(f"--{summary}--", sumstr)
            if not m:
                continue
            m = re.search(f"diff-thresh-{thresh:.2f}", sumstr)
            if not m:
                continue
            m = re.search(f"diff-ci-{diffci:.2f}", sumstr)
            if not m:
                continue
            return sumstr
        raise RuntimeError(
            f"could not find sumstr: {metric} {summary} diff-thresh-{thresh:.2f} diff-ci-{diffci:.2f}"
        )

    @staticmethod
    def make_key(
        metric,
        summary,
        diff_thresh,
        diff_ci,
        pri_relationship=0,
        alt_relationship=0,
        hdratio_diff_ci=0.1,
    ):
        return Summarizer(
            metric,
            summary,
            True,
            diff_thresh,
            diff_ci,
            Summarizer.DEFAULT_DEG_BASE_CI[metric][summary],
            pri_relationship,
            alt_relationship,
            hdratio_diff_ci,
        )


DataTuple = namedtuple(
    "DataTuple",
    ["shifted", "valid", "total", "frac_shifted", "frac_valid", "frac_total"],
)


class RunData:
    def __init__(self, basedir):
        with open(basedir / "temporal-behavior.pickle", "rb") as fd:
            name2data = pickle.load(fd)
        assert isinstance(name2data, dict)
        convert_bigints = lambda t: DataTuple(
            int(t[0]), int(t[1]), int(t[2]), t[3], t[4], t[5]
        )
        self.name2data = {n: convert_bigints(t) for n, t in name2data.items()}

    def get_continent_valid(self, continent):
        name = lambda pattern: f"{pattern}+{continent}"
        return sum(self.name2data[name(p)].valid for p in TemporalPattern.VALID)

    def get_valid(self):
        return sum(self.name2data[p].valid for p in TemporalPattern.VALID)


class TemporalPattern:
    ALL = [
        "MissingBins",
        "NoRoute",
        "Undersampled",
        "Uneventful",
        "Continuous",
        "Diurnal",
        "Episodic",
    ]
    VALID = ["Uneventful", "Continuous", "Diurnal", "Episodic"]
    WITH_SHIFTS = ["Continuous", "Diurnal", "Episodic"]


CLIENT_CONTINENTS = ["AF", "AS", "EU", "NA", "OC", "SA"]


class RelationshipData:
    PEER_TYPES = [
        "Private",
        "Public",
        "Private",  # "PeeringPaid", we consider Paid == Private
        "Transit",
        "Uninitialized",
    ]

    def __init__(self, basedir):
        self.prialt2counters = dict()
        self.total_bytes = 0
        with open(basedir / "opp-vs-relationship.pickle", "rb") as fd:
            prialt2counters = pickle.load(fd)
        for prialt, counters in prialt2counters.items():
            pri, alt = prialt
            pri, alt = (
                RelationshipData.PEER_TYPES[pri],
                RelationshipData.PEER_TYPES[alt],
            )
            cdata = self.prialt2counters.setdefault(
                (pri, alt), Counter(total=0, longer=0, prepended_more=0)
            )
            cdata["total"] += int(counters[0])
            cdata["longer"] += int(counters[1])
            cdata["prepended_more"] += int(counters[2])
            self.total_bytes += int(counters[0])


class Plots:
    METRIC_SUMMARY_XLABEL = {
        "minrtt50": {
            "opp": "$\\mathrm{MinRTT}_\\mathrm{P50}$ Difference [Preferred $-$ Alternate]",
            "deg": "$\\mathrm{MinRTT}_\\mathrm{P50}$ Degradation [Current $-$ Baseline]",
            # "relationships": "$\\mathrm{MinRTT}_\\mathrm{P50}$ Difference",
            "relationships": "Median Minimum RTT Difference [ms]",
        },
        "hdratio50": {
            "opp": "$\\mathrm{HDratio}_\\mathrm{P50}$ Difference [Alternate $-$ Preferred]",
            "deg": "$\\mathrm{HDratio}_\\mathrm{P50}$ Degradation [Baseline $-$ Current]",
            "relationships": "$\\mathrm{HDratio}_\\mathrm{P50}$ Difference",
        },
        "hdratioboot": {
            "opp": "$\\mathrm{HDratio}_\\mathrm{P50}$ Difference [Alternate $-$ Preferred]",
            "deg": "$\\mathrm{HDratio}_\\mathrm{P50}$ Degradation [Baseline $-$ Current]",
            "relationships": "$\\mathrm{HDratio}_\\mathrm{P50}$ Difference",
        },
    }
    METRIC_SUMMARY_XLIM = {
        "minrtt50": {"opp": (-10, 10), "deg": (0, 20), "relationships": (-10, 10)},
        "hdratio50": {
            "opp": (-0.2, 0.2),
            "deg": (0, 0.4),
            "relationships": (-0.2, 0.2),
        },
        "hdratioboot": {
            "opp": (-0.2, 0.2),
            "deg": (0, 0.4),
            "relationships": (-0.2, 0.2),
        },
    }
    METRIC_SUMMARY_LABELS = {
        "minrtt50": {
            "opp": [
                (
                    (METRIC_SUMMARY_XLIM["minrtt50"]["opp"][0] + 1, 0.25),
                    "left",
                    "BGP\nis better",
                ),
                (
                    (METRIC_SUMMARY_XLIM["minrtt50"]["opp"][1] - 1, 0.75),
                    "right",
                    "Alternate\nis better",
                ),
            ],
            "deg": [],
            "relationships": [
                (
                    (METRIC_SUMMARY_XLIM["minrtt50"]["opp"][0] + 1, 0.25),
                    "left",
                    "Preferred\nis better",
                ),
                (
                    (METRIC_SUMMARY_XLIM["minrtt50"]["opp"][1] - 1, 0.75),
                    "right",
                    "Alternate\nis better",
                ),
            ],
        },
        "hdratio50": {
            "opp": [
                (
                    (METRIC_SUMMARY_XLIM["hdratio50"]["opp"][0] + 0.01, 0.25),
                    "left",
                    "Preferred\nis better",
                ),
                (
                    (METRIC_SUMMARY_XLIM["hdratio50"]["opp"][1] - 0.01, 0.75),
                    "right",
                    "Alternate\nis better",
                ),
            ],
            "deg": [],
            "relationships": [
                (
                    (METRIC_SUMMARY_XLIM["hdratio50"]["opp"][0] + 0.01, 0.25),
                    "left",
                    "Preferred\nis better",
                ),
                (
                    (METRIC_SUMMARY_XLIM["hdratio50"]["opp"][1] - 0.01, 0.75),
                    "right",
                    "Alternate\nis better",
                ),
            ],
        },
        "hdratioboot": {
            "opp": [
                (
                    (METRIC_SUMMARY_XLIM["hdratioboot"]["opp"][0] + 0.01, 0.25),
                    "left",
                    "Preferred\nis better",
                ),
                (
                    (METRIC_SUMMARY_XLIM["hdratioboot"]["opp"][1] - 0.01, 0.75),
                    "right",
                    "Alternate\nis better",
                ),
            ],
            "deg": [],
            "relationships": [
                (
                    (METRIC_SUMMARY_XLIM["hdratioboot"]["opp"][0] + 0.01, 0.25),
                    "left",
                    "Preferred\nis better",
                ),
                (
                    (METRIC_SUMMARY_XLIM["hdratioboot"]["opp"][1] - 0.01, 0.75),
                    "right",
                    "Alternate\nis better",
                ),
            ],
        },
    }

    @staticmethod
    def get_xlabel(sumstr):
        m = Summarizer.get_metric(sumstr)
        s = Summarizer.get_summary(sumstr)
        return Plots.METRIC_SUMMARY_XLABEL[m][s]

    @staticmethod
    def get_xlim(sumstr):
        m = Summarizer.get_metric(sumstr)
        s = Summarizer.get_summary(sumstr)
        return Plots.METRIC_SUMMARY_XLIM[m][s]

    @staticmethod
    def get_labels(sumstr):
        m = Summarizer.get_metric(sumstr)
        s = Summarizer.get_summary(sumstr)
        return Plots.METRIC_SUMMARY_LABELS[m][s]

    @staticmethod
    def readcdf(fpath):
        cdf = list()
        with open(fpath) as fd:
            for line in fd:
                x, y = line.split()
                cdf.append((float(x), float(y)))
        return cdf

    @staticmethod
    def plot_diff_ci(
        diff_cdf: List[Tuple[float, float]],
        lower_bound_cdf: List[Tuple[float, float]],
        upper_bound_cdf: List[Tuple[float, float]],
        xlabel: str,
        ylabel: str,
        xlim: Tuple[float, float],
        labels: List[Tuple[Tuple[float, float], str, str]],
        outfile: pathlib.Path,
    ):
        logging.info("plotting %s", str(outfile))
        fig, ax1 = plt.subplots(figsize=(7,3.5))
        ax1.set_xlabel(xlabel, fontsize=20)
        ax1.set_ylabel(ylabel, fontsize=20)
        ax1.tick_params(axis="both", which="major", labelsize=16)
        ax1.set_xlim(xlim[0], xlim[1])
        ax1.xaxis.set_ticks(np.arange(xlim[0], xlim[1]+xlim[1]/10000, (xlim[1]-xlim[0])/4))
        ax1.set_ylim(0, 1)
        for pos, alignment, text in labels:
            ax1.annotate(
                text,
                xy=pos,
                fontsize=16,
                horizontalalignment=alignment,
                backgroundcolor="white",
            )
        fig.tight_layout()

        xs, ys = zip(*diff_cdf)
        ax1.step(xs, ys, where="post")

        xslo, yslo = zip(*lower_bound_cdf)
        xsup, ysup = zip(*upper_bound_cdf)
        xslonorm = list()
        xsupnorm = list()
        joinys = list(sorted(yslo + ysup))
        for y in joinys:
            i = bisect.bisect_right(yslo, y)
            i = min(i, len(xslo) - 1)
            xslonorm.append(xslo[i])
            i = bisect.bisect_right(ysup, y)
            i = min(i, len(xsup) - 1)
            xsupnorm.append(xsup[i])
        ax1.fill_betweenx(
            joinys,
            xslonorm,
            xsupnorm,
            step="post",
            color="#333333",
            alpha=0.4,
            linewidth=0,
        )

        # plt.legend(loc="best", fontsize=16)
        plt.grid()
        plt.savefig(outfile, bbox_inches="tight")
        plt.close(fig)

    @staticmethod
    def plot_multiline(
        label2cdf: Mapping[str, List[Tuple[float, float]]],
        xlabel: str,
        ylabel: str,
        xlim: Tuple[float, float],
        labels: List[Tuple[Tuple, str]],
        outfile: pathlib.Path,
    ):
        lines = ["-", "--", "-.", ":"]
        linecycler = cycle(lines)

        fig, ax1 = plt.subplots(figsize=(7,3.5))
        ax1.set_xlabel(xlabel, fontsize=20)
        ax1.set_ylabel(ylabel, fontsize=20)
        ax1.tick_params(axis="both", which="major", labelsize=16)
        ax1.set_xlim(xlim[0], xlim[1])
        ax1.xaxis.set_ticks(np.arange(xlim[0], xlim[1]+xlim[1]/1000, (xlim[1]-xlim[0])/4))
        ax1.set_ylim(0, 1)
        for pos, alignment, text in labels:
            ax1.annotate(
                text,
                xy=pos,
                fontsize=16,
                horizontalalignment=alignment,
                backgroundcolor="white",
            )
        fig.tight_layout()
        for label, cdf in label2cdf.items():
            xs, ys = zip(*cdf)
            ax1.step(xs, ys, next(linecycler), label=label, where="post")
        plt.legend(loc="best", fontsize=16)
        plt.grid()
        plt.savefig(outfile, bbox_inches="tight")
        plt.close(fig)
