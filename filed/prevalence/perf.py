from typing import Any, Mapping

import configs
import csvhelp


class PerfDB:
    def __init__(self, db: csvhelp.DB, config: configs.Config):
        self.pid2stats = dict()
        for pathid, time2bin in db.pid2time2bin.items():
            self.pid2stats[pathid] = PathPerfStats(time2bin, config)


class PathPerfStats:
    def __init__(self, time2bin: Mapping[int, csvhelp.TimeBin], config: configs.Config):
        self.time2stats = dict()
        self.primary_route_keys = set()
        for time, timebin in time2bin.items():
            self.time2stats[time] = TimeBinStats(timebin, config)
            self.primary_route_keys.add(timebin.get_primary_route().key)


class TimeBinStats:
    def __init__(self, timebin, config):
        primary = timebin.get_primary_route()
        bestalt = timebin.get_best_alternate(config.timebin_compare_routes)
        self.primary_type = primary.peer_type
        self.primary_subtype = primary.peer_subtype
        self.bestalt_type = bestalt.peer_type
        self.bestalt_subtype = bestalt.peer_subtype
        self.diff_ci = config.timebin_perf_diff_ci(primary, bestalt)
        self.diff_shift = config.timebin_perf_diff_shift(self.diff_ci, config)
        self.aspath_len_diff = primary.bgp_as_path_len - bestalt.bgp_as_path_len
        self.aspath_len_diff_wo_prepend = (
            primary.bgp_as_path_min_len_prepending_removed
            - bestalt.bgp_as_path_min_len_prepending_removed
        )
        self.bestalt_prepended = (
            bestalt.bgp_as_path_len > bestalt.bgp_as_path_min_len_prepending_removed
        )

    # CheckShift checks if there is a shift in a given TimeBinStats object based on
    # the confidence interval of the performance difference (TimeBinStats.diff_ci)
    # at a given threshold level.
    class CiLowerBoundShiftChecker:
        def __init__(self, threshold: Any[int, float]):
            self.threshold = threshold

        def __call__(self, stats: TimeBinStats):
            return stats.diff_ci[0] > self.threshold

        def __str__(self):
            if isinstance(self.threshold, int):
                return "shift-lb-%d" % self.threshold
            if isinstance(self.threshold, float):
                return "shift-lb-%0.2f" % self.threshold
            raise RuntimeError("unknown threshold type: %s" % type(self.threshold))
