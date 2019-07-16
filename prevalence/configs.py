import csvhelp
import perf


# bin_duration_secs
#   Compute the number of bins per day and between two timestamps.
#
# min_frac_bins_per_day
#   The number of bins we require a route to show up for to consider it in the
#   analysis. Brandon found that most of the traffic is concentrated in prefixes
#   that have enough samples for most bins.
#


CONFIGS = [
    {
        "prefix": "minrtt",
        "routeinfo_validator": csvhelp.RouteInfo.MaxMinRttCiSizeValidator(3),
        "timebin_compare_routes": csvhelp.RouteInfo.compare_median_minrtt,
        "timebin_perf_diff_ci": csvhelp.RouteInfo.minrtt_median_diff_ci,
        "timebin_perf_diff_shift": perf.TimeBinStats.CiLowerBoundShiftChecker(5),
        "bin_duration_secs": 900,
        "min_frac_bins_per_day": 0.8,
    }
]


class Config:
    # pylint: disable=E1101
    def __init__(self, param2value):
        for param, value in param2value.items():
            setattr(self, param, value)
        self.min_bins_per_day = 24 * 3600 * self.min_frac_bins_per_day // self.bin_duration_secs

    def __str__(self):
        return "%s--bins-%d--cisizes-%d-%0.2f--thresh-%d-%0.2f" % (
            self.prefix,
            self.min_bins_per_day,
            self.max_minrtt_ci_size,
            self.max_hdratio_ci_size,
            self.minrtt_threshold,
            self.hdratio_threshold,
        )
