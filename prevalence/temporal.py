from collections import Counter, namedtuple
from typing import Mapping

from perf import PerfDB, PathPerfStats


class PathTemporalStats:
    CONTINUOUS = "continuous"
    UNEVENTFUL = "uneventful"
    DIURNAL = "diurnal"
    EPISODIC = "episodic"
    Config = namedtuple(
        "TemporalStatsConfig",
        [
            "continuous_min_frac_bins_shifted",
            "uneventful_max_frac_bins_shifted",
            "diurnal_min_shifted_days_bad_bins",
            "diurnal_min_frac_bad_bins",
            "bin_duration_secs",
        ],
    )

    def __init__(self, time2shift: Mapping[int, PathPerfStats], config: PathTemporalStats.Config):
        series = sorted(list(time2shift.items()))
        bins_per_day = 3600 * 24 // config.bin_duration_secs
        basetime = series[0][0]

        bin_shift_counters = Counter()
        day_shift_counters = Counter()
        day_counters = Counter()
        num_shifts = 0
        shift_durations = list()

        shifted = False
        shift_streak = 0
        for time, shift in series:
            offset = (time - basetime) // config.bin_duration_secs
            bin_shift_counters[offset % bins_per_day] += 1 if shift else 0
            day_shift_counters[offset // bins_per_day] += 1 if shift else 0
            day_counters[offset // bins_per_day] += 1
            if shift:
                num_shifts += 0 if shifted else 1
                shift_streak += 1
            elif shifted:
                assert shift_streak >= 1
                shift_durations.append(shift_streak * config.bin_duration_secs)
                shift_streak = 0
            shifted = shift

        num_days = len(day_shift_counters)
        num_days_with_shift = len(c for c in day_shift_counters.values() if c > 0)
        num_bins = len(time2shift)
        num_shifted_bins = len(s for s in time2shift.values() if s)
        num_bad_bins = len(
            s for s in bin_shift_counters.values() if s > config.diurnal_min_number_bin_bad_days
        )

        fshift = num_shifted_bins / num_bins
        if fshift > config.continuous_min_frac_bins_shifted:
            pattern = PathTemporalStats.CONTINUOUS
        if fshift < config.uneventful_max_frac_bins_shifted:
            pattern = PathTemporalStats.UNEVENTFUL
        if num_bad_bins / bins_per_day > config.diurnal_min_frac_bad_bins:
            pattern = PathTemporalStats.DIURNAL
        pattern = PathTemporalStats.EPISODIC

        self.num_days = num_days
        self.min_num_bins_per_day = min(day_counters)
        self.num_days_with_shift = num_days_with_shift
        self.num_bins = num_bins
        self.num_shifted_bins = num_shifted_bins
        self.num_bad_bins = num_bad_bins
        self.num_shifts = num_shifts
        self.shift_durations_secs = shift_durations
        self.pattern = pattern
