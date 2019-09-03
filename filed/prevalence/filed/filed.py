# ComparatorFactory {{{
# ComparatorFactory generates functions to check if there is performance
# improvement for a given TimeSeries.TimeBinStats object.  It can generate
# functions that check for improvement based on the confidence interval of the
# performance difference (TimeBinStats.perf_diff_ci) at different threshold
# levels.  Functions return +1 when the alternate has better performance than
# alternate (as defined by the threshold), 0 when there is no significant
# difference (lower than threshold), and -1 when the primary path has better
# performance than the alternate.
class ComparatorFactory:
    @staticmethod
    def make_ci_comparator(threshold):
        def ci_comparator(stats, last_comparison_result):
            if stats.diff_ci[0] > threshold:
                return +1
            if stats.diff_ci[1] < threshold:
                return -1
            return 0
        return ci_comparator
    @staticmethod
    def make_median_comparator(threshold):
        def median_comparator(stats, last_comparison_result):
            if stats.diff_ci[1] > threshold:
                return +1
            if stats.diff_ci[1] < threshold:
                return -1
            return 0
        return median_comparator
# }}}
