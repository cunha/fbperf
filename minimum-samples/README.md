# Computing Minimum Number of Samples

This code plots a graph that varies the distribution of confidence
interval sizes (for the median and P10 minimum RTTs) for different
number of samples.  The goal is to pick a minimum number of samples that
allows computation of "reasonably accurate" confidence intervals while
keeping as as much data (prefix, metro, time) in the dataset as
possible.

When a given (prefix, metro, time) tuple has more than X samples,
Brandon subsamples it down to X samples; so each line in the graph shows
the distribution of confidence interval sizes for a specific, exact
number of samples.
