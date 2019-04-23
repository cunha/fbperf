This directory contains helper/test code for computing confidence
intervals of means, percentiles, difference of means, and difference of
percentiles

# Confidence Interval for Means and Difference of Means

For this we are just assuming that the sample is normally distributed
and applying normal techniques.

https://en.wikipedia.org/wiki/Confidence_interval
ihttps://www.kean.edu/~fosborne/bstat/06b2means.html

# Confidence Interval for Percentiles

This approach was suggested to us by Matt Calder.  We have used the
following references to get an idea of what it works:

https://stats.stackexchange.com/questions/99829/how-to-obtain-a-confidence-interval-for-a-percentile
https://www-users.york.ac.uk/~mb55/intro/cicent.htm
https://newonlinecourses.science.psu.edu/stat414/node/317/

The Stack Exchange solution with most votes presents a more advanced
solution than the one we use (and tested in this code).  The more
advanced solutions allows for (tighter) non-symmetric confidence
intervals around the target percentile, while the solution we use assume
symmetric confidence intervals.

The following is a reference we can use for the technique.

```
@book{conover80stats,
  author={Conover, W. J.}
  title={{Practical Nonparametric Statistics}},
  publisher={John Wiley and Sons},
  year={1980}
}
```

# Confidence Interval of Difference Between Medians

This approach was found by Brandon.  It is distribution-free and should
work for small samples too.  BibTeX:

```
@article{price02medianci,
author = {Robert M. Price and Douglas G. Bonett},
title = {{Distribution-Free Confidence Intervals for Difference and Ratio of Medians}},
journal = {Journal of Statistical Computation and Simulation},
volume = {72},
number = {2},
pages = {119--124},
year  = {2002},
}
publisher = {Taylor & Francis},
doi = {10.1080/00949650212140},
```
