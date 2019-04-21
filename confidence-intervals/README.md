# Confidence Interval for Percentiles

This is helper/test code for computing percentiles of distributions.
The approach was suggested to us by Matt Calder.  We have used the
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
