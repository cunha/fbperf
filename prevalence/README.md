# Classifying prefixes by opportunity pattern

```
day:
  nbins:
  nbins_improv:
  longest_streak:
  nshifts:

continuous:
  nbins_improv >= 0.9 nbins (for all days)

oneOff:
  longest_streak > X
  nshifts == 1
  only one day with nbins_improv > 0

diurnal:
  longest_streak > X and nshifts == 1 (for all days)
```
