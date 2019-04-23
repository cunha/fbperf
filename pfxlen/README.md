# Computing MinRTT Spread as Function of Prefix Length

This code plots a graph of MinRTT spread (the spread is the difference
between two percentiles of the MinRTT distribution, e.g., the P75
- P25).  We vary prefix lengths as a function of prefix lengths.

Each row of the CSV can either have data for:

* A (real) BGP prefix (not-NULL original and NULL aggregated prefix
  columns)
* Aggregate data for all BGP prefixes for a given ASN (two NULL prefix
  columns)
* Deaggregate data for a BGP prefix into multiple /24s or multiple /48s
  (NULL original and not-NULL aggregated prefix columns)
