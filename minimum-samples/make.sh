#!/bin/bash
set -eu

INPUT=$(pwd)/tests/daiquery_8120.csv.gz
OUTDIR=$(pwd)/output

mkdir -p $OUTDIR

../golang/install.sh minsamples $(pwd)

./minsamples $INPUT $OUTDIR

PERCENTILES="10 50"
NSAMPLES_LIMITS="0 50 100 150 200 250 500 750 1000"

for pct in $PERCENTILES ; do
    for limit in $NSAMPLES_LIMITS ; do
        statsfn=output/ci_stats_${limit}samples.txt 
        if [[ ! -s $statsfn ]] ; then continue ; fi
        xsv select MinRttP${pct}Diff $statsfn \
                | tail -n +2 | sort -n | buildcdf \
                > output/minrtt${pct}_ci_size_${limit}samples.cdf
    done
done

./plot.py --outdir output/
