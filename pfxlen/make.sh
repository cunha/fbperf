#!/bin/bash
set -eu

NUMSAMPLES="200 1000"
OUTDIR=$(pwd)/output
INPUT=$(pwd)/tests/test.csv.gz
INPUT=/home/cunha/Dropbox/shared/SIGCOMM-2019-Daiquery/imc2019/latest_1.2.1_impact_aggregations.csv.gz


../golang/install.sh pfxlen $(pwd)

for ns in $NUMSAMPLES ; do
    mkdir -p $OUTDIR/$ns
    ./pfxlen $INPUT $OUTDIR/$ns $ns
    ./plot.py --outdir output/$ns
done

