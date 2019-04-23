#!/bin/bash
set -eu

OUTDIR=$(pwd)/output
INPUT=$(pwd)/tests/daiquery_8120.csv.gz
INPUT=/home/cunha/Dropbox/shared/SIGCOMM-2019-Daiquery/imc2019/daiquery_812039595842379.csv.gz

mkdir -p $OUTDIR

../golang/install.sh minsamples $(pwd)

# ./minsamples $INPUT $OUTDIR

./plot.py --outdir output/
