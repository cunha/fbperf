#!/bin/bash
set -eu

INPUT=$(pwd)/tests/daiquery_8120.csv.gz
OUTDIR=$(pwd)/output

mkdir -p $OUTDIR

../golang/install.sh minsamples $(pwd)
./minsamples $INPUT $OUTDIR
