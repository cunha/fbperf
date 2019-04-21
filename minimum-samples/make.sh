#!/bin/bash
set -eu

INPUT=$(pwd)/tests/daiquery_8120.csv
OUTDIR=$(pwd)/output

mkdir -p $OUTDIR

../golang/install.sh minsamples $(pwd)
./minsamples $INPUT $OUTDIR
