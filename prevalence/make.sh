#!/bin/bash
set -eu

OUTDIR=output/
# INPUT=/home/cunha/Dropbox/shared/SIGCOMM-2019-Daiquery/imc2019/daiquery_383373092256903.csv.gz
INPUT=/home/cunha/Dropbox/shared/SIGCOMM-2019-Daiquery/imc2019/0510/daiquery_326322991366217.csv.gz
INPUT=$(pwd)/tests/test.csv.gz

mkdir -p $OUTDIR
zcat $INPUT | ./proc.py
