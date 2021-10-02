#!/bin/bash
set -eu
set -x

PATHID=kul,202.44.224.0/19
PATHID=lax,2001:e60:a000::/36
PATHID=qro,177.237.160.0/19
PATHID=dfw,136.50.0.0/20
PATHID=hkg,210.213.219.0/24
PATHID=dfw,168.194.0.0/24
PATHID=mrs,154.121.16.0/21
PATHID=arn,85.115.248.0/24
PATHID=lax,75.84.0.0/15
PATHID=mia,186.91.96.0/19
PATHID=sof,83.168.0.0/19

BASEDIR=/home/cunha/data/dump-split-5410
OUTDIR=/home/cunha/git/fbperf/plots/talk/plot-timeseries/outdir

file=$BASEDIR/${PATHID%%,*}.tsv.gz
outfile=$OUTDIR/${PATHID/\//-}
grepexp=$(echo -e ${PATHID/,/\\t})

zcat "$file" | grep "$grepexp" | sort -k 1 -g > $outfile
