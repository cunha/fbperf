#!/bin/bash
set -eu

OUTDIR=output/
INPUT=$(pwd)/tests/test.csv.gz
INPUT=/home/cunha/Dropbox/shared/SIGCOMM-2019-Daiquery/imc2019/daiquery_383373092256903.csv.gz

make_cdf () {
    local filter=$1
    local prefix=$2
    echo "[$prefix] [$filter]"
    zcat $OUTDIR/bestalt-vs-pri.csv.gz | tail -n +2 \
            | $filter | tee $prefix.txt \
            | awk '{print $7,$2;}' \
            | sort -g -t " " -k 1 | buildcdf > $prefix.cdf
    zcat $OUTDIR/bestalt-vs-pri.csv.gz | tail -n +2 \
            | $filter | tee $prefix-lb.txt \
            | awk '{print $6,$2;}' \
            | sort -g -t " " -k 1 | buildcdf > $prefix-lb.cdf
    zcat $OUTDIR/bestalt-vs-pri.csv.gz | tail -n +2 \
            | $filter | tee $prefix-ub.txt \
            | awk '{print $8,$2;}' \
            | sort -g -t " " -k 1 | buildcdf > $prefix-ub.cdf
}


plot_pri_vs_alt () {
    local prefix=$1
    ./plot.py $prefix.cdf $prefix-lb.cdf $prefix-ub.cdf $prefix.pdf
}

mkdir -p $OUTDIR
zcat $INPUT | ./proc.py --outdir $OUTDIR

make_cdf cat $OUTDIR/bestalt-vs-pri-all
make_cdf 'awk {if($3==1){print($0);}}' $OUTDIR/bestalt-vs-pri-w-alt
make_cdf 'awk {if($1==0&&$3==1){print($0);}}' $OUTDIR/bestalt-vs-pri-w-alt-v4
make_cdf 'awk {if($1==1&&$3==1){print($0);}}' $OUTDIR/bestalt-vs-pri-w-alt-v6

plot_pri_vs_alt $OUTDIR/bestalt-vs-pri-all
plot_pri_vs_alt $OUTDIR/bestalt-vs-pri-w-alt
plot_pri_vs_alt $OUTDIR/bestalt-vs-pri-w-alt-v4
plot_pri_vs_alt $OUTDIR/bestalt-vs-pri-w-alt-v6
