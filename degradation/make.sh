#!/bin/bash
set -eu

OUTDIR=output/
INPUT=/home/cunha/Dropbox/shared/SIGCOMM-2019-Daiquery/imc2019/0510/daiquery_2428570520535743.csv.gz
INPUT=$(pwd)/tests/test.csv.gz

make_cdf () {
    local filter=$1
    local prefix=$2
    echo "[$prefix] [$filter]"
    for metric in rtt hdr ; do
        zcat $OUTDIR/bestalt-vs-pri-$metric.csv.gz | tail -n +2 \
                | $filter | tee $prefix-$metric.txt \
                | awk '{print $7,$2;}' \
                | sort -g -t " " -k 1 | buildcdf > $prefix-$metric.cdf
        zcat $OUTDIR/bestalt-vs-pri-$metric.csv.gz | tail -n +2 \
                | $filter | tee $prefix-$metric-lb.txt \
                | awk '{print $6,$2;}' \
                | sort -g -t " " -k 1 | buildcdf > $prefix-$metric-lb.cdf
        zcat $OUTDIR/bestalt-vs-pri-$metric.csv.gz | tail -n +2 \
                | $filter | tee $prefix-$metric-ub.txt \
                | awk '{print $8,$2;}' \
                | sort -g -t " " -k 1 | buildcdf > $prefix-$metric-ub.cdf
    done
}


plot_pri_vs_alt () {
    local prefix=$1
    for metric in rtt hdr ; do
    ./plot-$metric.py $prefix-$metric.cdf $prefix-$metric-lb.cdf \
            $prefix-$metric-ub.cdf $prefix-$metric.pdf
    done
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
