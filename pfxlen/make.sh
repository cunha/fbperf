#!/bin/bash
set -eu

NUMSAMPLES="200 1000"
OUTDIR=$(pwd)/output
INPUT=$(pwd)/tests/test.csv.gz
INPUT=/home/cunha/Dropbox/shared/SIGCOMM-2019-Daiquery/imc2019/latest_1.2.1_impact_aggregations.csv.gz

make_aggtype_cdfs () {
    ns=$1
    DEAGG_TYPES="asn bgp /24"
    DEAGG_DATA=/home/cunha/Dropbox/shared/SIGCOMM-2019-Daiquery/imc2019/daiquery_433148210591155.csv.gz
    # The data file only has v4 prefixes, so we only plot v4
    for dt in $DEAGG_TYPES ; do
        echo "processing $ns samples, aggtype $dt"
        zcat $DEAGG_DATA \
            | csvgrep --tabs -c deagg_type -r "^$dt$" \
            | csvcut -c num_samples,bytes_acked_sum,minrtt_ms_percentiles_p10_p25_p50_p75_p90 \
            | tail -n +2 | tr -d '"[]' | tr ',' ' ' \
            | gawk '{if($1 > '$ns'){print $6 - $4, $2;}}' \
            | sort -g -k 1 \
            | buildcdf \
            > $OUTDIR/$ns/aggtype_${dt/\//s}_25spread75_v4.cdf
            # The file only has v4 prefixes, no need to filter:
            # | xsv search --select bgp_ip_prefix "[0-9]+\.[0-9]\." \
    done
}

../golang/install.sh pfxlen $(pwd)
for ns in $NUMSAMPLES ; do
    mkdir -p $OUTDIR/$ns
    # ./pfxlen $INPUT $OUTDIR/$ns $ns
    # ./plot.py --outdir output/$ns
    make_aggtype_cdfs $ns
    ./plot-aggtype-cdfs.py --outdir $OUTDIR/$ns
done

