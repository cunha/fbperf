#!/bin/bash
set -eu

INPUT=/home/cunha/Dropbox/shared/SIGCOMM-2019-Daiquery/imc2019/0510/daiquery_2428570520535743.csv.gz
RTT_MIN_SAMPLES=200
HDR_MIN_SAMPLES=200
CONTINENTS="AF AS EU NA OC SA"

mkdir -p rtt
mkdir -p hdr

zcat $INPUT \
        | xsv select -d '\t' \
                client_continent,num_samples,bytes_acked_sum,minrtt_ms_p50 \
        | tail -n +2 \
        | awk -F, '{if($2>='$RTT_MIN_SAMPLES'){print $4,$3 >> "rtt/"$1".data";}}'

zcat $INPUT \
        | xsv select -d '\t' \
                client_continent,num_samples,bytes_acked_sum,hdratio \
        | tail -n +2 \
        | awk -F, '{if($2>='$HDR_MIN_SAMPLES'&&$4!="NULL"){print $4,$3 >> "hdr/"$1".data";}}'


for metric in rtt hdr ; do
for continent in $CONTINENTS ; do
    gzip $metric/$continent.data
done
done

for metric in rtt hdr ; do
    zcat $metric/*.data.gz \
            | sort --numeric-sort --key 1 --field-separator " " \
            | buildcdf > $metric/all.cdf
for continent in $CONTINENTS ; do
    zcat $metric/$continent.data.gz \
            | sort --numeric-sort --key 1 --field-separator " " \
            | buildcdf > $metric/$continent.cdf
done
done


