#!/bin/sh
set -eu

awk -F, 'NR>1{if($3==""&&$4!=""){print $4,$1;}}' 8.csv | sort -k1 -g | buildcdf > 8a-minrtt.cdf

awk -F, 'NR>1{if($3==""&&$5!=""){print $5,$1;}}' 8.csv | sort -k1 -g | buildcdf  > 8a-hdratio.cdf

rm -rf rtt
mkdir -p rtt
awk -F, 'NR>1{if($3!=""&&$4!=""){print $4,$1 >> "rtt/"$3".txt";}}' 8.csv
for fn in rtt/*.txt ; do
    sort -k1 -g $fn | buildcdf > ${fn%%.txt}.cdf
done

rm -rf hdr
mkdir -p hdr
awk -F, 'NR>1{if($3!=""&&$5!=""){print $5,$1 >> "hdr/"$3".txt";}}' 8.csv
for fn in hdr/*.txt ; do
    sort -k1 -g $fn | buildcdf > ${fn%%.txt}.cdf
done
