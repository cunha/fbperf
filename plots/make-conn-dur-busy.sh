#!/bin/bash
set -eu
set -x

# DURATION_DUMP=/home/cunha/Dropbox/shared/SIGCOMM-2019-Daiquery/imc2019/0513/daiquery_326772568007375.csv.gz

DURATION_DUMP=/home/cunha/Dropbox/IMC19-ycchiu/daiquery_2404384046312928.csv

# BUSYTIME_DUMP=/home/cunha/Dropbox/shared/SIGCOMM-2019-Daiquery/imc2019/0513/daiquery_2327912730816455.csv.gz

BUSYTIME_DUMP=/home/cunha/Dropbox/IMC19-ycchiu/daiquery_510874769458116.csv

mkdir -p conn-dur-busy

cat $DURATION_DUMP \
        | xsv select -d '\t' time_since_accept_ms_rounded,num_connections,protocol \
        | tail -n +2 \
        | awk -F, '{if($3=="http/1.1"){print $1,$2;}}' \
        | sort --numeric-sort --key 1 --field-separator " " \
        | buildcdf > conn-dur-busy/dur-http1.cdf

cat $DURATION_DUMP \
        | xsv select -d '\t' time_since_accept_ms_rounded,num_connections,protocol \
        | tail -n +2 \
        | awk -F, '{if($3=="h2"){print $1,$2;}}' \
        | sort --numeric-sort --key 1 --field-separator " " \
        | buildcdf > conn-dur-busy/dur-http2.cdf

cat $DURATION_DUMP \
        | xsv select -d '\t' time_since_accept_ms_rounded,num_connections,protocol \
        | tail -n +2 \
        | awk -F, '{if($3=="NULL"){print $1,$2;}}' \
        | sort --numeric-sort --key 1 --field-separator " " \
        | buildcdf > conn-dur-busy/dur-all.cdf

cat $BUSYTIME_DUMP \
        | xsv select -d '\t' percent_time_busy_or_stalled_rounded,num_connections,protocol \
        | tail -n +2 \
        | awk -F, '{if($3=="http/1.1"){print $1,$2;}}' \
        | sort --general-numeric-sort --key 1 --field-separator " " \
        | buildcdf > conn-dur-busy/busy-http1.cdf

cat $BUSYTIME_DUMP \
        | xsv select -d '\t' percent_time_busy_or_stalled_rounded,num_connections,protocol \
        | tail -n +2 \
        | awk -F, '{if($3=="h2"){print $1,$2;}}' \
        | sort --general-numeric-sort --key 1 --field-separator " " \
        | buildcdf > conn-dur-busy/busy-http2.cdf

cat $BUSYTIME_DUMP \
        | xsv select -d '\t' percent_time_busy_or_stalled_rounded,num_connections,protocol \
        | tail -n +2 \
        | awk -F, '{if($3=="NULL"){print $1,$2;}}' \
        | sort --general-numeric-sort --key 1 --field-separator " " \
        | buildcdf > conn-dur-busy/busy-all.cdf
