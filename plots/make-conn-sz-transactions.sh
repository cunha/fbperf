#!/bin/bash
set -eu

DUMP=/home/cunha/Dropbox/shared/SIGCOMM-2019-Daiquery/imc2019/0513/daiquery_320042105334265.csv.gz

mkdir -p conn-sz-transactions

zcat $DUMP \
        | xsv search -d '\t' --select protocol "http/1.1" \
        | xsv select num_transactions,cdf_bytes_acked \
        | tail -n +2 \
        | tr "," " " \
        | sort --numeric-sort --key 1 --field-separator " " \
        | awk 'BEGIN{h=0.0;}{while(h>$2){next;}print $0;h+=0.0001;}' \
        > conn-sz-transactions/bytes-http1.cdf

zcat $DUMP \
        | xsv search -d '\t' --select protocol "h2" \
        | xsv select num_transactions,cdf_bytes_acked \
        | tail -n +2 \
        | tr "," " " \
        | sort --numeric-sort --key 1 --field-separator " " \
        | awk 'BEGIN{h=0.0;}{while(h>$2){next;}print $0;h+=0.0001;}' \
        > conn-sz-transactions/bytes-http2.cdf

zcat $DUMP \
        | xsv search -d '\t' --select protocol "NULL" \
        | xsv select num_transactions,cdf_bytes_acked \
        | tail -n +2 \
        | tr "," " " \
        | sort --numeric-sort --key 1 --field-separator " " \
        | awk 'BEGIN{h=0.0;}{while(h>$2){next;}print $0;h+=0.0001;}' \
        > conn-sz-transactions/bytes-all.cdf

