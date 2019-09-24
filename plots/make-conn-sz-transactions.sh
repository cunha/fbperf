#!/bin/bash
set -eu

DUMP=/home/cunha/Dropbox/IMC19-ycchiu/daiquery_732325460552579.csv

mkdir -p conn-sz-transactions

cat $DUMP \
        | xsv search -d '\t' --select protocol "http/1.1" \
        | xsv select num_transactions,cdf_bytes_acked \
        | tail -n +2 \
        | tr "," " " \
        | sort --numeric-sort --key 1 --field-separator " " \
        | awk 'BEGIN{h=0.0;}{while(h>$2){next;}print $0;h+=0.0001;}' \
        > conn-sz-transactions/bytes-http1.cdf

cat $DUMP \
        | xsv search -d '\t' --select protocol "h2" \
        | xsv select num_transactions,cdf_bytes_acked \
        | tail -n +2 \
        | tr "," " " \
        | sort --numeric-sort --key 1 --field-separator " " \
        | awk 'BEGIN{h=0.0;}{while(h>$2){next;}print $0;h+=0.0001;}' \
        > conn-sz-transactions/bytes-http2.cdf

cat $DUMP \
        | xsv search -d '\t' --select protocol "NULL" \
        | xsv select num_transactions,cdf_bytes_acked \
        | tail -n +2 \
        | tr "," " " \
        | sort --numeric-sort --key 1 --field-separator " " \
        | awk 'BEGIN{h=0.0;}{while(h>$2){next;}print $0;h+=0.0001;}' \
        > conn-sz-transactions/bytes-all.cdf

cat $DUMP \
        | xsv search -d '\t' --select protocol "http/1.1" \
        | xsv select num_transactions,num_connections \
        | tail -n +2 \
        | tr "," " " \
        | sort --numeric-sort --key 1 --field-separator " " \
        | buildcdf \
        > conn-sz-transactions/transactions-http1.cdf

cat $DUMP \
        | xsv search -d '\t' --select protocol "h2" \
        | xsv select num_transactions,num_connections \
        | tail -n +2 \
        | tr "," " " \
        | sort --numeric-sort --key 1 --field-separator " " \
        | buildcdf \
        > conn-sz-transactions/transactions-http2.cdf

cat $DUMP \
        | xsv search -d '\t' --select protocol "NULL" \
        | xsv select num_transactions,num_connections \
        | tail -n +2 \
        | tr "," " " \
        | sort --numeric-sort --key 1 --field-separator " " \
        | buildcdf \
        > conn-sz-transactions/transactions-all.cdf
