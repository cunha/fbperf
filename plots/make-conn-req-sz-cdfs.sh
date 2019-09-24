#!/bin/bash
set -eu

RESPONSES_DUMP=/home/cunha/Dropbox/IMC19-ycchiu/daiquery_396501081041409.csv
VIDEO_RESPONSES_DUMP=/home/cunha/Dropbox/IMC19-ycchiu/daiquery_520877295314526.csv
CONNECTIONS_DUMP=/home/cunha/Dropbox/IMC19-ycchiu/daiquery_2397086703900950.csv

mkdir -p conn-req-sz

cat $RESPONSES_DUMP \
        | xsv select -d '\t' response_size,cdf_responses \
        | tail -n +2 \
        | tr "," " " \
        | sort --numeric-sort --key 1 --field-separator " " \
        | awk 'BEGIN{h=0.0;}{while(h>$2){next;}print $0;h+=0.0001;}' \
        > conn-req-sz/responses.cdf

cat $VIDEO_RESPONSES_DUMP \
        | xsv select -d '\t' response_size,cdf_responses \
        | tail -n +2 \
        | tr "," " " \
        | sort --numeric-sort --key 1 --field-separator " " \
        | awk 'BEGIN{h=0.0;}{while(h>$2){next;}print $0;h+=0.0001;}' \
        > conn-req-sz/video.cdf

cat $CONNECTIONS_DUMP \
        | xsv select -d '\t' bytes_acked,cdf_connections \
        | tail -n +2 \
        | tr "," " " \
        | sort --numeric-sort --key 1 --field-separator " " \
        | awk 'BEGIN{h=0.0;}{while(h>$2){next;}print $0;h+=0.0001;}' \
        > conn-req-sz/connections.cdf

