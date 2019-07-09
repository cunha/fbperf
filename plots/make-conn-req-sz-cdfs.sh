#!/bin/bash
set -eu

RESPONSES_DUMP=/home/cunha/Dropbox/shared/SIGCOMM-2019-Daiquery/imc2019/0513/daiquery_613916099082180.csv.gz
VIDEO_RESPONSES_DUMP=/home/cunha/Dropbox/shared/SIGCOMM-2019-Daiquery/imc2019/0513/daiquery_291959698414168.csv.gz
CONNECTIONS_DUMP=/home/cunha/Dropbox/shared/SIGCOMM-2019-Daiquery/imc2019/0513/daiquery_385605432055681.csv.gz

mkdir -p conn-req-sz

zcat $RESPONSES_DUMP \
        | xsv select -d '\t' response_size,cdf_responses \
        | tail -n +2 \
        | tr "," " " \
        | sort --numeric-sort --key 1 --field-separator " " \
        | awk 'BEGIN{h=0.0;}{while(h>$2){next;}print $0;h+=0.0001;}' \
        > conn-req-sz/responses.cdf

zcat $VIDEO_RESPONSES_DUMP \
        | xsv select -d '\t' response_size,cdf_responses \
        | tail -n +2 \
        | tr "," " " \
        | sort --numeric-sort --key 1 --field-separator " " \
        | awk 'BEGIN{h=0.0;}{while(h>$2){next;}print $0;h+=0.0001;}' \
        > conn-req-sz/video.cdf

zcat $CONNECTIONS_DUMP \
        | xsv select -d '\t' bytes_acked,cdf_connections \
        | tail -n +2 \
        | tr "," " " \
        | sort --numeric-sort --key 1 --field-separator " " \
        | awk 'BEGIN{h=0.0;}{while(h>$2){next;}print $0;h+=0.0001;}' \
        > conn-req-sz/connections.cdf

