#!/bin/bash
set -eu

INPUT=/home/cunha/Dropbox/shared/SIGCOMM-2019-Daiquery/imc2019/0510/daiquery_412103476238599.csv.gz

mkdir -p hawaii

zcat $INPUT | xsv search -d '\t' --select client_state all \
        | xsv select time,minrtt_ms_p50,num_samples \
        | tail -n +2 \
        | tr "," " " \
        | sort --general-numeric-sort --key 1 --field-separator " " \
        > hawaii/all-minrtt-samples.ts

zcat $INPUT | xsv search -d '\t' --select client_state California \
        | xsv select time,minrtt_ms_p50,num_samples \
        | tail -n +2 \
        | tr "," " " \
        | sort --general-numeric-sort --key 1 --field-separator " " \
        > hawaii/ca-minrtt-samples.ts

zcat $INPUT | xsv search -d '\t' --select client_state Hawaii \
        | xsv select time,minrtt_ms_p50,num_samples \
        | tail -n +2 \
        | tr "," " " \
        | sort --general-numeric-sort --key 1 --field-separator " " \
        > hawaii/hi-minrtt-samples.ts

