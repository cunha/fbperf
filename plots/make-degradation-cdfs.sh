#!/bin/bash
set -eu

INPUT=/home/cunha/Dropbox/shared/SIGCOMM-2019-Daiquery/imc2019/0510/daiquery_2428570520535743.csv.gz

mkdir -p degradation

# zcat $INPUT \
#         | xsv select -d '\t' minrtt_ms_p50_lb_diff_of_ci_vs_best_bucket,bytes_acked_sum \
#         | tail -n +2 \
#         | tr "," " " \
#         | sort --general-numeric-sort --key 1 --field-separator " " \
#         | buildcdf > degradation/minrtt-traffic.cdf

zcat $INPUT \
        | xsv select -d '\t' hdratio_lb_ci_of_diff_vs_best_bucket,bytes_acked_sum \
        | grep -v NULL \
        | tail -n +2 \
        | tr "," " " \
        | sort --general-numeric-sort --key 1 --field-separator " " \
        | buildcdf > degradation/hdratio-traffic.cdf

