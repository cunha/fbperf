#!/bin/bash
# shellcheck disable=SC2231
set -eu

outdir=../../ns3-tcp/ns-allinone-3.29/ns-3.29/outdir/
output=simulation-results.txt

# RESULT target 312500 bottleneck 375000 1 |
# minRttMs 80 initCwnd 5 xferPkts 500 ackDelayMs 1 ssthresh XX |
# achievedTputBytesPerSec 329068 isValid 1 |
# targetTput1 279725 1 49342.7 targetTput2 280290 1 48777.6 |
# effectiveBottleneckBytesPerSec 361446
# maxCapableBytesPerSec 3.31806e+06
# maxAchievedBytesPerSec 360994
# outSlowStartRtts 3


generate_simulation_results () {
    rm -rf $output
    for fn in $outdir/target* ; do
        echo -n "."
        grep RESULT "$fn" \
            | awk '{print $3*8/1000, ($5-$3)*8/1000, $9, $11, $13, $15, $17, $22, $26, $30, ($20>=$3) ? 1 : 0, ($38>=$3*1440.0/1494.0) ? 1 : 0, ($38-$34)/$34, ($36 >= $34) ? 1 : 0, ($38-$20)/$34;}' \
                >> $output
    done
}

generate_generic_estimator_error_cdfs () {
    awk '{print $13;}' $output | sort -g | buildcdf \
            > generic-estimator-error-all.cdf
    awk '{if($14 == 1){ print $13; }}' $output | sort -g | buildcdf \
            > generic-estimator-error-valid.cdf
    awk '{if($6 == 1){ print $13; }}' $output | sort -g | buildcdf \
            > generic-estimator-error-nodelay.cdf
    awk '{if($6 == 1 && $14 == 1){ print $13; }}' $output | sort -g | buildcdf \
            > generic-estimator-error-valid+nodelay.cdf
    awk '{if($6 == 1 && $4*1494/$3 > $1){ print $13; }}' $output \
            | sort -g | buildcdf > generic-estimator-error-nodelay+largecwnd.cdf
}

# targetKbps bwDiffKbps minRttMs initCwndPkts xferPkts ackDelayMs ssthresh
# isValid tput1ok tput2ok baselineOk estimationOk estimationError
# estimationValid genericVsVanilla

# generate_simulation_results
generate_generic_estimator_error_cdfs

# make-plots.py --input $output
