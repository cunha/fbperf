#!/bin/bash
set -eu

# output file columns:
# 0 sorting_key (see definitions below)
# 1 vip_metro
# 2 bgp_prefix
# 3 cont
# 4 cc
# 5 distinct_shifts
# 6 bad_bytes
# 7 noroute_bytes
# 8 shifted_bytes
# 9 valid_bytes
# 10 wideci_bytes
# 11 bad_bins
# 12 noroute_bins
# 13 shifted_bins
# 14 valid_bins
# 15 wideci_bins
# 16 temporal_behavior

FIELDS="sorting_key vip_metro bgp_prefix cont cc distinct_shifts bad_bytes noroute_bytes shifted_bytes valid_bytes wideci_bytes bad_bins noroute_bins shifted_bins valid_bins wideci_bins temporal_behavior"

# sorting key for continuous: shifted_bins/valid_bins
# sorting key for diurnal: bad_bins/shifted_bins
# sorting key for episodic: bad_bins/distinct_shifts

function sort_continuous {
    local fn=$1
    grep Continuous $fn | awk '{print $13/$14,$0;}' | sort -k 1 -g
}

function sort_diurnal {
    local fn=$1
    grep Diurnal $fn | awk '{print $11/$13,$0;}' | sort -k 1 -g
}

function sort_episodic {
    local fn=$1
    grep Episodic $fn | awk '{print $13/$5,$0;}' | sort -k 1 -g
}

OUTDIR=sorted-path-summaries
TEMPDIR=../output-5410-talk/tempconfig--bin-900--days-2--fracExisting-0.80--fracWithAlternate-0.80--fracValid-0.80--cont-0.75--minBadBins-8--badBinPrev-0.80--uneventful-0.05/

mkdir -p $OUTDIR
basename $TEMPDIR > $OUTDIR/temp-config.txt

CONFIGS="hdratio50--deg--bound-true--diff-thresh-0.10--diff-ci-0.10--base-ci-0.20:hdratio50-deg-0.10 hdratio50--opp--bound-true--diff-thresh-0.10--diff-ci-0.10:hdratio50-opp-0.10 minrtt50--deg--bound-true--diff-thresh-10.00--diff-ci-10.00--base-ci-20.00:minrtt50-deg-10.0 minrtt50--opp--bound-true--diff-thresh-10.00--diff-ci-10.00--hdratio-diff-ci-0.10:minrtt50-opp-10.0"

for cfgspec in $CONFIGS ; do
    dir=${cfgspec%%:*}
    shortname=${cfgspec##*:}
    for behavior in continuous diurnal episodic ; do
        outfile=$OUTDIR/$shortname-$behavior.csv
        echo $FIELDS > $outfile
        sort_$behavior $TEMPDIR/$dir/path-summaries.txt >> $outfile
        sed -i 's/ /,/g' $outfile
    done
done