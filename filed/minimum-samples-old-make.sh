#!/bin/bash
set -eu

function compute_ci_size_from_csv_pctiles () {
    local z=1.959964
    local samples=$1
    local pct=$2
    diff=$(echo "scale=5; 100 * $z * sqrt(($pct/100) * (1 - ($pct/100)) / $samples)" | bc)
    lower=$(( pct - diff ))
    if [[ $lower -le 0 ]] ; then lower=1 fi
    upper=$(( pct + diff ))
    if [[ $upper -gt 100 ]] ; then upper=100 ; fi
    tr \" "" - | jq '.[lower],.[upper]' 
    return (lower, upper)

    # try tr | jq
}

INPUT=input.csv
OUTDIR=output/
CSVCOL_NSAMPLES=num_samples_in_subgroup
CSVCOL_RTT_P10_CI=minrtt_ms_p10_with_confidence_intervals
CSVCOL_RTT_P50_CI=minrtt_ms_p50_with_confidence_intervals
CSVCOL_RTT_PCTILES=minrtt_ms_percentiles
RTT_PCTILES="10 50"

# Get subsampling thresholds
subsamples=$(xsv select $CSVCOL_NSAMPLES --no-headers $INPUT | sort | uniq)

for ss in $subsamples ; do
    xsv search --select $CSVCOL_NSAMPLES $ss $INPUT \
            | xsv select $CSVCOL_RTT_PCTILES \
            | tr '"[],'
            > $OUTDIR/pctiles_${ss}_samples.txt
    for pct in $RTT_PCTILES ; do
        mkdir -p $OUTPUT/${ss}samples-p$pct
        cat $OUTDIR/pctiles_${ss}_samples.txt \
                | compute_ci_size_from_csv_pctiles $ss $pct \
                | sort | buildcdf \
                > $OUTDIR/rtt_p10_cidiff_${ss}_samples.cdf
    else


    fi


done
