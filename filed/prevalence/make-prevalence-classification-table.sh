#!/bin/bash
set -eu

BASE=output/nonsticky/hdratio_ci_lower_bound_0.05

get_table () {
    local base=$1
    max=$(grep global_improv_bytes $base/unknown.data | cut -d " " -f 3)
    max=$(echo "scale=5; 100*$max" | bc)
    improv_total=0
    echo "% $target $max"
    for cls in continuous one-off diurnal multiday unknown ; do
        if [[ -e $base/$cls.data ]] ; then
            improv=$(grep class_improv_bytes $base/$cls.data | cut -d " " -f 3)
            improv=$(echo "scale=5; 100*$improv" | bc)
        else
            improv=0.0
        fi
        improv_total=$(echo "scale=5; $improv_total + $improv" | bc)
    done
    for cls in continuous one-off diurnal multiday unknown ; do
        if [[ -e $base/$cls.data ]] ; then
            total=$(grep class_total_bytes $base/$cls.data | cut -d " " -f 3)
            total=$(echo "scale=5; 100*$total" | bc)
            improv=$(grep class_improv_bytes $base/$cls.data | cut -d " " -f 3)
            improv=$(echo "scale=5; 100*$improv*$max/$improv_total" | bc)
            improvrel=$(echo "scale=5; $improv/$max" | bc)
        else
            total=0.0
            improv=0.0
            improvrel=0.0
        fi
        echo "$cls & $total & $improv & $improvrel \\\\"
    done
}

for target in minrtt_ci_lower_bound_5 hdratio_ci_lower_bound_0.05 ; do
    get_table output-class/nonsticky/$target
done

