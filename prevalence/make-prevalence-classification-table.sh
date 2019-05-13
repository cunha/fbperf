#!/bin/bash
set -eu

BASE=output/nonsticky/hdratio_ci_lower_bound_0.05

get_table () {
    local base=$1
    for cls in continuous one-off diurnal unknown ; do
        if [[ -e $base/$cls.data ]] ; then
            total=$(grep class_total_bytes $base/$cls.data | cut -d " " -f 3)
            total=$(echo "scale=2; 100*$total" | bc)
            improv=$(grep class_improv_bytes $base/$cls.data | cut -d " " -f 3)
            improv=$(echo "scale=2; 100*$improv" | bc)
        else
            total=0.00
            improv=0.00
        fi
        echo "$cls & $total $ $improv \\"
    done
}

for target in minrtt_ci_lower_bound_5 hdratio_ci_lower_bound_0.05 ; do
    echo "% $target"
    get_table output/nonsticky/$target
done

