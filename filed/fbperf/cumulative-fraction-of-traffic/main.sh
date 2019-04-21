#!/bin/sh
set -eu
set -x

# FILE=../data/prefix_traffic/daiquery-2018-11-20T12_38_53-08_00.csv

# mkdir -p per-metro
# tail -n +2 $FILE | awk -F, '{ print $6 >> "per-metro/"$1".metro"; }'

# rm -f per-metro/table.txt
# for fn in per-metro/*.metro ; do
#     metro=$(basename "$fn")
#     metro=${metro%%.metro}
#     sort -r -g "$fn" | awk '{cnt+=$1;print cnt;}' > "$fn.cumulative"
#     p90idx=$(sort -r -g "$fn" | awk '{cnt+=$1;if(cnt>0.90){print NR; exit;}}')
#     p95idx=$(sort -r -g "$fn" | awk '{cnt+=$1;if(cnt>0.95){print NR; exit;}}')
#     p99idx=$(sort -r -g "$fn" | awk '{cnt+=$1;if(cnt>0.99){print NR; exit;}}')
#     nlines=$(wc -l < "$fn")
#     frac90=$(echo "scale=6; $p90idx/$nlines" | bc)
#     frac95=$(echo "scale=6; $p95idx/$nlines" | bc)
#     frac99=$(echo "scale=6; $p99idx/$nlines" | bc)
#     echo "$metro $p90idx $p95idx $p99idx $nlines $frac90 $frac95 $frac99" \
#             >> per-metro/table.txt
# done

cut -d " " -f 2 per-metro/table.txt | sort -g | buildcdf \
        > per-metro/metro-num-prefixes-p90.cdf
cut -d " " -f 3 per-metro/table.txt | sort -g | buildcdf \
        > per-metro/metro-num-prefixes-p95.cdf
cut -d " " -f 4 per-metro/table.txt | sort -g | buildcdf \
        > per-metro/metro-num-prefixes-p99.cdf

cut -d " " -f 6 per-metro/table.txt | sort -g | buildcdf \
        > per-metro/metro-frac-prefixes-p90.cdf
cut -d " " -f 7 per-metro/table.txt | sort -g | buildcdf \
        > per-metro/metro-frac-prefixes-p95.cdf
cut -d " " -f 8 per-metro/table.txt | sort -g | buildcdf \
        > per-metro/metro-frac-prefixes-p99.cdf

./plot-metro-prefix-coverage.py
