#!/bin/sh
set -eu

path=$1

cat $1/path-summaries.txt \
    | grep -Ee "(Uneventful|Continuous|Diurnal|Episodic)" \
    | awk '{cnt += $9;}END{print cnt;}'