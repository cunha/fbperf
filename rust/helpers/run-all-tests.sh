#!/bin/sh
set -eu

for fn in *.csv.gz ; do
    outdir=${fn%%.csv.gz}
    rm -rf ../$outdir
    RUST_BACKTRACE=1 RUST_LOG=trace ../target/debug/perfstats \
        $fn --outdir ../$outdir
done