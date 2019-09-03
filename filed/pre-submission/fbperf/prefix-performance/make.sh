#!/bin/bash
set -eu

DROPBOX=/home/cunha/Dropbox/shared/SIGCOMM-2019-Data/fb_exports
CSV=dscp_47_bucketed_hourly/2018-11-23-2-unquoted.csv

NPROCS=4

breakup_per_prefixes () {  # untested
    local outdir prefixes pfx_per_file
    mkdir -p outdir
    prefixes=$(cut -d, -f4 < "$DROPBOX/$CSV" | sort -u | wc -l)
    pfx_per_file=$(( prefixes / 1024 + 1 ))
    gawk -vFPAT='[^,]*|"[^"]*"' 'BEGIN{ pcnt = '$pfx_per_file'; }
             {if($1$2 != last){ last=$1$2; pcnt+=1; }
              if(pcnt > '$pfx_per_file'){ pcnt=1; fcnt+=1; }
              print $0 >> "'$outdir'/"cnt"-metro.csv" }' < "$CSV"
}

breakup_per_metro () {
    local outdir=$1
    mkdir -p "$outdir"
    echo "breaking up $DROPBOX/$CSV per metro into $outdir"
    tail -n +2 "$DROPBOX/$CSV" | gawk -vFPAT='[^,]*|"[^"]*"' \
            '{ print $0 >> "'$outdir'/"$4"-metro.csv"; }'
}

make_mpp_pickles () {
    local datadir=$1
    CSVHEADER="$DROPBOX/$CSV.header"
    for csvfn in $datadir/*-metro.csv ; do
        local picklefn=${csvfn%%.csv}.pickle
        echo "--csv $csvfn --csvheader $CSVHEADER --pickle $picklefn"
    done > pickle.tasks
    local ntasks=$(wc -l < pickle.tasks)
    echo "make_pickles: $NPROCS workers, $ntasks tasks"
    xargs --max-procs "$NPROCS" --max-args 6 ./pickle-per-metro.py < pickle.tasks
    rm -f pickle.tasks
}

make_graphs () {
    local datadir=$1
    local graphdir=$2
    for picklefn in $datadir/*-metro.pickle ; do
        metro=$(basename "$picklefn")
        metro=${metro%%-metro.pickle}
        mkdir -p "$graphdir/$metro"
        echo "--pickle $picklefn --graphdir $graphdir/$metro"
    done > sample.tasks
    xargs --max-procs 12 --max-args 6 ./plot-sample-prefixes.py < sample.tasks
    rm -f sample.tasks
}

repickle_mpp_per_asn () {
    local indir=$1
    local outdir=$2
    local asnlist=$3
    mkdir -p "$outdir"
    for picklefn in $indir/*-metro.pickle ; do
        local basefn=$(basename "$picklefn")
        echo "--pickle $picklefn --out-pickle $outdir/$basefn --asn $asnlist"
    done > perasn.tasks
    local ntasks=$(wc -l < perasn.tasks)
    echo "repickle_per_asn: $NPROCS workers, $ntasks tasks"
    xargs --max-procs "$NPROCS" --max-args 6 ./repickle-per-asn.py < perasn.tasks
    rm -f perasn.tasks
}

# breakup_per_metro per-metro
# make_mpp_pickles per-metro
repickle_mpp_per_asn per-metro per-asn 52782,52419,16822,12351,52413,1376
# make_graphs data graphs

