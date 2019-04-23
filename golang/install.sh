#!/bin/sh
set -eu

PROGDIR=$(cd "$(dirname "$0")" ; pwd -P)
export GOPATH=$PROGDIR

go install cdnperf/minsamples
go install cdnperf/pfxlen

if [ $# -ge 2 ] ; then
    bin=$1
    dstdir=$2
    cp $PROGDIR/bin/$bin $dstdir
fi
