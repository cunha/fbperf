#!/bin/sh
set -eu
set -x

METRO_LIST_FILE=metro-list.txt
DROPBOX_BASEDIR=/home/cunha/Dropbox/shared/SIGCOMM-2019-Daiquery/imc2019

CSVFILE=$DROPBOX_BASEDIR/0922/daiquery_412229066096438.csv.gz

OUTDIR=/home/cunha/data/dump-split-4122
mkdir -p $OUTDIR

while read metro ; do
    zcat $CSVFILE | head -n 1 > $OUTDIR/$metro.tsv
done < $METRO_LIST_FILE

zcat $CSVFILE | awk '{print $0 >> "'$OUTDIR'/"$2".tsv";}'
rm $OUTDIR/vip_metro.tsv

pigz $OUTDIR/*.tsv
