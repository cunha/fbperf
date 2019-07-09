#!/bin/bash
set -eu

DESTDIR=/home/cunha/git/peering/FBMeasurements/graphs/imc09/plots/

mkdir -p $DESTDIR/rtt-hdr
cp rtt/all.pdf $DESTDIR/rtt-hdr/all.pdf
cp rtt/per-continent.pdf $DESTDIR/rtt-hdr/rtt-per-continent.pdf
cp hdr/per-continent.pdf $DESTDIR/rtt-hdr/hdr-per-continent.pdf

mkdir -p $DESTDIR/conn-req-sz
cp conn-req-sz/all.pdf $DESTDIR/conn-req-sz/all.pdf

mkdir -p $DESTDIR/conn-dur-busy
cp conn-dur-busy/dur.pdf $DESTDIR/conn-dur-busy/dur.pdf
cp conn-dur-busy/busy.pdf $DESTDIR/conn-dur-busy/busy.pdf

mkdir -p $DESTDIR/hawaii
cp hawaii/nsamples.pdf $DESTDIR/hawaii/
cp hawaii/minrtt.pdf $DESTDIR/hawaii/

mkdir -p $DESTDIR/conn-sz-transactions
cp conn-sz-transactions/bytes.pdf $DESTDIR/conn-sz-transactions/

mkdir -p $DESTDIR/degradation
cp degradation/minrtt-traffic.pdf $DESTDIR/degradation/
cp degradation/hdratio-traffic.pdf $DESTDIR/degradation/

