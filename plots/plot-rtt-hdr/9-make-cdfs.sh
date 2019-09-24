#!/bin/sh
set -eu

awk -F, 'NR>1{if($3<=30){print $4,$1;}}' 9.csv | sort -k1 -g | buildcdf > 0-30.cdf
awk -F, 'NR>1{if($3>30&&$3<=50){print $4,$1;}}' 9.csv | sort -k1 -g | buildcdf > 31-50.cdf
awk -F, 'NR>1{if($3>50&&$3<=80){print $4,$1;}}' 9.csv | sort -k1 -g | buildcdf > 51-80.cdf
awk -F, 'NR>1{if($3>=80){print $4,$1;}}' 9.csv | sort -k1 -g | buildcdf > 81+.cdf
