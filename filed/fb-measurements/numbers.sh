#!/bin/bash
set -eu

echo "###########################################################################"
echo "# PREF vs BEST"
echo "###########################################################################"
for metric in rtt_ms_p50 rtt_ms_p95 hd_capable_frac ; do
N=$(cat data/private,public,transit-private,public,transit-smpl500-$metric.txt | wc -l)
echo "### METRIC $metric ($N prefix,metro pairs)"
unweight=$(awk '{if($1==0){print $2;}}' data/private,public,transit-private,public,transit-smpl500-$metric.cdf)
weighted=$(awk '{if($1==0){print $2;}}' data/private,public,transit-private,public,transit-smpl500-$metric-weight.cdf)
echo "fraction of metro,prefix and traffic where preferred >= best: $unweight $weighted"
done

echo "###########################################################################"
echo "# PEER vs TRANSIT"
echo "###########################################################################"
for metricspec in rtt_ms_p50:20 rtt_ms_p95:20 hd_capable_frac:0.05 ; do
read -r metric noisethresh <<< "${metricspec//:/ }"
N=$(cat data/private-transit-smpl500-$metric.txt data/public-transit-smpl500-$metric.txt | wc -l)
echo "### METRIC $metric ($N prefix,metro pairs)"
unweight=$(awk '{if($1==0){print $2;}}' data/peers-transit-smpl500-$metric.cdf)
weighted=$(awk '{if($1==0){print $2;}}' data/peers-transit-smpl500-$metric-weight.cdf)
echo "fraction of metro,prefix and traffic where peers >= transit: $unweight $weighted"
unweight=$(cat data/private-transit-smpl500-$metric.txt data/public-transit-smpl500-$metric.txt | awk '{if($1<0){cnt+=$1;c+=1;}}END{print cnt/c;}')
weighted=$(cat data/private-transit-smpl500-$metric.txt data/public-transit-smpl500-$metric.txt | awk '{if($1<0){cnt+=$1*$5;c+=$5;}}END{print cnt/c;}')
echo "average improvement when peer > transit: $unweight $weighted"
unweight=$(cat data/private-transit-smpl500-$metric.txt data/public-transit-smpl500-$metric.txt | awk '{if($1>0){cnt+=$1;c+=1}}END{print cnt/c;}')
weighted=$(cat data/private-transit-smpl500-$metric.txt data/public-transit-smpl500-$metric.txt | awk '{if($1>0){cnt+=$1*$5;c+=$5}}END{print cnt/c;}')
echo "average degradation when peer < transit: $unweight $weighted"

unweight=$(awk '{if($1>=-'$noisethresh'){print $2; exit;}}' data/peers-transit-smpl500-$metric.cdf)
weighted=$(awk '{if($1>=-'$noisethresh'){print $2; exit;}}' data/peers-transit-smpl500-$metric-weight.cdf)
echo "fraction of metro,prefix and traffic where peers >= transit by $noisethresh: $unweight $weighted"
unweight=$(cat data/private-transit-smpl500-$metric.txt data/public-transit-smpl500-$metric.txt | awk '{if($1<-'$noisethresh'){cnt+=$1;c+=1;}}END{print cnt/c;}')
weighted=$(cat data/private-transit-smpl500-$metric.txt data/public-transit-smpl500-$metric.txt | awk '{if($1<-'$noisethresh'){cnt+=$1*$5;c+=$5;}}END{print cnt/c;}')
echo "average improvement when peer > transit by $noisethresh: $unweight $weighted"
unweight=$(awk '{if($1>='$noisethresh'){print 1-$2; exit;}}' data/peers-transit-smpl500-$metric.cdf)
weighted=$(awk '{if($1>='$noisethresh'){print 1-$2; exit;}}' data/peers-transit-smpl500-$metric-weight.cdf)
echo "fraction of metro,prefix and traffic where peers < transit by $noisethresh: $unweight $weighted"
unweight=$(cat data/private-transit-smpl500-$metric.txt data/public-transit-smpl500-$metric.txt | awk '{if($1>'$noisethresh'){cnt+=$1;c+=1;}}END{print cnt/c;}')
weighted=$(cat data/private-transit-smpl500-$metric.txt data/public-transit-smpl500-$metric.txt | awk '{if($1>'$noisethresh'){cnt+=$1*$5;c+=$5;}}END{print cnt/c;}')
echo "average degradation when peer < transit by $noisethresh: $unweight $weighted"

echo -n "average AS path len difference: "
cat data/private-transit-smpl500-$metric.txt data/public-transit-smpl500-$metric.txt | awk '{print $7-$8;}' | stats --mean
echo -n "average AS path len difference when peers < transit: "
cat data/private-transit-smpl500-$metric.txt data/public-transit-smpl500-$metric.txt | awk '{if($1>0){print $7-$8;}}' | stats --mean
echo -n "average AS path len difference when peers < transit by $noisethresh: "
cat data/private-transit-smpl500-$metric.txt data/public-transit-smpl500-$metric.txt | awk '{if($1>'$noisethresh'){print $7-$8;}}' | stats --mean
done

echo "###########################################################################"
echo "# PNI vs PX"
echo "###########################################################################"

for metric in rtt_ms_p50 rtt_ms_p95 hd_capable_frac ; do
N=$(cat data/private-public-smpl500-$metric.txt | wc -l)
M=$(awk '{if($11==1){print $0;}}' data/private-public-smpl500-$metric.txt | wc -l)
echo "### METRIC $metric ($N prefix,metro pairs, $M with same peer ASN)"

unweight=$(awk '{if($1==0){print $2;}}' data/private-public-smpl500-$metric-sameasn.cdf)
weighted=$(awk '{if($1==0){print $2;}}' data/private-public-smpl500-$metric-sameasn-weight.cdf)
echo "fraction of metro,prefix and traffic where PNI >= PX and peer ASN is the same: $unweight $weighted"
unweight=$(cat data/private-public-smpl500-$metric.txt | awk '{if($1<0){cnt+=$1;c+=1;}}END{print cnt/c;}')
weighted=$(cat data/private-public-smpl500-$metric.txt | awk '{if($1<0){cnt+=$1*$5;c+=$5;}}END{print cnt/c;}')
echo "average improvement when PNI > PX: $unweight $weighted"
unweight=$(cat data/private-public-smpl500-$metric.txt | awk '{if($1>0){cnt+=$1;c+=1;}}END{print cnt/c;}')
weighted=$(cat data/private-public-smpl500-$metric.txt | awk '{if($1>0){cnt+=$1*$5;c+=$5;}}END{print cnt/c;}')
echo "average degradation when PNI < PX: $unweight $weighted"

done

echo "###########################################################################"
echo "# AS path lengths"
echo "###########################################################################"

for metric in rtt_ms_p50 rtt_ms_p95 hd_capable_frac ; do
for types in private,public transit private,public,transit ; do
N=$(cat data/aspathlen-$types-$types-smpl500-$metric.txt | wc -l)
echo "### METRIC $metric $types ($N prefix,metro pairs)"

unweight=$(awk '{if($7<$8){diff+=1;}c+=1;}END{print diff/c;}' data/aspathlen-$types-$types-smpl500-$metric.txt )
weighted=$(awk '{if($7<$8){diff+=$5;}c+=$5;}END{print diff/c;}' data/aspathlen-$types-$types-smpl500-$metric.txt )
echo "fraction of metro,prefix and traffic with different AS-path lengths: $unweight $weighted"
unweight=$(awk '{if($7<$8){c+=1;if($1>0){worse+=1;}}}END{print worse/c;}' data/aspathlen-$types-$types-smpl500-$metric.txt)
weighted=$(awk '{if($7<$8){c+=$5;if($1>0){worse+=$5;}}}END{print worse/c;}' data/aspathlen-$types-$types-smpl500-$metric.txt)
echo "fraction of metro,prefix and traffic where short performs worse (1+ len diff): $unweight $weighted"
unweight=$(awk '{if($7<$8 && $1>0){c+=1;if($10<$8){prep+=1;}}}END{print prep/c;}' data/aspathlen-$types-$types-smpl500-$metric.txt)
weighted=$(awk '{if($7<$8 && $1>0){c+=$5;if($10<$8){prep+=$5;}}}END{print prep/c;}' data/aspathlen-$types-$types-smpl500-$metric.txt)
echo "when short performs worse, fraction of long with prepending: $unweight $weighted"
unweight=$(awk '{if($7<$8 && $1>0){c+=1;if($10<=$7){prep+=1;}}}END{print prep/c;}' data/aspathlen-$types-$types-smpl500-$metric.txt)
weighted=$(awk '{if($7<$8 && $1>0){c+=$5;if($10<=$7){prep+=$5;}}}END{print prep/c;}' data/aspathlen-$types-$types-smpl500-$metric.txt)
echo "w.s.p.w., fraction of long with same length ignoring prepending on long: $unweight $weighted"
unweight=$(awk '{if($7<$8 && $1>0){c+=1;if($10<=$9){prep+=1;}}}END{print prep/c;}' data/aspathlen-$types-$types-smpl500-$metric.txt)
weighted=$(awk '{if($7<$8 && $1>0){c+=$5;if($10<=$9){prep+=$5;}}}END{print prep/c;}' data/aspathlen-$types-$types-smpl500-$metric.txt)
echo "w.s.p.w., fraction of long with same length ignoring prepending on both: $unweight $weighted"
done
done

exit 0


echo "###########################################################################"
echo "##### Major stats per continent"

for cont in AF AS EU NA OC SA ; do
for metric in rtt_ms_p50 ; do
echo "###########################################################################"
echo "### CONTINENT $cont METRIC $metric"
echo -n "fraction of metro,prefix where preferred >= best: "
awk '{if($1==0){print $2; exit;}}' data/private,public,transit-private,public,transit-smpl500-$metric-$cont.cdf
echo -n "fraction of traffic where preferred >= best: "
awk '{if($1==0){print $2; exit;}}' data/private,public,transit-private,public,transit-smpl500-$metric-weight-$cont.cdf
echo -n "fraction of metro,prefix where peers >= transit: "
awk '{if($1==0){print $2; exit;}}' data/peers-transit-smpl500-$metric-$cont.cdf
echo -n "fraction of traffic where peers >= transit: "
awk '{if($1==0){print $2; exit;}}' data/peers-transit-smpl500-$metric-weight-$cont.cdf
echo -n "average improvement (unweighted) when peer > transit: "
cat data/private-transit-smpl500-$metric.txt data/public-transit-smpl500-$metric.txt \
    | awk '{if($6=="'$cont'"){print $0;}}' \
    | awk '{if($1<0){cnt+=$1;c+=1;}}END{if(c==0){c=1;}print cnt/c;}'
echo -n "average degradation (unweighted) when peer < transit: "
cat data/private-transit-smpl500-$metric.txt data/public-transit-smpl500-$metric.txt \
    | awk '{if($6=="'$cont'"){print $0;}}' \
    | awk '{if($1>0){cnt+=$1;c+=1}}END{if(c==0){c=1;}print cnt/c;}'
echo -n "fraction of metro,prefix where PNI >= PX and peer ASN is the same: "
awk '{if($1==0){print $2; exit;}}' data/private-public-smpl500-$metric-sameasn-$cont.cdf
echo -n "fraction of metro,prefix where short performs worse (1+ len diff): "
awk '{if($6=="'$cont'"){print $0;}}' data/aspathlen-smpl500-$metric.txt \
    | awk '{if($7<$8){c+=1;if($1>0){worse+=1;}}}END{if(c==0){c=1;}print worse/c;}'
done
done
