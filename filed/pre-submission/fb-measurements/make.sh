#!/bin/bash
set -eu
set -x

# CSVFILE=daiquery-Aggregated-performance-per-metro-prefix-dscpValue-2018-05-23T20_28_05-07_00.csv
CSVFILE=daiquery-Annoymize-aggregated-performance-2430163597023641-2018-06-15T001859-0700.csv
CSVFILE=daiquery-2430163597023641-2018-06-15T001859-0700-extended.csv
PICKLE=filtered.pickle


generate_pickle () {
    ./pickler.py --csv $CSVFILE --output $PICKLE
}


generate_bgp_pref_data () {
    rm -f bgp-attr-rtt.tasks
    for rtypes in private:public private:transit public:transit transit:transit private,public,transit:private,public,transit ; do
        read -r rtype1 rtype2 <<< "${rtypes//:/ }"
    for samples in 500 2000 ; do
    for metric in rtt_ms_p50 rtt_ms_p95 ; do
        echo --csv "$CSVFILE" \
             --primary "$rtype1" \
             --alternate "$rtype2" \
             --algo rtt \
             --metric-column "$metric" \
             --samples-column num_samples_for_rtt \
             --min-samples $samples \
             --output "data/$rtype1-$rtype2-smpl$samples-$metric.txt" \
             >> bgp-attr-rtt.tasks
    done
    for metric in hd_capable_frac ; do
        echo --csv "$CSVFILE" \
             --primary "$rtype1" \
             --alternate "$rtype2" \
             --algo throughput \
             --metric-column "$metric" \
             --samples-column num_samples_for_throughput \
             --min-samples $samples \
             --output "data/$rtype1-$rtype2-smpl$samples-$metric.txt" \
             >> bgp-attr-rtt.tasks
    done
#     for metric in throughput_p5 throughput_p50 ; do
#         echo --csv "$CSVFILE" \
#              --primary "$rtype1" \
#              --alternate "$rtype2" \
#              --algo throughput \
#              --metric-column "$metric" \
#              --samples-column num_samples_for_throughput \
#              --min-samples $samples \
#              --output "data/$rtype1-$rtype2-smpl$samples-$metric.txt" \
#              >> bgp-attr-rtt.tasks
#     done
    done
    done
    wc -l bgp-attr-rtt.tasks
    xargs --verbose --max-procs 8 --max-args 16 ./bgp-attr.py < bgp-attr-rtt.tasks
    rm -f bgp-attr-rtt.tasks
}


generate_as_path_len_data () {
    rm -f bgp-attr-rtt.tasks
    for rtypes in private,public:private,public transit:transit private,public,transit:private,public,transit ; do
        read -r rtype1 rtype2 <<< "${rtypes//:/ }"
    for samples in 500 ; do
    for metric in rtt_ms_p50 rtt_ms_p95 ; do
        echo --csv "$CSVFILE" \
             --primary "$rtype1" \
             --alternate "$rtype2" \
             --algo rtt-length \
             --metric-column "$metric" \
             --samples-column num_samples_for_rtt \
             --min-samples $samples \
             --output "data/aspathlen-$rtype1-$rtype2-smpl$samples-$metric.txt" \
             >> bgp-attr-rtt.tasks
    done
    for metric in hd_capable_frac ; do
        echo --csv "$CSVFILE" \
             --primary "$rtype1" \
             --alternate "$rtype2" \
             --algo throughput-length \
             --metric-column "$metric" \
             --samples-column num_samples_for_throughput \
             --min-samples $samples \
             --output "data/aspathlen-$rtype1-$rtype2-smpl$samples-$metric.txt" \
             >> bgp-attr-rtt.tasks
    done
    done
    done
    wc -l bgp-attr-rtt.tasks
    xargs --verbose --max-procs 8 --max-args 16 ./bgp-attr.py < bgp-attr-rtt.tasks
    rm -f bgp-attr-rtt.tasks
}


generate_current_data () {
    rm -f bgp-attr-rtt.tasks
    for rtypes in private,public,transit:private,public,transit ; do
        read -r rtype1 rtype2 <<< "${rtypes//:/ }"
    for samples in 500 ; do
    for metric in rtt_ms_p50 rtt_ms_p95 ; do
        echo --csv "$CSVFILE" \
             --primary "$rtype1" \
             --alternate "$rtype2" \
             --algo current-rtt \
             --metric-column "$metric" \
             --samples-column num_samples_for_rtt \
             --min-samples $samples \
             --output "data/current-$rtype1-$rtype2-smpl$samples-$metric.txt" \
             >> bgp-attr-rtt.tasks
    done
    for metric in hd_capable_frac ; do
        echo --csv "$CSVFILE" \
             --primary "$rtype1" \
             --alternate "$rtype2" \
             --algo current-tput \
             --metric-column "$metric" \
             --samples-column num_samples_for_throughput \
             --min-samples $samples \
             --output "data/current-$rtype1-$rtype2-smpl$samples-$metric.txt" \
             >> bgp-attr-rtt.tasks
    done
    done
    done
    wc -l bgp-attr-rtt.tasks
    xargs --verbose --max-procs 8 --max-args 16 ./bgp-attr.py < bgp-attr-rtt.tasks
    rm -f bgp-attr-rtt.tasks
}


generate_cdfs () {
    for fn in data/private,public,transit-*.txt ; do
        cut -d " " -f 1 "$fn" | sort -g | buildcdf > "${fn%%.txt}.cdf"
        echo "${fn%%.txt}.cdf $(awk '{print $1;}' "$fn" | wc -l)" >> cdf-counts.txt
        # cut -d " " -f 2 "$fn" | sort -g | buildcdf > "${fn%%.txt}-rel.cdf"
        cut -d " " -f 1,5 "$fn" | sort -g | buildcdf > "${fn%%.txt}-weight.cdf"
        echo "${fn%%.txt}-weight.cdf $(awk '{c+=$5;}END{print c;}' "$fn")" >> cdf-counts.txt
#        for cont in AF AS EU NA OC SA ; do
#            awk '{if($6=="'$cont'"){print $1;}}' "$fn" | sort -g | buildcdf > "${fn%%.txt}-$cont.cdf"
#            echo "${fn%%.txt}-$cont.cdf $(awk '{if($6=="'$cont'"){print $0;}}' "$fn" | wc -l)" >> cdf-counts.txt
#            awk '{if($6=="'$cont'"){print $1,$5;}}' "$fn" | sort -g | buildcdf > "${fn%%.txt}-weight-$cont.cdf"
#            echo "${fn%%.txt}-weight-$cont.cdf $(awk '{if($6=="'$cont'"){c+=$5;}}END{print c;}' "$fn")" >> cdf-counts.txt
#        done
    done
    for fn in data/private-public-*.txt ; do
        cut -d " " -f 1 "$fn" | sort -g | buildcdf > "${fn%%.txt}.cdf"
        echo "${fn%%.txt}.cdf $(awk '{print $1;}' "$fn" | wc -l)" >> cdf-counts.txt
        # cut -d " " -f 2 "$fn" | sort -g | buildcdf > "${fn%%.txt}-rel.cdf"
        # cut -d " " -f 3 "$fn" | sort -g | buildcdf > "${fn%%.txt}-avg.cdf"
        cut -d " " -f 1,5 "$fn" | sort -g | buildcdf > "${fn%%.txt}-weight.cdf"
        echo "${fn%%.txt}-weight.cdf $(awk '{c+=$5;}END{print c;}' "$fn")" >> cdf-counts.txt
        awk '{if($11==1){print $1;}}' "$fn" | sort -g | buildcdf > "${fn%%.txt}-sameasn.cdf"
        echo "${fn%%.txt}-sameasn.cdf $(awk '{if($11==1){print $0;}}' "$fn" | wc -l)" >> cdf-counts.txt
        awk '{if($11==1){print $1,$5;}}' "$fn" | sort -g | buildcdf > "${fn%%.txt}-sameasn-weight.cdf"
        echo "${fn%%.txt}-sameasn-weight.cdf $(awk '{if($11==1){c+=$5;}}END{print c;}' "$fn")" >> cdf-counts.txt
#        for cont in AF AS EU NA OC SA ; do
#            awk '{if($6=="'$cont'"){print $1;}}' "$fn" | sort -g | buildcdf > "${fn%%.txt}-$cont.cdf"
#            echo "${fn%%.txt}-$cont.cdf $(awk '{if($6=="'$cont'"){print $0;}}' "$fn" | wc -l)" >> cdf-counts.txt
#            awk '{if($6=="'$cont'"){print $1,$5;}}' "$fn" | sort -g | buildcdf > "${fn%%.txt}-weight-$cont.cdf"
#            echo "${fn%%.txt}-weight-$cont.cdf $(awk '{if($6=="'$cont'"){c+=$5;}}END{print c;}' "$fn")" >> cdf-counts.txt
#            # awk '{if($6=="'$cont'" && $11==1){print $1;}}' "$fn" | sort -g | buildcdf > "${fn%%.txt}-sameasn-$cont.cdf"
#        done
    done
    for fn in data/private-transit-smpl500*.txt ; do
        base=${fn#data/private*}
        # shellcheck disable=SC2086
        cat data/{private,public}$base | cut -d " " -f 1 | sort -g | buildcdf > "data/peers${base%%.txt}.cdf"
        echo "data/peers${base%%.txt}.cdf $(cat data/{private,public}$base | wc -l)" >> cdf-counts.txt
        cat data/{private,public}$base | cut -d " " -f 1,5 | sort -g | buildcdf > "data/peers${base%%.txt}-weight.cdf"
        echo "data/peers${base%%.txt}-weight.cdf $(cat data/{private,public}$base | awk '{c+=$5;}END{print c;}')" >> cdf-counts.txt
        # cat data/*$base | cut -d " " -f 2 | sort -g | buildcdf > "data/peers${base%%.txt}-rel.cdf"
        # cat data/*$base | cut -d " " -f 3 | sort -g | buildcdf > "data/peers${base%%.txt}-avg.cdf"
        # cat data/*$base | awk '{if($7==1){print $1;}}' | sort -g | buildcdf > "data/peers${base%%.txt}-1hop.cdf"
#        for cont in AF AS EU NA OC SA ; do
#            cat data/*$base | awk '{if($6=="'$cont'"){print $1;}}' | sort -g | buildcdf > "data/peers${base%%.txt}-$cont.cdf"
#            echo "data/peers${base%%.txt}-$cont.cdf $(cat data/*$base | awk '{if($6=="'$cont'"){print $0;}}' | wc -l)" >> cdf-counts.txt
#            cat data/*$base | awk '{if($6=="'$cont'"){print $1,$5;}}' | sort -g | buildcdf > "data/peers${base%%.txt}-weight-$cont.cdf"
#            echo "data/peers${base%%.txt}-weight-$cont.cdf $(cat data/*$base | awk '{if($6=="'$cont'"){c+=$5;}}END{print c;}')" >> cdf-counts.txt
#            # cat data/*$base | awk '{if($6=="'$cont'" && $11==1){print $1;}}' | sort -g | buildcdf > "data/peers${base%%.txt}-sameasn-$cont.cdf"
#        done
    done

}

generate_aspathlen_cdfs () {
    for fn in data/aspathlen-*.txt ; do
        # prefixes
        awk '{print $1;}' "$fn" | sort -g | buildcdf > "${fn%%.txt}.cdf"
        echo "${fn%%.txt}.cdf $(awk '{print $1;}' "$fn" | wc -l)" >> cdf-counts.txt
        awk '{if($7==$8){print $1;}}' "$fn" | sort -g | buildcdf > "${fn%%.txt}-0len.cdf"
        echo "${fn%%.txt}-0len.cdf $(awk '{if($7==$8){print $1;}}' "$fn" | wc -l)" >> cdf-counts.txt
        awk '{if($7==$8-1){print $1;}}' "$fn" | sort -g | buildcdf > "${fn%%.txt}-1len.cdf"
        echo "${fn%%.txt}-1len.cdf $(awk '{if($7==$8-1){print $1;}}' "$fn" | wc -l)" >> cdf-counts.txt
        awk '{if($7==$8-2){print $1;}}' "$fn" | sort -g | buildcdf > "${fn%%.txt}-2len.cdf"
        echo "${fn%%.txt}-2len.cdf $(awk '{if($7==$8-2){print $1;}}' "$fn" | wc -l)" >> cdf-counts.txt
        awk '{if($7==$8-3){print $1;}}' "$fn" | sort -g | buildcdf > "${fn%%.txt}-3len.cdf"
        echo "${fn%%.txt}-3len.cdf $(awk '{if($7==$8-3){print $1;}}' "$fn" | wc -l)" >> cdf-counts.txt
        awk '{if($9==$10-1){print $1;}}' "$fn" | sort -g | buildcdf > "${fn%%.txt}-noprep1len.cdf"
        echo "${fn%%.txt}-noprep1len.cdf $(awk '{if($9==$10-1){print $1;}}' "$fn" | wc -l)" >> cdf-counts.txt
        awk '{if($9==$10-2){print $1;}}' "$fn" | sort -g | buildcdf > "${fn%%.txt}-noprep2len.cdf"
        echo "${fn%%.txt}-noprep2len.cdf $(awk '{if($9==$10-2){print $1;}}' "$fn" | wc -l)" >> cdf-counts.txt
        awk '{if($9<=$10-3){print $1;}}' "$fn" | sort -g | buildcdf > "${fn%%.txt}-noprep3mlen.cdf"
        echo "${fn%%.txt}-noprep3mlen.cdf $(awk '{if($9<=$10-3){print $1;}}' "$fn" | wc -l)" >> cdf-counts.txt
        awk '{if($7<=$8-1){print $1;}}' "$fn" | sort -g | buildcdf > "${fn%%.txt}-1plus.cdf"
        echo "${fn%%.txt}-1plus.cdf $(awk '{if($7<=$8-1){print $1;}}' "$fn" | wc -l)" >> cdf-counts.txt
        awk '{if($7<=$8-3){print $1;}}' "$fn" | sort -g | buildcdf > "${fn%%.txt}-3plus.cdf"
        echo "${fn%%.txt}-3plus.cdf $(awk '{if($7<=$8-3){print $1;}}' "$fn" | wc -l)" >> cdf-counts.txt
        awk '{if($7<=$8-3 && $10<$8){print $1;}}' "$fn" | sort -g | buildcdf > "${fn%%.txt}-3plus-wprep.cdf"
        echo "${fn%%.txt}-3plus-wprep.cdf $(awk '{if($7<=$8-3 && $10<$8){print $1;}}' "$fn" | wc -l)" >> cdf-counts.txt
        awk '{if($7<=$8-4){print $1;}}' "$fn" | sort -g | buildcdf > "${fn%%.txt}-4plus.cdf"
        echo "${fn%%.txt}-4plus.cdf $(awk '{if($7<=$8-4){print $1;}}' "$fn" | wc -l)" >> cdf-counts.txt
        # traffic
        awk '{print $1,$5;}' "$fn" | sort -g | buildcdf > "${fn%%.txt}-weight.cdf"
        echo "${fn%%.txt}-weight.cdf $(awk '{cnt+=$5;}END{print cnt;}' "$fn")" >> cdf-counts.txt
        awk '{if($7==$8){print $1,$5;}}' "$fn" | sort -g | buildcdf > "${fn%%.txt}-0len-weight.cdf"
        echo "${fn%%.txt}-0len-weight.cdf $(awk '{if($7==$8){c+=$5;}}END{print c;}' "$fn")" >> cdf-counts.txt
        awk '{if($7==$8-1){print $1,$5;}}' "$fn" | sort -g | buildcdf > "${fn%%.txt}-1len-weight.cdf"
        echo "${fn%%.txt}-1len-weight.cdf $(awk '{if($7==$8-1){c+=$5;}}END{print c;}' "$fn")" >> cdf-counts.txt
        awk '{if($7==$8-2){print $1,$5;}}' "$fn" | sort -g | buildcdf > "${fn%%.txt}-2len-weight.cdf"
        echo "${fn%%.txt}-2len-weight.cdf $(awk '{if($7==$8-2){c+=$5;}}END{print c;}' "$fn")" >> cdf-counts.txt
        awk '{if($7==$8-3){print $1,$5;}}' "$fn" | sort -g | buildcdf > "${fn%%.txt}-3len-weight.cdf"
        echo "${fn%%.txt}-3len-weight.cdf $(awk '{if($7==$8-3){c+=$5;}}END{print c;}' "$fn")" >> cdf-counts.txt
        awk '{if($9==$10-1){print $1,$5;}}' "$fn" | sort -g | buildcdf > "${fn%%.txt}-noprep1len-weight.cdf"
        echo "${fn%%.txt}-noprep1len-weight.cdf $(awk '{if($9==$10-1){c+=$5;}}END{print c;}' "$fn")" >> cdf-counts.txt
        awk '{if($9==$10-2){print $1,$5;}}' "$fn" | sort -g | buildcdf > "${fn%%.txt}-noprep2len-weight.cdf"
        echo "${fn%%.txt}-noprep2len-weight.cdf $(awk '{if($9==$10-2){c+=$5;}}END{print c;}' "$fn")" >> cdf-counts.txt
        awk '{if($9<=$10-3){print $1,$5;}}' "$fn" | sort -g | buildcdf > "${fn%%.txt}-noprep3mlen-weight.cdf"
        echo "${fn%%.txt}-noprep3mlen-weight.cdf $(awk '{if($9<=$10-3){c+=$5;}}END{print c;}' "$fn")" >> cdf-counts.txt
        awk '{if($7<=$8-1){print $1,$5;}}' "$fn" | sort -g | buildcdf > "${fn%%.txt}-1plus-weight.cdf"
        echo "${fn%%.txt}-1plus-weight.cdf $(awk '{if($7<=$8-1){c+=$5;}}END{print c;}' "$fn")" >> cdf-counts.txt
        awk '{if($7<=$8-3){print $1,$5;}}' "$fn" | sort -g | buildcdf > "${fn%%.txt}-3plus-weight.cdf"
        echo "${fn%%.txt}-3plus-weight.cdf $(awk '{if($7<=$8-3){c+=$5;}}END{print c;}' "$fn")" >> cdf-counts.txt
        awk '{if($7<=$8-4){print $1,$5;}}' "$fn" | sort -g | buildcdf > "${fn%%.txt}-4plus-weight.cdf"
        echo "${fn%%.txt}-4plus-weight.cdf $(awk '{if($7<=$8-4){c+=$5;}}END{print c;}' "$fn")" >> cdf-counts.txt
    done
}

generate_aslendiff_cdfs () {
    for fn in data/private-transit-smpl500-rtt_ms_p50.txt ; do
        base=${fn#data/private*}
        # prefixes
        cat data/{private,public}$base | awk '{print $7-$8;}' | sort -g | buildcdf > "data/peers${base%%.txt}-aspathlendiff.cdf"
        echo "data/peers${base%%.txt}-aspathlendiff.cdf $(cat data/{private,public}$base | wc -l)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1>0){print $7-$8;}}' | sort -g | buildcdf > "data/peers-lose${base%%.txt}-aspathlendiff.cdf"
        echo "data/peers-lose${base%%.txt}-aspathlendiff.cdf $(awk '{if($1>0){print;}}' data/{private,public}$base | wc -l)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1>0){print $9-$10;}}' | sort -g | buildcdf > "data/peers-lose${base%%.txt}-uniqlendiff.cdf"
        echo "data/peers-lose${base%%.txt}-uniqlendiff.cdf $(awk '{if($1>0){print;}}' data/{private,public}$base | wc -l)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1>20){print $7-$8;}}' | sort -g | buildcdf > "data/peers-lose20${base%%.txt}-aspathlendiff.cdf"
        echo "data/peers-lose20${base%%.txt}-aspathlendiff.cdf $(awk '{if($1>20){print;}}' data/{private,public}$base | wc -l)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1>20){print $9-$10;}}' | sort -g | buildcdf > "data/peers-lose20${base%%.txt}-uniqlendiff.cdf"
        echo "data/peers-lose20${base%%.txt}-uniqlendiff.cdf $(awk '{if($1>20){print;}}' data/{private,public}$base | wc -l)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1<0){print $7-$8;}}' | sort -g | buildcdf > "data/peers-win${base%%.txt}-aspathlendiff.cdf"
        echo "data/peers-win${base%%.txt}-aspathlendiff.cdf $(awk '{if($1<0){print;}}' data/{private,public}$base | wc -l)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1<0){print $9-$10;}}' | sort -g | buildcdf > "data/peers-win${base%%.txt}-uniqlendiff.cdf"
        echo "data/peers-win${base%%.txt}-uniqlendiff.cdf $(awk '{if($1<0){print;}}' data/{private,public}$base | wc -l)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1<-20){print $7-$8;}}' | sort -g | buildcdf > "data/peers-win20${base%%.txt}-aspathlendiff.cdf"
        echo "data/peers-win20${base%%.txt}-aspathlendiff.cdf $(awk '{if($1<-20){print;}}' data/{private,public}$base | wc -l)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1<-20){print $9-$10;}}' | sort -g | buildcdf > "data/peers-win20${base%%.txt}-uniqlendiff.cdf"
        echo "data/peers-win20${base%%.txt}-uniqlendiff.cdf $(awk '{if($1<-20){print;}}' data/{private,public}$base | wc -l)" >> cdf-counts.txt
        # traffic
        cat data/{private,public}$base | awk '{print $7-$8,$5;}' | sort -g | buildcdf > "data/peers${base%%.txt}-aspathlendiff-weight.cdf"
        echo "data/peers${base%%.txt}-aspathlendiff-weight.cdf $(awk '{c+=$5;}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1>0){print $7-$8,$5;}}' | sort -g | buildcdf > "data/peers-lose${base%%.txt}-aspathlendiff-weight.cdf"
        echo "data/peers-lose${base%%.txt}-aspathlendiff-weight.cdf $(awk '{if($1>0){c+=$5;}}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1>0){print $9-$10,$5;}}' | sort -g | buildcdf > "data/peers-lose${base%%.txt}-uniqlendiff-weight.cdf"
        echo "data/peers-lose${base%%.txt}-uniqlendiff-weight.cdf $(awk '{if($1>0){c+=$5;}}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1>20){print $7-$8,$5;}}' | sort -g | buildcdf > "data/peers-lose20${base%%.txt}-aspathlendiff-weight.cdf"
        echo "data/peers-lose20${base%%.txt}-aspathlendiff-weight.cdf $(awk '{if($1>20){c+=$5;}}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1>20){print $9-$10,$5;}}' | sort -g | buildcdf > "data/peers-lose20${base%%.txt}-uniqlendiff-weight.cdf"
        echo "data/peers-lose20${base%%.txt}-uniqlendiff-weight.cdf $(awk '{if($1>20){c+=$5;}}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1<0){print $7-$8,$5;}}' | sort -g | buildcdf > "data/peers-win${base%%.txt}-aspathlendiff-weight.cdf"
        echo "data/peers-win${base%%.txt}-aspathlendiff-weight.cdf $(awk '{if($1<0){c+=$5;}}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1<0){print $9-$10,$5;}}' | sort -g | buildcdf > "data/peers-win${base%%.txt}-uniqlendiff-weight.cdf"
        echo "data/peers-win${base%%.txt}-uniqlendiff-weight.cdf $(awk '{if($1<0){c+=$5;}}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1<-20){print $7-$8,$5;}}' | sort -g | buildcdf > "data/peers-win20${base%%.txt}-aspathlendiff-weight.cdf"
        echo "data/peers-win20${base%%.txt}-aspathlendiff-weight.cdf $(awk '{if($1<-20){c+=$5;}}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1<-20){print $9-$10,$5;}}' | sort -g | buildcdf > "data/peers-win20${base%%.txt}-uniqlendiff-weight.cdf"
        echo "data/peers-win20${base%%.txt}-uniqlendiff-weight.cdf $(awk '{if($1<-20){c+=$5;}}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
    done
    for fn in data/private-transit-smpl500-rtt_ms_p95.txt ; do
        base=${fn#data/private*}
        # prefixes
        cat data/{private,public}$base | awk '{print $7-$8;}' | sort -g | buildcdf > "data/peers${base%%.txt}-aspathlendiff.cdf"
        echo "data/peers${base%%.txt}-aspathlendiff.cdf $(cat data/{private,public}$base | wc -l)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1>0){print $7-$8;}}' | sort -g | buildcdf > "data/peers-lose${base%%.txt}-aspathlendiff.cdf"
        echo "data/peers-lose${base%%.txt}-aspathlendiff.cdf $(awk '{if($1>0){print;}}' data/{private,public}$base | wc -l)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1>0){print $9-$10;}}' | sort -g | buildcdf > "data/peers-lose${base%%.txt}-uniqlendiff.cdf"
        echo "data/peers-lose${base%%.txt}-uniqlendiff.cdf $(awk '{if($1>0){print;}}' data/{private,public}$base | wc -l)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1>60){print $7-$8;}}' | sort -g | buildcdf > "data/peers-lose60${base%%.txt}-aspathlendiff.cdf"
        echo "data/peers-lose60${base%%.txt}-aspathlendiff.cdf $(awk '{if($1>60){print;}}' data/{private,public}$base | wc -l)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1>60){print $9-$10;}}' | sort -g | buildcdf > "data/peers-lose60${base%%.txt}-uniqlendiff.cdf"
        echo "data/peers-lose60${base%%.txt}-uniqlendiff.cdf $(awk '{if($1>60){print;}}' data/{private,public}$base | wc -l)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1<0){print $7-$8;}}' | sort -g | buildcdf > "data/peers-win${base%%.txt}-aspathlendiff.cdf"
        echo "data/peers-win${base%%.txt}-aspathlendiff.cdf $(awk '{if($1<0){print;}}' data/{private,public}$base | wc -l)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1<0){print $9-$10;}}' | sort -g | buildcdf > "data/peers-win${base%%.txt}-uniqlendiff.cdf"
        echo "data/peers-win${base%%.txt}-uniqlendiff.cdf $(awk '{if($1<0){print;}}' data/{private,public}$base | wc -l)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1<-60){print $7-$8;}}' | sort -g | buildcdf > "data/peers-win60${base%%.txt}-aspathlendiff.cdf"
        echo "data/peers-win60${base%%.txt}-aspathlendiff.cdf $(awk '{if($1<-60){print;}}' data/{private,public}$base | wc -l)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1<-60){print $9-$10;}}' | sort -g | buildcdf > "data/peers-win60${base%%.txt}-uniqlendiff.cdf"
        echo "data/peers-win60${base%%.txt}-uniqlendiff.cdf $(awk '{if($1<-60){print;}}' data/{private,public}$base | wc -l)" >> cdf-counts.txt
        # traffic
        cat data/{private,public}$base | awk '{print $7-$8,$5;}' | sort -g | buildcdf > "data/peers${base%%.txt}-aspathlendiff-weight.cdf"
        echo "data/peers${base%%.txt}-aspathlendiff-weight.cdf $(awk '{c+=$5;}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1>0){print $7-$8,$5;}}' | sort -g | buildcdf > "data/peers-lose${base%%.txt}-aspathlendiff-weight.cdf"
        echo "data/peers-lose${base%%.txt}-aspathlendiff-weight.cdf $(awk '{if($1>0){c+=$5;}}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1>0){print $9-$10,$5;}}' | sort -g | buildcdf > "data/peers-lose${base%%.txt}-uniqlendiff-weight.cdf"
        echo "data/peers-lose${base%%.txt}-uniqlendiff-weight.cdf $(awk '{if($1>0){c+=$5;}}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1>60){print $7-$8,$5;}}' | sort -g | buildcdf > "data/peers-lose60${base%%.txt}-aspathlendiff-weight.cdf"
        echo "data/peers-lose60${base%%.txt}-aspathlendiff-weight.cdf $(awk '{if($1>60){c+=$5;}}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1>60){print $9-$10,$5;}}' | sort -g | buildcdf > "data/peers-lose60${base%%.txt}-uniqlendiff-weight.cdf"
        echo "data/peers-lose60${base%%.txt}-uniqlendiff-weight.cdf $(awk '{if($1>60){c+=$5;}}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1<0){print $7-$8,$5;}}' | sort -g | buildcdf > "data/peers-win${base%%.txt}-aspathlendiff-weight.cdf"
        echo "data/peers-win${base%%.txt}-aspathlendiff-weight.cdf $(awk '{if($1<0){c+=$5;}}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1<0){print $9-$10,$5;}}' | sort -g | buildcdf > "data/peers-win${base%%.txt}-uniqlendiff-weight.cdf"
        echo "data/peers-win${base%%.txt}-uniqlendiff-weight.cdf $(awk '{if($1<0){c+=$5;}}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1<-60){print $7-$8,$5;}}' | sort -g | buildcdf > "data/peers-win60${base%%.txt}-aspathlendiff-weight.cdf"
        echo "data/peers-win60${base%%.txt}-aspathlendiff-weight.cdf $(awk '{if($1<-60){c+=$5;}}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1<-60){print $9-$10,$5;}}' | sort -g | buildcdf > "data/peers-win60${base%%.txt}-uniqlendiff-weight.cdf"
        echo "data/peers-win60${base%%.txt}-uniqlendiff-weight.cdf $(awk '{if($1<-60){c+=$5;}}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
    done
    for fn in data/private-transit-smpl500-hd_capable_frac*.txt ; do
        base=${fn#data/private*}
        # prefixes
        cat data/{private,public}$base | awk '{print $7-$8;}' | sort -g | buildcdf > "data/peers${base%%.txt}-aspathlendiff.cdf"
        echo "data/peers${base%%.txt}-aspathlendiff.cdf $(cat data/{private,public}$base | wc -l)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1>0){print $7-$8;}}' | sort -g | buildcdf > "data/peers-lose${base%%.txt}-aspathlendiff.cdf"
        echo "data/peers-lose${base%%.txt}-aspathlendiff.cdf $(awk '{if($1>0){print;}}' data/{private,public}$base | wc -l)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1>0){print $9-$10;}}' | sort -g | buildcdf > "data/peers-lose${base%%.txt}-uniqlendiff.cdf"
        echo "data/peers-lose${base%%.txt}-uniqlendiff.cdf $(awk '{if($1>0){print;}}' data/{private,public}$base | wc -l)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1<0){print $7-$8;}}' | sort -g | buildcdf > "data/peers-win${base%%.txt}-aspathlendiff.cdf"
        echo "data/peers-win${base%%.txt}-aspathlendiff.cdf $(awk '{if($1<0){print;}}' data/{private,public}$base | wc -l)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1<0){print $9-$10;}}' | sort -g | buildcdf > "data/peers-win${base%%.txt}-uniqlendiff.cdf"
        echo "data/peers-win${base%%.txt}-uniqlendiff.cdf $(awk '{if($1<0){print;}}' data/{private,public}$base | wc -l)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1>0.05){print $7-$8;}}' | sort -g | buildcdf > "data/peers-lose5p${base%%.txt}-aspathlendiff.cdf"
        echo "data/peers-lose5p${base%%.txt}-aspathlendiff.cdf $(awk '{if($1>0.05){print;}}' data/{private,public}$base | wc -l)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1>0.05){print $9-$10;}}' | sort -g | buildcdf > "data/peers-lose5p${base%%.txt}-uniqlendiff.cdf"
        echo "data/peers-lose5p${base%%.txt}-uniqlendiff.cdf $(awk '{if($1>0.05){print;}}' data/{private,public}$base | wc -l)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1<-0.05){print $7-$8;}}' | sort -g | buildcdf > "data/peers-win5p${base%%.txt}-aspathlendiff.cdf"
        echo "data/peers-win5p${base%%.txt}-aspathlendiff.cdf $(awk '{if($1<-0.05){print;}}' data/{private,public}$base | wc -l)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1<-0.05){print $9-$10;}}' | sort -g | buildcdf > "data/peers-win5p${base%%.txt}-uniqlendiff.cdf"
        echo "data/peers-win5p${base%%.txt}-uniqlendiff.cdf $(awk '{if($1<-0.05){print;}}' data/{private,public}$base | wc -l)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1>0.1){print $7-$8;}}' | sort -g | buildcdf > "data/peers-lose10p${base%%.txt}-aspathlendiff.cdf"
        echo "data/peers-lose10p${base%%.txt}-aspathlendiff.cdf $(awk '{if($1>0.1){print;}}' data/{private,public}$base | wc -l)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1>0.1){print $9-$10;}}' | sort -g | buildcdf > "data/peers-lose10p${base%%.txt}-uniqlendiff.cdf"
        echo "data/peers-lose10p${base%%.txt}-uniqlendiff.cdf $(awk '{if($1>0.1){print;}}' data/{private,public}$base | wc -l)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1<-0.1){print $7-$8;}}' | sort -g | buildcdf > "data/peers-win10p${base%%.txt}-aspathlendiff.cdf"
        echo "data/peers-win10p${base%%.txt}-aspathlendiff.cdf $(awk '{if($1<-0.1){print;}}' data/{private,public}$base | wc -l)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1<-0.1){print $9-$10;}}' | sort -g | buildcdf > "data/peers-win10p${base%%.txt}-uniqlendiff.cdf"
        echo "data/peers-win10p${base%%.txt}-uniqlendiff.cdf $(awk '{if($1<-0.1){print;}}' data/{private,public}$base | wc -l)" >> cdf-counts.txt
        # traffic
        cat data/{private,public}$base | awk '{print $7-$8,$5;}' | sort -g | buildcdf > "data/peers${base%%.txt}-aspathlendiff-weight.cdf"
        echo "data/peers${base%%.txt}-aspathlendiff-weight.cdf $(awk '{c+=$5;}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1>0){print $7-$8,$5;}}' | sort -g | buildcdf > "data/peers-lose${base%%.txt}-aspathlendiff-weight.cdf"
        echo "data/peers-lose${base%%.txt}-aspathlendiff-weight.cdf $(awk '{if($1>0){c+=$5;}}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1>0){print $9-$10,$5;}}' | sort -g | buildcdf > "data/peers-lose${base%%.txt}-uniqlendiff-weight.cdf"
        echo "data/peers-lose${base%%.txt}-uniqlendiff-weight.cdf $(awk '{if($1>0){c+=$5;}}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1<0){print $7-$8,$5;}}' | sort -g | buildcdf > "data/peers-win${base%%.txt}-aspathlendiff-weight.cdf"
        echo "data/peers-win${base%%.txt}-aspathlendiff-weight.cdf $(awk '{if($1<0){c+=$5;}}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1<0){print $9-$10,$5;}}' | sort -g | buildcdf > "data/peers-win${base%%.txt}-uniqlendiff-weight.cdf"
        echo "data/peers-win${base%%.txt}-uniqlendiff-weight.cdf $(awk '{if($1<0){c+=$5;}}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1>0.05){print $7-$8,$5;}}' | sort -g | buildcdf > "data/peers-lose5p${base%%.txt}-aspathlendiff-weight.cdf"
        echo "data/peers-lose5p${base%%.txt}-aspathlendiff-weight.cdf $(awk '{if($1>0.05){c+=$5;}}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1>0.05){print $9-$10,$5;}}' | sort -g | buildcdf > "data/peers-lose5p${base%%.txt}-uniqlendiff-weight.cdf"
        echo "data/peers-lose5p${base%%.txt}-uniqlendiff-weight.cdf $(awk '{if($1>0.05){c+=$5;}}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1<-0.05){print $7-$8,$5;}}' | sort -g | buildcdf > "data/peers-win5p${base%%.txt}-aspathlendiff-weight.cdf"
        echo "data/peers-win5p${base%%.txt}-aspathlendiff-weight.cdf $(awk '{if($1<-0.05){c+=$5;}}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1<-0.05){print $9-$10,$5;}}' | sort -g | buildcdf > "data/peers-win5p${base%%.txt}-uniqlendiff-weight.cdf"
        echo "data/peers-win5p${base%%.txt}-uniqlendiff-weight.cdf $(awk '{if($1<-0.05){c+=$5;}}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1>0.1){print $7-$8,$5;}}' | sort -g | buildcdf > "data/peers-lose10p${base%%.txt}-aspathlendiff-weight.cdf"
        echo "data/peers-lose10p${base%%.txt}-aspathlendiff-weight.cdf $(awk '{if($1>0.1){c+=$5;}}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1>0.1){print $9-$10,$5;}}' | sort -g | buildcdf > "data/peers-lose10p${base%%.txt}-uniqlendiff-weight.cdf"
        echo "data/peers-lose10p${base%%.txt}-uniqlendiff-weight.cdf $(awk '{if($1>0.1){c+=$5;}}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1<-0.1){print $7-$8,$5;}}' | sort -g | buildcdf > "data/peers-win10p${base%%.txt}-aspathlendiff-weight.cdf"
        echo "data/peers-win10p${base%%.txt}-aspathlendiff-weight.cdf $(awk '{if($1<-0.1){c+=$5;}}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($1<-0.1){print $9-$10,$5;}}' | sort -g | buildcdf > "data/peers-win10p${base%%.txt}-uniqlendiff-weight.cdf"
        echo "data/peers-win10p${base%%.txt}-uniqlendiff-weight.cdf $(awk '{if($1<-0.1){c+=$5;}}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
    done
    for fn in data/private-transit-smpl500*.txt ; do
        base=${fn#data/private*}
        # prefixes
        cat data/{private,public}$base | awk '{if($7==$8){print $1;}}' | sort -g | buildcdf > "data/peers${base%%.txt}-0len.cdf"
        echo "data/peers${base%%.txt}-0len.cdf $(awk '{if($7==$8){print $0;}}' data/{private,public}$base | wc -l)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($7==$8+1){print $1;}}' | sort -g | buildcdf > "data/peers${base%%.txt}-long1.cdf"
        echo "data/peers${base%%.txt}-long1.cdf $(awk '{if($7==$8+1){print $0;}}' data/{private,public}$base | wc -l)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($7==$8+2){print $1;}}' | sort -g | buildcdf > "data/peers${base%%.txt}-long2.cdf"
        echo "data/peers${base%%.txt}-long2.cdf $(awk '{if($7==$8+2){print $0;}}' data/{private,public}$base | wc -l)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($7==$8-1){print $1;}}' | sort -g | buildcdf > "data/peers${base%%.txt}-short1.cdf"
        echo "data/peers${base%%.txt}-short1.cdf $(awk '{if($7==$8-1){print $0;}}' data/{private,public}$base | wc -l)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($7==$8-2){print $1;}}' | sort -g | buildcdf > "data/peers${base%%.txt}-short2.cdf"
        echo "data/peers${base%%.txt}-short2.cdf $(awk '{if($7==$8-2){print $0;}}' data/{private,public}$base | wc -l)" >> cdf-counts.txt
        # prefixes uniq
        cat data/{private,public}$base | awk '{if($9==$10){print $1;}}' | sort -g | buildcdf > "data/peers${base%%.txt}-0lennp.cdf"
        echo "data/peers${base%%.txt}-0lennp.cdf $(awk '{if($9==$10){print $0;}}' data/{private,public}$base | wc -l)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($9==$10+1){print $1;}}' | sort -g | buildcdf > "data/peers${base%%.txt}-long1np.cdf"
        echo "data/peers${base%%.txt}-long1np.cdf $(awk '{if($9==$10+1){print $0;}}' data/{private,public}$base | wc -l)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($9>=$10+1){print $1;}}' | sort -g | buildcdf > "data/peers${base%%.txt}-long1mnp.cdf"
        echo "data/peers${base%%.txt}-long1mnp.cdf $(awk '{if($9>=$10+1){print $0;}}' data/{private,public}$base | wc -l)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($9>=$10+2){print $1;}}' | sort -g | buildcdf > "data/peers${base%%.txt}-long2mnp.cdf"
        echo "data/peers${base%%.txt}-long2mnp.cdf $(awk '{if($9>=$10+2){print $0;}}' data/{private,public}$base | wc -l)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($9==$10-1){print $1;}}' | sort -g | buildcdf > "data/peers${base%%.txt}-short1np.cdf"
        echo "data/peers${base%%.txt}-short1np.cdf $(awk '{if($9==$10-1){print $0;}}' data/{private,public}$base | wc -l)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($9==$10-2){print $1;}}' | sort -g | buildcdf > "data/peers${base%%.txt}-short2np.cdf"
        echo "data/peers${base%%.txt}-short2np.cdf $(awk '{if($9==$10-2){print $0;}}' data/{private,public}$base | wc -l)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($9<=$10-3){print $1;}}' | sort -g | buildcdf > "data/peers${base%%.txt}-short3mnp.cdf"
        echo "data/peers${base%%.txt}-short3mnp.cdf $(awk '{if($9<=$10-3){print $0;}}' data/{private,public}$base | wc -l)" >> cdf-counts.txt
        # traffic
        cat data/{private,public}$base | awk '{if($7==$8){print $1,$5;}}' | sort -g | buildcdf > "data/peers${base%%.txt}-0len-weight.cdf"
        echo "data/peers${base%%.txt}-0len-weight.cdf $(awk '{if($7==$8){c+=$5;}}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($7==$8+1){print $1,$5;}}' | sort -g | buildcdf > "data/peers${base%%.txt}-long1-weight.cdf"
        echo "data/peers${base%%.txt}-long1-weight.cdf $(awk '{if($7==$8+1){c+=$5;}}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($7==$8+2){print $1,$5;}}' | sort -g | buildcdf > "data/peers${base%%.txt}-long2-weight.cdf"
        echo "data/peers${base%%.txt}-long2-weight.cdf $(awk '{if($7==$8+2){c+=$5;}}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($7==$8-1){print $1,$5;}}' | sort -g | buildcdf > "data/peers${base%%.txt}-short1-weight.cdf"
        echo "data/peers${base%%.txt}-short1-weight.cdf $(awk '{if($7==$8-1){c+=$5;}}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($7==$8-2){print $1,$5;}}' | sort -g | buildcdf > "data/peers${base%%.txt}-short2-weight.cdf"
        echo "data/peers${base%%.txt}-short2-weight.cdf $(awk '{if($7==$8-2){c+=$5;}}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
        # traffic uniq
        cat data/{private,public}$base | awk '{if($9==$10){print $1,$5;}}' | sort -g | buildcdf > "data/peers${base%%.txt}-0lennp-weight.cdf"
        echo "data/peers${base%%.txt}-0lennp-weight.cdf $(awk '{if($9==$10){c+=$5;}}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($9==$10+1){print $1,$5;}}' | sort -g | buildcdf > "data/peers${base%%.txt}-long1np-weight.cdf"
        echo "data/peers${base%%.txt}-long1np-weight.cdf $(awk '{if($9==$10+1){c+=$5;}}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($9>=$10+1){print $1,$5;}}' | sort -g | buildcdf > "data/peers${base%%.txt}-long1mnp-weight.cdf"
        echo "data/peers${base%%.txt}-long1mnp-weight.cdf $(awk '{if($9>=$10+1){c+=$5;}}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($9>=$10+2){print $1,$5;}}' | sort -g | buildcdf > "data/peers${base%%.txt}-long2mnp-weight.cdf"
        echo "data/peers${base%%.txt}-long2mnp-weight.cdf $(awk '{if($9>=$10+2){c+=$5;}}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($9==$10-1){print $1,$5;}}' | sort -g | buildcdf > "data/peers${base%%.txt}-short1np-weight.cdf"
        echo "data/peers${base%%.txt}-short1np-weight.cdf $(awk '{if($9==$10-1){c+=$5;}}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($9==$10-2){print $1,$5;}}' | sort -g | buildcdf > "data/peers${base%%.txt}-short2np-weight.cdf"
        echo "data/peers${base%%.txt}-short2np-weight.cdf $(awk '{if($9==$10-2){c+=$5;}}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
        cat data/{private,public}$base | awk '{if($9<=$10-3){print $1,$5;}}' | sort -g | buildcdf > "data/peers${base%%.txt}-short3mnp-weight.cdf"
        echo "data/peers${base%%.txt}-short3mnp-weight.cdf $(awk '{if($9<=$10-3){c+=$5;}}END{print c;}' data/{private,public}$base)" >> cdf-counts.txt
    done
}

generate_current_cdfs () {  #{{{
    for fn in data/current-*.txt ; do
        # prefixes
        awk '{print $1;}' "$fn" | sort -g | buildcdf > "${fn%%.txt}-current.cdf"
        echo "${fn%%.txt}-current.cdf $(awk '{print $1;}' "$fn" | wc -l)" >> cdf-counts.txt
        awk '{print $3;}' "$fn" | sort -g | buildcdf > "${fn%%.txt}-preflen.cdf"
        echo "${fn%%.txt}-preflen.cdf $(awk '{print $3;}' "$fn" | wc -l)" >> cdf-counts.txt
        awk '{print $5;}' "$fn" | sort -g | buildcdf > "${fn%%.txt}-pref.cdf"
        echo "${fn%%.txt}-pref.cdf $(awk '{print $5;}' "$fn" | wc -l)" >> cdf-counts.txt
        awk '{print $7;}' "$fn" | sort -g | buildcdf > "${fn%%.txt}-best.cdf"
        echo "${fn%%.txt}-best.cdf $(awk '{print $7;}' "$fn" | wc -l)" >> cdf-counts.txt
        # traffic
        awk '{print $1,$9;}' "$fn" | sort -g | buildcdf > "${fn%%.txt}-current-weight.cdf"
        echo "${fn%%.txt}-current-weight.cdf $(awk '{c+=$9;}END{print c;}' "$fn")" >> cdf-counts.txt
        awk '{print $3,$9;}' "$fn" | sort -g | buildcdf > "${fn%%.txt}-preflen-weight.cdf"
        echo "${fn%%.txt}-preflen-weight.cdf $(awk '{c+=$9;}END{print c;}' "$fn")" >> cdf-counts.txt
        awk '{print $5,$9;}' "$fn" | sort -g | buildcdf > "${fn%%.txt}-pref-weight.cdf"
        echo "${fn%%.txt}-pref-weight.cdf $(awk '{c+=$9;}END{print c;}' "$fn")" >> cdf-counts.txt
        awk '{print $7,$9;}' "$fn" | sort -g | buildcdf > "${fn%%.txt}-best-weight.cdf"
        echo "${fn%%.txt}-best-weight.cdf $(awk '{c+=$9;}END{print c;}' "$fn")" >> cdf-counts.txt
    done
}
#}}}


plot_graphs () {
    ./plots/plot-hdfrac.py
    ./plots/plot-rtt-p50.py
    ./plots/plot-rtt-p95.py

    # ./plots/plot-rtt-p50-cont.py

    ./plots/plot-sameasn.py

    ./plots/plot-rtt-p50-aslen.py
    ./plots/plot-rtt-p95-aslen.py
    ./plots/plot-hdfrac-aslen.py


    ./plots/plot-current.py
    ./plots/plot-aslendiff.py
}

# generate_pickle

generate_bgp_pref_data
generate_as_path_len_data
generate_current_data

rm -f cdf-counts.txt
generate_cdfs
generate_aspathlen_cdfs
generate_aslendiff_cdfs
generate_current_cdfs

plot_graphs
