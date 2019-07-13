use crate::PrefixData;

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use ipnet::IpNet;
use serde::{Deserialize, Serialize};

use crate::timeseries::Timed;

#[derive(Debug, Deserialize)]
pub(super) struct RouteInfo {
    #[serde(rename = "time")]
    pub time: u64,
    // This can be a list of prefixes in the CSV file; reasons include (i) path changes during the
    // time window and (ii) multiple prefixes being merged into a larger prefix. In these cases,
    // we always pick the *most specific* of the listed prefixes. We can differentiate between (i)
    // and (ii) by checking if `prefix.contains(bgp_prefix)`. We compute the set of original BGP
    // prefixes by looking at cases (i) only.
    #[serde(rename = "bgp_prefix")]
    pub bgp_prefix: IpNet,
    #[serde(rename = "agg_prefix")]
    pub prefix: IpNet,
    #[serde(rename = "origin_asn")]
    pub origin_asn: u32,
    #[serde(rename = "min_rtt_p50")]
    pub lat50: i32,
    #[serde(rename = "hdratio")]
    pub hdratio: f64,
}

impl Timed for RouteInfo {
    fn get_time(&self) -> i64 {
        self.time as i64
    }
}

#[derive(Debug, Serialize)]
struct PrefixOutputStats {
    prefix: IpNet,
    bgp_prefix: IpNet,
    prefix_traffic: u64,
    bgp_prefix_traffic: u64,
}

impl PrefixOutputStats {
    fn new(prefix: &IpNet, prefix2data: &HashMap<IpNet, PrefixData>) -> PrefixOutputStats {
        let data = prefix2data.get(prefix).unwrap();
        let bgp_prefix_data = prefix2data.get(&data.bgp_prefix).unwrap();
        PrefixOutputStats {
            prefix: *prefix,
            bgp_prefix: data.bgp_prefix,
            prefix_traffic: data.total_traffic,
            bgp_prefix_traffic: bgp_prefix_data.total_traffic,
        }
    }
}

pub(super) fn load_input(infn: &PathBuf) -> HashMap<IpNet, PrefixData> {
    let mut prefix2data: HashMap<IpNet, PrefixData> = HashMap::new();
    let mut rdr = csv::Reader::from_path(infn).unwrap();
    for result in rdr.deserialize() {
        let rtinfo: RouteInfo = result.unwrap();
        let pfxdata: &mut PrefixData =
            prefix2data.entry(rtinfo.prefix).or_insert_with(|| PrefixData::new(&rtinfo));
        pfxdata.timeseries.insert(rtinfo).unwrap();
    }
    prefix2data
}

pub(super) fn dump_output(
    prefix2data: &HashMap<IpNet, PrefixData>,
    aggregated: &HashSet<IpNet>,
    outfn: &PathBuf,
) {
    let mut writer = csv::Writer::from_path(outfn).unwrap();

    let mut traffic_total: u64 = 0;
    let mut traffic_kept: u64 = 0;
    let mut traffic_merged: u64 = 0;
    let mut traffic_deagg: u64 = 0;
    let mut prefixes_kept: HashSet<IpNet> = HashSet::new();
    let mut prefixes_merged: HashSet<IpNet> = HashSet::new();
    let mut prefixes_deagg: HashSet<IpNet> = HashSet::new();

    for prefix in aggregated {
        let rec = PrefixOutputStats::new(prefix, prefix2data);
        traffic_total += rec.prefix_traffic;
        if rec.prefix != rec.bgp_prefix {
            if rec.prefix.contains(&rec.bgp_prefix) {
                traffic_merged += rec.prefix_traffic;
                prefixes_merged.insert(rec.prefix);
            } else {
                debug_assert!(rec.bgp_prefix.contains(&rec.prefix));
                traffic_deagg += rec.prefix_traffic;
                prefixes_deagg.insert(rec.prefix);
            }
        } else {
            traffic_kept += rec.prefix_traffic;
            prefixes_kept.insert(rec.prefix);
        }
        writer.serialize(rec).unwrap();
    }

    println!("original prefixes:");
    println!("  kept: {}", prefixes_kept.len());
    println!("  merged: {}", prefixes_merged.len());
    println!("  deagg: {}", prefixes_deagg.len());
    println!("traffic:");
    println!("  total: {} 100.0", traffic_total);
    println!("  kept: {} {}", traffic_kept, traffic_kept as f64 / traffic_total as f64);
    println!("  merged: {} {}", traffic_merged, traffic_merged as f64 / traffic_total as f64);
    println!("  deagg: {} {}", traffic_deagg, traffic_deagg as f64 / traffic_total as f64);
}
