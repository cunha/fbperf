use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use ipnet::IpNet;
use serde::{Deserialize, Serialize};

use crate::aggregation::timeseries::{TimeSeries, Timed};

#[derive(Debug, Deserialize)]
pub struct RouteInfo {
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
    pub hdratio: f32,
}

impl Timed for RouteInfo {
    fn get_time(&self) -> i64 {
        self.time as i64
    }
}

#[derive(Debug, Serialize)]
struct PrefixOutputStats {
    origin_asn: u32,
    prefix: IpNet,
    bgp_prefix: IpNet,
    prefix_traffic: u64,
    bgp_prefix_traffic: u64,
}

impl PrefixOutputStats {
    fn new(
        asn: u32,
        prefix: &IpNet,
        asn2prefix2data: &HashMap<u32, HashMap<IpNet, PrefixData>>,
    ) -> PrefixOutputStats {
        let data = asn2prefix2data.get(&asn).unwrap().get(prefix).unwrap();
        let bgp_prefix_data = asn2prefix2data.get(&asn).unwrap().get(&data.bgp_prefix).unwrap();
        PrefixOutputStats {
            origin_asn: asn,
            prefix: *prefix,
            bgp_prefix: data.bgp_prefix,
            prefix_traffic: data.total_traffic,
            bgp_prefix_traffic: bgp_prefix_data.total_traffic,
        }
    }
}

pub fn load_input(infn: &PathBuf) -> HashMap<u32, HashMap<IpNet, PrefixData>> {
    let mut asn2prefix2data: HashMap<u32, HashMap<IpNet, PrefixData>> = HashMap::new();
    let mut rdr = csv::Reader::from_path(infn).unwrap();
    for result in rdr.deserialize() {
        let rtinfo: RouteInfo = result.unwrap();
        let prefix2data = asn2prefix2data.entry(rtinfo.origin_asn).or_insert_with(HashMap::new);
        let pfxdata: &mut PrefixData =
            prefix2data.entry(rtinfo.prefix).or_insert_with(|| PrefixData::new(&rtinfo));
        pfxdata.timeseries.insert(rtinfo).unwrap();
    }
    asn2prefix2data
}

pub fn dump_output(
    asn2prefix2data: &HashMap<u32, HashMap<IpNet, PrefixData>>,
    asn2aggregated: &HashMap<u32, HashSet<IpNet>>,
    outfn: &PathBuf,
) {
    let mut writer = csv::Writer::from_path(outfn).unwrap();

    let mut traffic_kept: u64 = 0;
    let mut traffic_merged: u64 = 0;
    let mut traffic_deagg: u64 = 0;
    let mut asnpfx_kept: HashSet<(u32, IpNet)> = HashSet::new();
    let mut asnpfx_merged: HashSet<(u32, IpNet)> = HashSet::new();
    let mut asnpfx_deagg: HashSet<(u32, IpNet)> = HashSet::new();

    for (asn, aggregated) in asn2aggregated {
        for prefix in aggregated {
            let rec = PrefixOutputStats::new(*asn, prefix, asn2prefix2data);
            if rec.prefix != rec.bgp_prefix {
                if rec.prefix.contains(&rec.bgp_prefix) {
                    traffic_merged += rec.prefix_traffic;
                    asnpfx_merged.insert((*asn, rec.prefix));
                } else {
                    debug_assert!(rec.bgp_prefix.contains(&rec.prefix));
                    traffic_deagg += rec.prefix_traffic;
                    asnpfx_deagg.insert((*asn, rec.prefix));
                }
            } else {
                traffic_kept += rec.prefix_traffic;
                asnpfx_kept.insert((*asn, rec.prefix));
            }
            writer.serialize(rec).unwrap();
        }
    }

    let traffic_total: f64 = (traffic_kept + traffic_merged + traffic_deagg) as f64;
    println!("(asn, prefix) pairs:");
    println!("  kept: {}", asnpfx_kept.len());
    println!("  merged: {}", asnpfx_merged.len());
    println!("  deagg: {}", asnpfx_deagg.len());
    println!("traffic:");
    println!("  total: {} 100.0", traffic_total);
    println!("  kept: {} {}", traffic_kept, traffic_kept as f64 / traffic_total);
    println!("  merged: {} {}", traffic_merged, traffic_merged as f64 / traffic_total);
    println!("  deagg: {} {}", traffic_deagg, traffic_deagg as f64 / traffic_total);
}

pub struct PrefixData {
    pub prefix: IpNet,
    pub bgp_prefix: IpNet,
    pub origin_asn: u32,
    pub timeseries: TimeSeries<RouteInfo>,
    pub total_traffic: u64,
}

impl PrefixData {
    pub fn new(init: &RouteInfo) -> PrefixData {
        PrefixData {
            prefix: init.prefix,
            bgp_prefix: init.bgp_prefix,
            origin_asn: init.origin_asn,
            timeseries: TimeSeries::new(),
            total_traffic: 0,
        }
    }
    pub fn is_deaggregated(&self) -> bool {
        self.bgp_prefix.contains(&self.prefix)
    }
    pub fn equivalent_performance(
        &self,
        other: &PrefixData,
        max_lat50_diff: i32,
        max_hdratio_diff: f32,
    ) -> bool {
        for (time, route1) in self.timeseries.iter() {
            let route2 = match other.timeseries.get(*time) {
                None => continue,
                Some(route2) => route2,
            };
            let lat50_diff = (route1.lat50 - route2.lat50).abs();
            if lat50_diff > max_lat50_diff {
                return false;
            }
            let hdratio_diff = (route1.hdratio - route2.hdratio).abs();
            if hdratio_diff > max_hdratio_diff {
                return false;
            }
        }
        true
    }
}
