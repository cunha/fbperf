use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::{hash_map, HashMap};
use std::hash::{Hash, Hasher};

use ipnet::IpNet;
use log::{debug, error, info};

mod error;
use error::{ParseError, ParseErrorKind};

const CONFIDENCE_Z: f32 = 2.0;
const MAX_TIMEBIN_ROUTES: usize = 7;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum PeerSubtype {
    Private,
    Public,
    Paid,
}
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum PeerType {
    Peering(PeerSubtype),
    Transit,
}

pub struct DB {
    pub path2time2bin: HashMap<PathId, HashMap<u64, TimeBin>>,
    pub path2traffic: HashMap<PathId, u64>,
    pub total_traffic: u64,
    pub rows: u64,
    error_counts: HashMap<ParseErrorKind, u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PathId {
    pub vip_metro: String,
    pub bgp_ip_prefix: IpNet,
}

#[derive(Debug)]
pub struct TimeBin {
    pub time_bucket: u64,
    pub bytes_acked_sum: u64,
    pub num2route: Vec<Option<Box<RouteInfo>>>,
}

#[derive(Clone, Copy, Debug)]
pub struct RouteInfo {
    pub apm_route_num: u8,
    pub bgp_as_path_len: u8,
    pub bgp_as_path_len_wo_prepend: u8,
    pub bgp_as_path_prepending: bool,
    pub peer_type: PeerType,
    pub minrtt_num_samples: u32,
    pub minrtt_ms_p10: i16,
    pub minrtt_ms_p50: i16,
    pub minrtt_ms_p50_ci_lb: i16,
    pub minrtt_ms_p50_ci_ub: i16,
    pub hdratio_num_samples: u32,
    pub minrtt_ms_p50_var: f32,
    pub hdratio: f32,
    pub hdratio_var: f32,
    pub px_nexthops: u64,
}

trait RouteInfoValidator {
    fn check(&self, route: &RouteInfo) -> bool;
    fn describe(&self) -> String;
}

#[derive(Clone, Copy, Debug)]
struct MaxCiSizeValidator {
    median_minrtt_ci_ms: i16,
    average_hdratio_ci: f32,
}

impl PeerType {
    fn new(peer_type: &str, peer_subtype: &str) -> Result<PeerType, ParseError> {
        match (peer_type, peer_subtype) {
            ("peering", "mixed") => Ok(PeerType::Peering(PeerSubtype::Private)),
            ("peering", "private") => Ok(PeerType::Peering(PeerSubtype::Private)),
            ("peering", "public") => Ok(PeerType::Peering(PeerSubtype::Public)),
            ("route_server", "mixed") => Ok(PeerType::Peering(PeerSubtype::Public)),
            ("peering", "paid") => Ok(PeerType::Peering(PeerSubtype::Paid)),
            ("transit", "") => Ok(PeerType::Transit),
            (_, _) => Err(ParseError {
                kind: ParseErrorKind::UnknownPeeringRelationship,
                message: format!("peer_type: {}, peer_subtype: {}", peer_type, peer_subtype),
            }),
        }
    }
}

impl DB {
    pub fn from_csv_reader<R: std::io::Read>(reader: &mut csv::Reader<R>) -> DB {
        let mut db = DB {
            path2time2bin: HashMap::new(),
            path2traffic: HashMap::new(),
            total_traffic: 0,
            rows: 0,
            error_counts: HashMap::new(),
        };
        for result in reader.deserialize() {
            db.rows += 1;
            let record: HashMap<String, String> = result.unwrap();
            if (db.rows % 10000) == 0 {
                info!("{} rows", db.rows);
            }
            let pid = match PathId::from_record(&record) {
                Ok(p) => p,
                Err(e) => {
                    *db.error_counts.entry(e.kind).or_insert(0) += 1;
                    continue;
                }
            };
            let timebin = match TimeBin::from_record(&record) {
                Ok(t) => t,
                Err(e) => {
                    *db.error_counts.entry(e.kind).or_insert(0) += 1;
                    continue;
                }
            };
            db.total_traffic += timebin.bytes_acked_sum;
            db.path2traffic.entry(pid.clone()).and_modify(|e| *e += timebin.bytes_acked_sum);
            let time2bin = db.path2time2bin.entry(pid.clone()).or_insert_with(HashMap::new);
            match time2bin.entry(timebin.time_bucket) {
                hash_map::Entry::Vacant(e) => e.insert(timebin),
                hash_map::Entry::Occupied(_) => {
                    error!("TimeBin already exists, path {:?}, time {}", pid, &timebin.time_bucket);
                    debug!("{:?}", &record);
                    *db.error_counts.entry(ParseErrorKind::RepeatedTimebin).or_insert(0) += 1;
                    continue;
                }
            };
        }
        db
    }
}

impl PathId {
    fn from_record(record: &HashMap<String, String>) -> Result<PathId, ParseError> {
        if record["vip_metro"] == "NULL" {
            Err(ParseError {
                kind: ParseErrorKind::VipMetroIsNull,
                message: "vip_metro must not be NULL".to_string(),
            })
        } else {
            Ok(PathId {
                vip_metro: record["vip_metro"].to_string(),
                bgp_ip_prefix: record["bgp_ip_prefix"].parse::<IpNet>()?,
            })
        }
    }
}

impl TimeBin {
    fn from_record(rec: &HashMap<String, String>) -> Result<TimeBin, ParseError> {
        let mut timebin = TimeBin {
            time_bucket: rec["time_bucket"].parse::<u64>()?,
            bytes_acked_sum: rec["bytes_acked_sum"].parse::<u64>()?,
            num2route: Vec::with_capacity(MAX_TIMEBIN_ROUTES),
        };
        for i in 0..MAX_TIMEBIN_ROUTES {
            timebin.num2route.insert(i, RouteInfo::from_record(i, rec).ok());
        }
        Ok(timebin)
    }
    pub fn get_primary_route(&self) -> &Option<Box<RouteInfo>> {
        self.num2route.get(0).unwrap()
    }
    pub fn get_best_alternate<F>(&self, mut compare: F) -> &Option<Box<RouteInfo>> where F: FnMut(&RouteInfo, &RouteInfo) -> Ordering,
    {
        let mut bestopt: &Option<Box<RouteInfo>> = &None;
        for rtopt in &self.num2route {
            match rtopt {
                None => continue,
                Some(ref rtbox) => {
                    if rtbox.apm_route_num == 1 {
                        continue;
                    }
                    match bestopt {
                        None => bestopt = rtopt,
                        Some(ref bestbox) => {
                            if compare(bestbox.borrow(), rtbox.borrow()) == Ordering::Less {
                                bestopt = rtopt;
                            }
                        }
                    }
                }
            }
        }
        bestopt
    }
}

impl RouteInfo {
    fn from_record(i: usize, rec: &HashMap<String, String>) -> Result<Box<RouteInfo>, ParseError> {
        Ok(Box::new(RouteInfo {
            apm_route_num: rec[&format!("r{}_apm_route_num", i)].parse()?,
            bgp_as_path_len: rec[&format!("r{}_bgp_as_path_len", i)].parse()?,
            bgp_as_path_len_wo_prepend: rec
                [&format!("r{}_bgp_as_path_min_len_prepending_removed", i)]
                .parse()?,
            bgp_as_path_prepending: string_to_bool(&rec[&format!("r{}_bgp_as_path_prepending", i)]),
            peer_type: PeerType::new(
                &rec[&format!("r{}_peer_type", i)],
                &rec[&format!("r{}_peer_subtype", i)],
            )?,
            minrtt_num_samples: rec[&format!("r{}_num_samples", i)].parse()?,
            minrtt_ms_p10: rec[&format!("r{}_minrtt_ms_p10", i)].parse()?,
            minrtt_ms_p50: rec[&format!("r{}_minrtt_ms_p50", i)].parse()?,
            minrtt_ms_p50_ci_lb: rec[&format!("r{}_minrtt_ms_p50_ci_lb", i)].parse()?,
            minrtt_ms_p50_ci_ub: rec[&format!("r{}_minrtt_ms_p50_ci_ub", i)].parse()?,
            hdratio_num_samples: rec[&format!("r{}_hdratio_num_samples", i)].parse()?,
            minrtt_ms_p50_var: rec[&format!("r{}_minrtt_ms_p50_var", i)].parse()?,
            hdratio: rec[&format!("r{}_hdratio", i)].parse()?,
            hdratio_var: rec[&format!("r{}_hdratio_var", i)].parse()?,
            px_nexthops: string_to_u64(&rec[&format!("r{}_px_nexthops", i)]),
        }))
    }

    pub fn minrtt_median_diff_ci(rt1: &RouteInfo, rt2: &RouteInfo) -> (f32, f32, f32) {
        assert!(rt1.apm_route_num == 0, "Can only compute diff_ci on primary route.");
        let med1 = rt1.minrtt_ms_p50;
        let med2 = rt2.minrtt_ms_p50;
        let var1 = rt1.minrtt_ms_p50_var;
        let var2 = rt2.minrtt_ms_p50_var;
        let md: f32 = (med1 - med2) as f32;
        let interval = CONFIDENCE_Z * (var1 + var2).sqrt();
        (md - interval, md, md + interval)
    }

    pub fn hdratio_median_diff_ci(rt1: &RouteInfo, rt2: &RouteInfo) -> (f32, f32, f32) {
        assert!(rt1.apm_route_num == 0, "Can only compute diff_ci on primary route.");
        let diff: f32 = (rt1.hdratio - rt2.hdratio) as f32;
        let var1: f32 = rt1.hdratio_var;
        let var2: f32 = rt2.hdratio_var;
        let n1: f32 = rt1.hdratio_num_samples as f32;
        let n2: f32 = rt2.hdratio_num_samples as f32;
        let interval = CONFIDENCE_Z * (var1/n1 + var2/n2).sqrt();
        (diff - interval, diff, diff + interval)
    }

    pub fn compare_median_minrtt(rt1: &RouteInfo, rt2: &RouteInfo) -> Ordering {
        rt1.minrtt_ms_p50.cmp(&rt2.minrtt_ms_p50)
    }

    pub fn compare_median_hdratio(rt1: &RouteInfo, rt2: &RouteInfo) -> Ordering {
        rt1.hdratio.partial_cmp(&rt2.hdratio).unwrap_or(Ordering::Equal)
    }
}

impl MaxCiSizeValidator {
    pub fn new(median_minrtt_ci_ms: i16, average_hdratio_ci: f32) -> Self {
        MaxCiSizeValidator {
            median_minrtt_ci_ms, average_hdratio_ci,
        }
    }
}

impl RouteInfoValidator for MaxCiSizeValidator {
    fn check(&self, route: &RouteInfo) -> bool {
        let minrtt_ci: i16 = route.minrtt_ms_p50_ci_ub - route.minrtt_ms_p50_ci_lb;
        let root: f32 = (route.hdratio_var / route.hdratio_num_samples as f32).sqrt();
        let hdratio_ci: f32 = CONFIDENCE_Z * root;
        minrtt_ci < self.median_minrtt_ci_ms && hdratio_ci < self.average_hdratio_ci
    }
    fn describe(&self) -> String {
        format!("max-ci-{}-{:0.2}", self.median_minrtt_ci_ms, self.average_hdratio_ci)
    }
}

fn string_to_bool(s: &str) -> bool {
    ["ok", "Ok", "OK", "true", "True", "false", "False", "0", "1"].contains(&s)
}

fn string_to_u64(string: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    string.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peer_type_ord() {
        let transit = PeerType::Transit;
        let paid = PeerType::Peering(PeerSubtype::Paid);
        let public = PeerType::Peering(PeerSubtype::Public);
        let private = PeerType::Peering(PeerSubtype::Private);
        assert!(transit > paid);
        assert!(transit > public);
        assert!(transit > private);
        assert!(paid > public);
        assert!(paid > private);
        assert!(public > private);
    }
}
