use std::collections::HashMap;
// use std::cmp::{Eq, PartialEq, PartialOrd, Ord};

use num_traits::{FromPrimitive, ToPrimitive};
use num_derive::{FromPrimitive, ToPrimitive};
use ipnet::IpNet;
use log::{debug, info};

#[repr(u8)]
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub enum PeerSubtype {
    Private,
    Public,
    Paid,
}

#[repr(u8)]
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub enum PeerType {
    Peering(PeerSubtype),
    Transit,
}

fn parse_peer_type(peer_type: &str, peer_subtype: &str) -> Result<PeerType, Box<dyn std::error::Error>> {
    match (peer_type, peer_subtype) {
        ("peering", "mixed") => Ok(PeerType::Peering(PeerSubtype::Private)),
        ("peering", "private") => Ok(PeerType::Peering(PeerSubtype::Private)),
        ("peering", "public") => Ok(PeerType::Peering(PeerSubtype::Public)),
        ("route_server", "mixed") => Ok(PeerType::Peering(PeerSubtype::Public)),
        ("peering", "paid") => Ok(PeerType::Peering(PeerSubtype::Paid)),
        ("transit", "") => Ok(PeerType::Transit),
        (_, _) => Err(Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData,
        format!("Unknown peer tuple {} {}", peer_type, peer_subtype)))),
    }
}

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

pub struct DB {
    pub path2time2bin: HashMap<PathId, HashMap<u64, TimeBin>>,
    pub path2traffic: HashMap<PathId, u64>,
    pub total_traffic: u64,
    pub rows: u64,
    pub parsing_errors: u64,
}

impl DB {
    pub fn from_csv_reader<R: std::io::Read>(reader: &mut csv::Reader<R>) -> DB {
        let mut db = DB {
            path2time2bin: HashMap::new(),
            path2traffic: HashMap::new(),
            total_traffic: 0,
            rows: 0,
            parsing_errors: 0,
        };
        for result in reader.deserialize() {
            db.rows += 1;
            let record: HashMap<String, String> = result.unwrap();
            if (db.rows % 100_000) == 0 {
                info!("{} rows, {} parsing errors", db.rows, db.parsing_errors);
            }
            let pid = match PathId::from_record(&record) {
                Ok(p) => p,
                Err(_) => {
                    db.parsing_errors += 1;
                    continue;
                }
            };
            let timebin = match TimeBin::from_record(&record) {
                Ok(t) => t,
                Err(_) => {
                    db.parsing_errors += 1;
                    continue;
                }
            };
            let time2bin = db.path2time2bin.entry(pid).or_insert_with(HashMap::new);
            db.total_traffic += timebin.bytes_acked_sum;
            // path2traffic.entry(pid).update(|e| *e + 1);  // timebin.bytes_acked_sum);
            time2bin.insert(timebin.time_bucket, timebin);
        }
        db
    }
}

#[derive(PartialEq, Eq, Hash)]
pub struct PathId {
    vip_metro: String,
    bgp_ip_prefix: IpNet,
}

impl PathId {
    fn from_record(record: &HashMap<String, String>) -> Result<PathId, Box<dyn std::error::Error>> {
        if record["vip_metro"] == "NULL" {
            Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "vip_metro must not be NULL",
            )))
        } else {
            Ok(PathId {
                vip_metro: record["vip_metro"].to_string(),
                bgp_ip_prefix: record["bgp_ip_prefix"].parse::<IpNet>()?,
            })
        }
    }
}

const MAX_NUM_ROUTES: usize = 7;

pub struct TimeBin {
    time_bucket: u64,
    bytes_acked_sum: u64,
    num2route: [Option<RouteInfo>; MAX_NUM_ROUTES],
    route_parsing_errors: u8,
}

impl TimeBin {
    fn from_record(rec: &HashMap<String, String>) -> Result<TimeBin, Box<dyn std::error::Error>> {
        let mut bin = TimeBin { time_bucket: 0, bytes_acked_sum: 0, num2route: [None; 7], route_parsing_errors: 0 };
        bin.time_bucket = rec["time_bucket"].parse::<u64>()?;
        bin.bytes_acked_sum = rec["bytes_acked_sum"].parse::<u64>()?;
        for i in 0..=MAX_NUM_ROUTES {
            bin.num2route[i] = RouteInfo::from_record(i, rec).ok();
            if bin.num2route[i].is_none() {
                bin.route_parsing_errors += 1;
            }
        }
        Ok(bin)
    }
}

const MAX_NEXT_HOPS: usize = 4;

#[derive(Copy, Clone)]
pub struct RouteInfo<'a> {
    apm_route_num: u8,
    bgp_as_path_len: u8,
    bgp_as_path_min_len_prepending_removed: u8,
    bgp_as_path_prepending: bool,
    peer_type: PeerType,
    minrtt_num_samples: u32,
    minrtt_ms_p10: i16,
    minrtt_ms_p50: i16,
    minrtt_ms_p50_ci_lb: i16,
    minrtt_ms_p50_ci_ub: i16,
    hdratio_num_samples: u32,
    minrtt_ms_p50_var: f32,
    hdratio: f32,
    hdratio_var: f32,
    px_nexthops: &'a str,
}

impl RouteInfo {
    fn from_record(i: usize, rec: &HashMap<String, String>) -> Result<RouteInfo, Box<dyn std::error::Error>> {
        Ok(RouteInfo {
            apm_route_num: rec[&format!("r{}_apm_num_route", i)].parse()?,
            bgp_as_path_len: rec[&format!("r{}_bgp_as_path_len", i)].parse()?,
            bgp_as_path_min_len_prepending_removed: rec[&format!("r{}_bgp_as_path_min_len_prepending_removed", i)].parse()?,
            bgp_as_path_prepending: string_to_bool(&rec[&format!("r{}_bgp_as_path_prepending", i)]),
            peer_type: parse_peer_type(&rec[&format!("r{}_peer_type", i)],
            &rec[&format!("r{}_peer_subtype", i)])?,
            minrtt_num_samples: rec[&format!("r{}_minrtt_num_samples", i)].parse()?,
            minrtt_ms_p10: rec[&format!("r{}_minrtt_ms_p10", i)].parse()?,
            minrtt_ms_p50: rec[&format!("r{}_minrtt_ms_p50", i)].parse()?,
            minrtt_ms_p50_ci_lb: rec[&format!("r{}_minrtt_ms_p50_ci_lb", i)].parse()?,
            minrtt_ms_p50_ci_ub: rec[&format!("r{}_minrtt_ms_p50_ci_ub", i)].parse()?,
            hdratio_num_samples: rec[&format!("r{}_hdratio_num_samples", i)].parse()?,
            minrtt_ms_p50_var: rec[&format!("r{}_minrtt_ms_p50_var", i)].parse()?,
            hdratio: rec[&format!("r{}_hdratio", i)].parse()?,
            hdratio_var: rec[&format!("r{}_hdratio_var", i)].parse()?,
            px_nexthops: rec[&format!("r{}_px_nexthops", i)],
        })
    }
}

fn string_to_bool(s: &str) -> bool {
    ["ok", "Ok", "OK", "true", "True", "false", "False", "0", "1"].contains(&s)
}