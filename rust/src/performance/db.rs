use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::{btree_map, BTreeMap, HashMap};
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::BufReader;
use std::path::PathBuf;
use std::rc::Rc;

use flate2::bufread::GzDecoder;
use ipnet::IpNet;
use log::{debug, error, info};

mod error;
use error::{ParseError, ParseErrorKind};

const CONFIDENCE_Z: f32 = 2.0;

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

#[derive(Default)]
pub struct DB {
    pub pathid2time2bin: HashMap<Rc<PathId>, BTreeMap<u64, TimeBin>>,
    pub pathid2traffic: HashMap<Rc<PathId>, u64>,
    pub total_traffic: u64,
    pub rows: u64,
    error_counts: HashMap<ParseErrorKind, u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PathId {
    pub vip_metro: String,
    pub bgp_ip_prefix: IpNet,
}

#[derive(Clone, Debug)]
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
    pub minrtt_ms_p10: u16,
    pub minrtt_ms_p50: u16,
    pub minrtt_ms_p50_ci_halfwidth: u16,
    pub minrtt_ms_p50_var: f32,
    pub hdratio_num_samples: u32,
    pub hdratio: f32,
    pub hdratio_var: f32,
    pub px_nexthops: u64,
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
    pub fn from_file(input: &PathBuf) -> Result<DB, std::io::Error> {
        let f = File::open(input)?;
        let filerdr = BufReader::new(f);
        let gzrdr = GzDecoder::new(filerdr);
        let mut csvrdr = csv::ReaderBuilder::new().delimiter(b'\t').from_reader(gzrdr);
        let mut db = DB {
            pathid2time2bin: HashMap::new(),
            pathid2traffic: HashMap::new(),
            total_traffic: 0,
            rows: 0,
            error_counts: HashMap::new(),
        };
        for result in csvrdr.deserialize() {
            db.rows += 1;
            let record: HashMap<String, String> = result.unwrap();
            if (db.rows % 10000) == 0 {
                info!("{} rows", db.rows);
            }
            let pid: Rc<PathId> = match PathId::from_record(&record) {
                Ok(p) => Rc::new(p),
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
            db.pathid2traffic
                .entry(Rc::clone(&pid))
                .and_modify(|e| *e += timebin.bytes_acked_sum)
                .or_insert(timebin.bytes_acked_sum);
            let time2bin = db.pathid2time2bin.entry(Rc::clone(&pid)).or_insert_with(BTreeMap::new);
            match time2bin.entry(timebin.time_bucket) {
                btree_map::Entry::Vacant(e) => e.insert(timebin),
                btree_map::Entry::Occupied(_) => {
                    error!("TimeBin already exists, path {:?}, time {}", pid, &timebin.time_bucket);
                    debug!("{:?}", &record);
                    *db.error_counts.entry(ParseErrorKind::RepeatedTimebin).or_insert(0) += 1;
                    continue;
                }
            };
        }
        Ok(db)
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
    const MAX_ROUTES: usize = 7;

    fn from_record(rec: &HashMap<String, String>) -> Result<TimeBin, ParseError> {
        let mut timebin = TimeBin {
            time_bucket: rec["time_bucket"].parse::<u64>()?,
            bytes_acked_sum: rec["bytes_acked_sum"].parse::<u64>()?,
            num2route: Vec::with_capacity(TimeBin::MAX_ROUTES),
        };
        for i in 0..TimeBin::MAX_ROUTES {
            timebin.num2route.insert(i, RouteInfo::from_record(i, rec).ok());
        }
        Ok(timebin)
    }
    pub fn get_primary_route(&self) -> &Option<Box<RouteInfo>> {
        self.num2route.get(0).unwrap()
    }
    pub fn get_best_alternate<F>(&self, mut compare: F) -> &Option<Box<RouteInfo>>
    where
        F: FnMut(&RouteInfo, &RouteInfo) -> Ordering,
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
                            if compare(bestbox.borrow(), rtbox.borrow()) == Ordering::Greater {
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
        let minrtt_ms_p50_ci_lb: u32 = rec[&format!("r{}_minrtt_ms_p50_ci_lb", i)].parse()?;
        let minrtt_ms_p50_ci_ub: u32 = rec[&format!("r{}_minrtt_ms_p50_ci_ub", i)].parse()?;
        let minrtt_ms_p50_ci_halfwidth = ((minrtt_ms_p50_ci_ub - minrtt_ms_p50_ci_lb) / 2) as u16;
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
            minrtt_ms_p50_ci_halfwidth,
            minrtt_ms_p50_var: rec[&format!("r{}_minrtt_ms_p50_var", i)].parse()?,
            hdratio_num_samples: rec[&format!("r{}_hdratio_num_samples", i)].parse()?,
            hdratio: rec[&format!("r{}_hdratio", i)].parse()?,
            hdratio_var: rec[&format!("r{}_hdratio_var", i)].parse()?,
            px_nexthops: string_to_hash_u64(&rec[&format!("r{}_px_nexthops", i)]),
        }))
    }

    pub fn minrtt_median_diff_ci(rt1: &RouteInfo, rt2: &RouteInfo) -> (f32, f32) {
        let med1 = rt1.minrtt_ms_p50;
        let med2 = rt2.minrtt_ms_p50;
        let var1 = rt1.minrtt_ms_p50_var;
        let var2 = rt2.minrtt_ms_p50_var;
        let md: f32 = f32::from(med1) - f32::from(med2);
        let interval: f32 = CONFIDENCE_Z * (var1 + var2).sqrt();
        (md, interval)
    }

    pub fn hdratio_diff_ci(rt1: &RouteInfo, rt2: &RouteInfo) -> (f32, f32) {
        let diff: f32 = rt1.hdratio - rt2.hdratio;
        let var1: f32 = rt1.hdratio_var;
        let var2: f32 = rt2.hdratio_var;
        let n1: f32 = rt1.hdratio_num_samples as f32;
        let n2: f32 = rt2.hdratio_num_samples as f32;
        let interval: f32 = CONFIDENCE_Z * (var1 / n1 + var2 / n2).sqrt();
        (diff, interval)
    }

    pub fn compare_median_minrtt(rt1: &RouteInfo, rt2: &RouteInfo) -> Ordering {
        rt1.minrtt_ms_p50.cmp(&rt2.minrtt_ms_p50)
    }

    pub fn compare_hdratio(rt1: &RouteInfo, rt2: &RouteInfo) -> Ordering {
        rt1.hdratio.partial_cmp(&rt2.hdratio).unwrap_or(Ordering::Equal)
    }
}

fn string_to_bool(s: &str) -> bool {
    ["ok", "Ok", "OK", "true", "True", "false", "False", "0", "1"].contains(&s)
}

fn string_to_hash_u64(string: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    string.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    impl DB {
        pub fn insert(
            &mut self,
            pid: PathId,
            time2bin: BTreeMap<u64, TimeBin>,
        ) -> Option<BTreeMap<u64, TimeBin>> {
            let rcpid: Rc<PathId> = Rc::new(pid);
            let path_traffic: u64 = time2bin.values().fold(0u64, |acc, e| acc + e.bytes_acked_sum);
            self.total_traffic += path_traffic;
            if let Some(traffic) = self.pathid2traffic.insert(Rc::clone(&rcpid), path_traffic) {
                self.total_traffic -= traffic;
            }
            self.pathid2time2bin.insert(Rc::clone(&rcpid), time2bin)
        }
    }

    impl TimeBin {
        pub(crate) const MOCK_TOTAL_BYTES: u64 = 1000;

        pub(crate) fn mock_week_minrtt_p50(
            bin_duration_secs: u64,
            pri_minrtt_p50_even: u16,
            alt_minrtt_p50_even: u16,
            minrtt_p50_var_even: f32,
            pri_minrtt_p50_odd: u16,
            alt_minrtt_p50_odd: u16,
            minrtt_p50_var_odd: f32,
        ) -> BTreeMap<u64, TimeBin> {
            let mut time2bin: BTreeMap<u64, TimeBin> = BTreeMap::new();
            for time in (0..7 * 86400).step_by(bin_duration_secs as usize) {
                if time % (2 * bin_duration_secs) == 0 {
                    let timebin = TimeBin::mock_minrtt_p50(
                        time,
                        pri_minrtt_p50_even,
                        alt_minrtt_p50_even,
                        minrtt_p50_var_even,
                    );
                    time2bin.insert(time, timebin);
                } else {
                    let timebin = TimeBin::mock_minrtt_p50(
                        time,
                        pri_minrtt_p50_odd,
                        alt_minrtt_p50_odd,
                        minrtt_p50_var_odd,
                    );
                    time2bin.insert(time, timebin);
                }
            }
            time2bin
        }

        pub(crate) fn mock_minrtt_p50(
            time: u64,
            pri_minrtt_p50: u16,
            alt_minrtt_p50: u16,
            minrtt_ms_p50_var: f32,
        ) -> TimeBin {
            let mut timebin = TimeBin {
                time_bucket: time,
                bytes_acked_sum: TimeBin::MOCK_TOTAL_BYTES,
                num2route: vec![None; TimeBin::MAX_ROUTES],
            };
            let primary = RouteInfo::mock_minrtt_p50(1, pri_minrtt_p50, minrtt_ms_p50_var);
            let alternate = RouteInfo::mock_minrtt_p50(2, alt_minrtt_p50, minrtt_ms_p50_var);
            timebin.num2route[0] = Some(Box::new(primary));
            timebin.num2route[1] = Some(Box::new(alternate));
            timebin
        }

        pub(crate) fn mock_week_hdratio(
            bin_duration_secs: u64,
            pri_hdratio_even: f32,
            alt_hdratio_even: f32,
            hdratio_var_even: f32,
            pri_hdratio_odd: f32,
            alt_hdratio_odd: f32,
            hdratio_var_odd: f32,
        ) -> BTreeMap<u64, TimeBin> {
            let mut time2bin: BTreeMap<u64, TimeBin> = BTreeMap::new();
            for time in (0..7 * 86400).step_by(bin_duration_secs as usize) {
                if time % (2 * bin_duration_secs) == 0 {
                    let timebin = TimeBin::mock_hdratio(
                        time,
                        pri_hdratio_even,
                        alt_hdratio_even,
                        hdratio_var_even,
                    );
                    time2bin.insert(time, timebin);
                } else {
                    let timebin = TimeBin::mock_hdratio(
                        time,
                        pri_hdratio_odd,
                        alt_hdratio_odd,
                        hdratio_var_odd,
                    );
                    time2bin.insert(time, timebin);
                }
            }
            time2bin
        }

        pub(crate) fn mock_hdratio(
            time: u64,
            pri_hdratio: f32,
            alt_hdratio: f32,
            hdratio_var: f32,
        ) -> TimeBin {
            let mut timebin = TimeBin {
                time_bucket: time,
                bytes_acked_sum: TimeBin::MOCK_TOTAL_BYTES,
                num2route: vec![None; TimeBin::MAX_ROUTES],
            };
            let primary = RouteInfo::mock_hdratio(1, pri_hdratio, hdratio_var);
            let alternate = RouteInfo::mock_hdratio(2, alt_hdratio, hdratio_var);
            timebin.num2route[0] = Some(Box::new(primary));
            timebin.num2route[1] = Some(Box::new(alternate));
            timebin
        }
    }

    impl RouteInfo {
        const MOCK_NUM_SAMPLES: u32 = 100;

        pub(crate) fn mock_minrtt_p50(
            apm_route_num: u8,
            minrtt_ms_p50: u16,
            minrtt_ms_p50_var: f32,
        ) -> RouteInfo {
            RouteInfo {
                apm_route_num,
                bgp_as_path_len: 3,
                bgp_as_path_len_wo_prepend: 2,
                bgp_as_path_prepending: true,
                peer_type: PeerType::Transit,
                minrtt_num_samples: 200,
                minrtt_ms_p10: 10,
                minrtt_ms_p50,
                minrtt_ms_p50_ci_halfwidth: 1,
                minrtt_ms_p50_var,
                hdratio_num_samples: 200,
                hdratio: 0.9,
                hdratio_var: 0.01,
                px_nexthops: 1,
            }
        }

        pub(crate) fn mock_hdratio(apm_route_num: u8, hdratio: f32, hdratio_var: f32) -> RouteInfo {
            RouteInfo {
                apm_route_num,
                bgp_as_path_len: 3,
                bgp_as_path_len_wo_prepend: 2,
                bgp_as_path_prepending: true,
                peer_type: PeerType::Transit,
                minrtt_num_samples: RouteInfo::MOCK_NUM_SAMPLES,
                minrtt_ms_p10: 10,
                minrtt_ms_p50: 20,
                minrtt_ms_p50_ci_halfwidth: 1,
                minrtt_ms_p50_var: 10.0,
                hdratio_num_samples: RouteInfo::MOCK_NUM_SAMPLES,
                hdratio,
                hdratio_var,
                px_nexthops: 1,
            }
        }
    }

    #[test]
    fn test_db_insert() {
        let bin_duration_secs: u64 = 900;
        let mut database: DB = DB::default();

        let time2bin =
            TimeBin::mock_week_minrtt_p50(bin_duration_secs, 50, 51, 100.0, 50, 51, 100.0);
        let nbins: u64 = time2bin.len() as u64;

        let pid1 = PathId {
            vip_metro: "gru".to_string(),
            bgp_ip_prefix: "1.0.0.0/24".parse().unwrap(),
        };
        assert!(database.insert(pid1, time2bin).is_none());
        assert!(database.total_traffic == nbins * TimeBin::MOCK_TOTAL_BYTES);

        let time2bin =
            TimeBin::mock_week_minrtt_p50(bin_duration_secs, 50, 51, 100.0, 50, 51, 100.0);
        let pid2 = PathId {
            vip_metro: "gru".to_string(),
            bgp_ip_prefix: "2.0.0.0/24".parse().unwrap(),
        };
        assert!(database.insert(pid2, time2bin).is_none());
        assert!(database.total_traffic == 2 * nbins * TimeBin::MOCK_TOTAL_BYTES);

        let time2bin =
            TimeBin::mock_week_minrtt_p50(bin_duration_secs / 2, 50, 51, 100.0, 50, 51, 100.0);
        let pid2 = PathId {
            vip_metro: "gru".to_string(),
            bgp_ip_prefix: "2.0.0.0/24".parse().unwrap(),
        };
        assert!(database.insert(pid2, time2bin).is_some());
        assert!(database.total_traffic == 3 * nbins * TimeBin::MOCK_TOTAL_BYTES);
    }

    #[test]
    fn test_timebin_mock_week() {
        let bin_duration_secs: u64 = 900;
        let time2bin =
            TimeBin::mock_week_minrtt_p50(bin_duration_secs, 50, 51, 100.0, 50, 51, 100.0);
        assert!(time2bin.len() == (7 * 86400 / bin_duration_secs) as usize);
        assert!(time2bin.values().fold(true, |_, e| e.num2route[0].is_some()));
        assert!(time2bin.values().fold(true, |_, e| e.num2route[1].is_some()));
        assert!(time2bin.values().fold(true, |_, e| e.num2route[2].is_none()));
        assert!(*time2bin.keys().max().unwrap() == 7 * 86400 - bin_duration_secs);
    }

    #[test]
    fn test_get_best_alternate() {
        let pri_minrtt: u16 = 50;
        let alt1_minrtt: u16 = 100;
        let alt2_minrtt: u16 = 60;
        let minrtt_var: f32 = 100.0;
        let mut timebin: TimeBin = TimeBin::mock_minrtt_p50(0, pri_minrtt, alt1_minrtt, minrtt_var);
        let rtinfo = timebin.get_best_alternate(RouteInfo::compare_median_minrtt).as_ref().unwrap();
        assert!(rtinfo.minrtt_ms_p50 == alt1_minrtt);
        timebin.num2route[2] = Some(Box::new(RouteInfo::mock_minrtt_p50(3, 60, 100.0)));
        let rtinfo = timebin.get_best_alternate(RouteInfo::compare_median_minrtt).as_ref().unwrap();
        assert!(rtinfo.minrtt_ms_p50 == alt2_minrtt);
    }

    #[test]
    fn test_minrtt_median_diff_ci_small() {
        let pri_minrtt: u16 = 50;
        let alt_minrtt: u16 = 60;
        let minrtt_var: f32 = 2.0;
        let timebin: TimeBin = TimeBin::mock_minrtt_p50(0, pri_minrtt, alt_minrtt, minrtt_var);
        let pribox: &RouteInfo = timebin.get_primary_route().as_ref().unwrap();
        let altbox: &RouteInfo =
            timebin.get_best_alternate(RouteInfo::compare_median_minrtt).as_ref().unwrap();
        let (diff, halfwidth) = RouteInfo::minrtt_median_diff_ci(pribox, altbox);
        assert!((diff - (f32::from(pri_minrtt) - f32::from(alt_minrtt))).abs() < 1e-6);
        let interval: f32 = 2.0 * (minrtt_var + minrtt_var).sqrt();
        assert!((halfwidth - interval).abs() < 1e-6);
    }

    #[test]
    fn test_minrtt_median_diff_ci_large() {
        let pri_minrtt: u16 = 50;
        let alt_minrtt: u16 = 60;
        let minrtt_var: f32 = 100.0;
        let timebin: TimeBin = TimeBin::mock_minrtt_p50(0, pri_minrtt, alt_minrtt, minrtt_var);
        let pribox: &RouteInfo = timebin.get_primary_route().as_ref().unwrap();
        let altbox: &RouteInfo =
            timebin.get_best_alternate(RouteInfo::compare_median_minrtt).as_ref().unwrap();
        let (diff, halfwidth) = RouteInfo::minrtt_median_diff_ci(pribox, altbox);
        assert!((diff - (f32::from(pri_minrtt) - f32::from(alt_minrtt))).abs() < 1e-6);
        let interval: f32 = 2.0 * (minrtt_var + minrtt_var).sqrt();
        assert!((halfwidth - interval).abs() < 1e-6);
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

    #[test]
    #[ignore]
    fn test_load_db() -> Result<(), Box<dyn std::error::Error>> {
        let file = PathBuf::from("/home/cunha/data/FBPerformance/test/perf-3263.csv.gz");
        let db = DB::from_file(&file)?;
        assert!(db.rows == 365_909);
        Ok(())
    }
}
