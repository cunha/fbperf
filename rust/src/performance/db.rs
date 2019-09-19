use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::{btree_map, BTreeMap, HashMap};
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::BufReader;
use std::path::PathBuf;
use std::rc::Rc;
use std::str::FromStr;

use flate2::bufread::GzDecoder;
use ipnet::IpNet;
use log::info;
use num_enum::TryFromPrimitive;
use serde::Serialize;

mod error;
use error::{ParseError, ParseErrorKind};

const CONFIDENCE_Z: f32 = 2.0;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Ord, PartialOrd, TryFromPrimitive, Serialize)]
pub enum PeerType {
    PeeringPrivate = 0,
    PeeringPublic = 1,
    PeeringPaid = 2,
    Transit = 3,
    Uninitialized = 4,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, Ord, PartialOrd, TryFromPrimitive)]
pub enum ClientContinent {
    AF = 0,
    AS = 1,
    EU = 2,
    NA = 3,
    OC = 4,
    SA = 5,
    Unknown = 6,
    SIZE = 7,
}

#[derive(Default)]
pub struct DB {
    pub pathid2info: HashMap<Rc<PathId>, PathInfo>,
    pub rows: u32,
    pub total_bins: u32,
    pub total_traffic: u128,
    error_counts: HashMap<ParseErrorKind, u32>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PathId {
    pub vip_metro: String,
    pub bgp_ip_prefix: IpNet,
    pub client_continent: ClientContinent,
    pub client_country: [char; 2],
}

#[derive(Clone, Debug, Default)]
pub struct PathInfo {
    pub time2bin: BTreeMap<u64, TimeBin>,
    pub total_traffic: u128,
}

#[derive(Clone, Debug)]
pub struct TimeBin {
    pub time_bucket: u64,
    pub bytes_acked_sum: u64,
    pub num2route: [Option<Box<RouteInfo>>; TimeBin::MAX_ROUTES],
    // pub num2route: Vec<Option<Box<RouteInfo>>>,
}

#[derive(Clone, Copy, Debug)]
pub struct RouteInfo {
    pub apm_route_num: u8,
    pub bgp_as_path_len: u8,
    pub bgp_as_path_prepends: u8,
    // pub bgp_as_path_len_wo_prepend: u8,
    // pub bgp_as_path_prepending: bool,
    pub peer_type: PeerType,
    pub minrtt_num_samples: u32,
    // pub minrtt_ms_p10: u16,
    pub minrtt_ms_p50: u16,
    pub minrtt_ms_p50_ci_halfwidth: u16,
    // pub minrtt_ms_p50_var: f32,
    pub hdratio_num_samples: u32,
    pub hdratio: f32,
    pub hdratio_var: f32,
    pub hdratio_p50: f32,
    pub hdratio_p50_ci_halfwidth: f32,
    pub hdratio_boot: f32,
    pub r0_hdratio_boot_diff_ci_lb: f32,
    pub r0_hdratio_boot_diff_ci_ub: f32,
    pub px_nexthops: u64,
}

impl PeerType {
    fn new(peer_type: &str, peer_subtype: &str) -> Result<PeerType, ParseError> {
        match (peer_type, peer_subtype) {
            ("peering", "mixed") => Ok(PeerType::PeeringPrivate),
            ("peering", "private") => Ok(PeerType::PeeringPrivate),
            ("peering", "public") => Ok(PeerType::PeeringPublic),
            ("route_server", "mixed") => Ok(PeerType::PeeringPublic),
            ("peering", "paid") => Ok(PeerType::PeeringPaid),
            ("transit", "") => Ok(PeerType::Transit),
            (_, _) => Err(ParseError {
                kind: ParseErrorKind::UnknownPeeringRelationship,
                message: format!("peer_type: {}, peer_subtype: {}", peer_type, peer_subtype),
            }),
        }
    }
}

impl Default for PeerType {
    fn default() -> Self {
        PeerType::Uninitialized
    }
}

impl DB {
    pub fn from_file(input: &PathBuf, bin_duration_secs: u32) -> Result<DB, std::io::Error> {
        let f = File::open(input)?;
        let filerdr = BufReader::new(f);
        let gzrdr = GzDecoder::new(filerdr);
        let mut csvrdr = csv::ReaderBuilder::new().delimiter(b'\t').from_reader(gzrdr);
        let mut db = DB {
            pathid2info: HashMap::new(),
            total_bins: 0,
            total_traffic: 0,
            rows: 0,
            error_counts: HashMap::new(),
        };
        let mut min_timestamp: u64 = std::u64::MAX;
        let mut max_timestamp: u64 = 0;
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
            min_timestamp = std::cmp::min(min_timestamp, timebin.time_bucket);
            max_timestamp = std::cmp::max(max_timestamp, timebin.time_bucket);
            let mut pinfo = db.pathid2info.entry(Rc::clone(&pid)).or_insert_with(Default::default);
            pinfo.total_traffic += u128::from(timebin.bytes_acked_sum);
            db.total_traffic += u128::from(timebin.bytes_acked_sum);
            match pinfo.time2bin.entry(timebin.time_bucket) {
                btree_map::Entry::Vacant(e) => e.insert(timebin),
                btree_map::Entry::Occupied(_) => {
                    *db.error_counts.entry(ParseErrorKind::RepeatedTimebin).or_insert(0) += 1;
                    continue;
                }
            };
        }
        let seconds: u32 = (max_timestamp - min_timestamp) as u32;
        db.total_bins = seconds / bin_duration_secs;
        info!(
            "DB rows={} paths={} seconds={} bins={} bytes={}",
            db.rows,
            db.pathid2info.len(),
            seconds,
            db.total_bins,
            db.total_traffic
        );
        info!("{:?}", db.error_counts);
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
        } else if record["client_country"] == "NULL" {
            Err(ParseError {
                kind: ParseErrorKind::ClientCountryIsNull,
                message: "client_country must not be NULL".to_string(),
            })
        } else {
            let mut client_country: [char; 2] = ['a', 'a'];
            let mut chars = record["client_country"].chars();
            client_country[0] = chars.next().unwrap();
            client_country[1] = chars.next().unwrap();
            Ok(PathId {
                vip_metro: record["vip_metro"].to_string(),
                bgp_ip_prefix: record["bgp_ip_prefix"].parse::<IpNet>()?,
                client_continent: record["client_continent"].parse::<ClientContinent>().unwrap(),
                client_country,
            })
        }
    }
}

impl TimeBin {
    const MAX_ROUTES: usize = 7;

    fn from_record(rec: &HashMap<String, String>) -> Result<TimeBin, ParseError> {
        let mut timebin = TimeBin {
            time_bucket: rec["time_bucket"].parse::<u64>()?,
            bytes_acked_sum: rec["bytes_acked"].parse::<u64>()?,
            num2route: [None, None, None, None, None, None, None], // ; TimeBin::MAX_ROUTES],
                                                                   // Vec::with_capacity(TimeBin::MAX_ROUTES),
        };
        for i in 0..TimeBin::MAX_ROUTES {
            // timebin.num2route.insert(i, RouteInfo::from_record(i, rec).ok());
            timebin.num2route[i] = match RouteInfo::from_record(i, rec) {
                Ok(rtinfo) => Some(rtinfo),
                Err(e) => {
                    if i == 0 {
                        return Err(ParseError {
                            kind: ParseErrorKind::MissingPrimaryRoute,
                            message: "missing primary route".to_string(),
                        });
                    } else if e.kind == ParseErrorKind::NotEnoughMinRttSamples {
                        return Err(e);
                    } else {
                        None
                    }
                }
            };
        }
        Ok(timebin)
    }

    fn get_primary_route<F>(&self, check_valid: F) -> &Option<Box<RouteInfo>>
    where
        F: Fn(&RouteInfo) -> bool,
    {
        let optbox: &Option<Box<RouteInfo>> = self.num2route.get(0).unwrap();
        match &optbox {
            None => &None,
            Some(rtbox) => {
                if check_valid(rtbox.borrow()) {
                    optbox
                } else {
                    &None
                }
            }
        }
    }

    pub fn get_primary_route_minrtt(&self) -> &Option<Box<RouteInfo>> {
        self.get_primary_route(RouteInfo::minrtt_valid)
    }

    pub fn get_primary_route_hdratio(&self) -> &Option<Box<RouteInfo>> {
        self.get_primary_route(RouteInfo::hdratio_valid)
    }

    fn get_best_alternate<F, G>(&self, compare: F, check_valid: G) -> &Option<Box<RouteInfo>>
    where
        F: Fn(&RouteInfo, &RouteInfo) -> Ordering,
        G: Fn(&RouteInfo) -> bool,
    {
        let mut bestopt: &Option<Box<RouteInfo>> = &None;
        for rtopt in &self.num2route[1..] {
            match rtopt {
                None => continue,
                Some(ref rtbox) => {
                    // r0 in the trace is the preferred route; it may
                    // include multiple routes tied for best (which are
                    // ECMP'd across).  when getting the best alternate
                    // we *do* consider the individual components of r0
                    // for best.  uncommenting the `if` below ignores
                    // the components of r0 and only consider other
                    // routes as alternates.
                    // if rtbox.apm_route_num == 1 {
                    //     continue;
                    // }
                    if !check_valid(rtbox.borrow()) {
                        continue;
                    }
                    match bestopt {
                        None => bestopt = rtopt,
                        Some(ref bestbox) => {
                            if compare(rtbox.borrow(), bestbox.borrow()) == Ordering::Greater {
                                bestopt = rtopt;
                            }
                        }
                    }
                }
            }
        }
        bestopt
    }

    pub fn get_best_alternate_minrtt<F>(&self, compare: F) -> &Option<Box<RouteInfo>>
    where
        F: Fn(&RouteInfo, &RouteInfo) -> Ordering,
    {
        self.get_best_alternate(compare, RouteInfo::minrtt_valid)
    }

    pub fn get_best_alternate_hdratio<F>(&self, compare: F) -> &Option<Box<RouteInfo>>
    where
        F: Fn(&RouteInfo, &RouteInfo) -> Ordering,
    {
        self.get_best_alternate(compare, RouteInfo::hdratio_valid)
    }
}

impl RouteInfo {
    pub const MIN_SAMPLES: u32 = 30;

    fn from_record(i: usize, rec: &HashMap<String, String>) -> Result<Box<RouteInfo>, ParseError> {
        let apm_route_num: u8 = rec[&format!("r{}_apm_route_num", i)].parse()?;
        let minrtt_num_samples: u32 = rec[&format!("r{}_num_samples", i)].parse()?;
        if minrtt_num_samples < RouteInfo::MIN_SAMPLES {
            return Err(ParseError {
                kind: ParseErrorKind::NotEnoughMinRttSamples,
                message: "Not enough minrtt samples".to_string(),
            });
        }
        let hdratio_num_samples: u32 = rec[&format!("r{}_num_samples_with_hdratio", i)].parse()?;

        let mut hdratio_p50_ci_halfwidth: f32 = 0.0;
        let mut hdratio: f32 = 0.0;
        let mut hdratio_var: f32 = 0.0;
        let mut hdratio_p50: f32 = 0.0;
        let mut hdratio_boot: f32 = 0.0;
        let mut r0_hdratio_boot_diff_ci_lb: f32 = 0.0;
        let mut r0_hdratio_boot_diff_ci_ub: f32 = 0.0;
        if hdratio_num_samples > RouteInfo::MIN_SAMPLES {
            let hdratio_p50_ci_lb: f32 = rec[&format!("r{}_hdratio_p50_ci_lb", i)].parse().unwrap();
            let hdratio_p50_ci_ub: f32 = rec[&format!("r{}_hdratio_p50_ci_ub", i)].parse().unwrap();
            hdratio_p50_ci_halfwidth = (hdratio_p50_ci_ub - hdratio_p50_ci_lb) / 2.0;
            hdratio = rec[&format!("r{}_hdratio_avg", i)].parse().unwrap();
            hdratio_var = rec[&format!("r{}_hdratio_normal_var", i)].parse().unwrap();
            hdratio_p50 = rec[&format!("r{}_hdratio_p50", i)].parse().unwrap();
            hdratio_boot = rec[&format!("r{}_hdratio_avg_bootstrapped", i)].parse().unwrap();
            if i > 0 {
                r0_hdratio_boot_diff_ci_lb = rec
                    [&format!("r{}_r0_diff_hdratio_avg_bootstrapped_ci_lb", i)]
                    .parse::<f32>()
                    .unwrap_or_default();
                r0_hdratio_boot_diff_ci_ub = rec
                    [&format!("r{}_r0_diff_hdratio_avg_bootstrapped_ci_ub", i)]
                    .parse::<f32>()
                    .unwrap_or_default();
                assert!(r0_hdratio_boot_diff_ci_ub >= r0_hdratio_boot_diff_ci_lb);
            }
        }

        let minrtt_ms_p50_ci_lb: f32 = rec[&format!("r{}_minrtt_ms_p50_ci_lb", i)].parse().unwrap();
        let minrtt_ms_p50_ci_ub: f32 = rec[&format!("r{}_minrtt_ms_p50_ci_ub", i)].parse().unwrap();
        let minrtt_ms_p50_ci_halfwidth = ((minrtt_ms_p50_ci_ub - minrtt_ms_p50_ci_lb) / 2.0) as u16;

        let bgp_as_path_len: u8 = rec[&format!("r{}_bgp_as_path_len", i)].parse().unwrap();
        let bgp_as_path_wo_prepend: u8 =
            rec[&format!("r{}_bgp_as_path_min_len_prepending_removed", i)].parse().unwrap();

        Ok(Box::new(RouteInfo {
            apm_route_num,
            bgp_as_path_len,
            bgp_as_path_prepends: bgp_as_path_len - bgp_as_path_wo_prepend,
            peer_type: PeerType::new(
                &rec[&format!("r{}_peer_type", i)],
                &rec[&format!("r{}_peer_subtype", i)],
            )?,
            minrtt_num_samples,
            // minrtt_ms_p10: rec[&format!("r{}_minrtt_ms_p10", i)].parse().unwrap(),
            minrtt_ms_p50: rec[&format!("r{}_minrtt_ms_p50", i)].parse::<f32>().unwrap() as u16,
            minrtt_ms_p50_ci_halfwidth,
            // minrtt_ms_p50_var: rec[&format!("r{}_minrtt_ms_p50_var", i)].parse().unwrap(),
            hdratio_num_samples,
            hdratio,
            hdratio_var,
            hdratio_p50,
            hdratio_p50_ci_halfwidth,
            hdratio_boot,
            r0_hdratio_boot_diff_ci_lb,
            r0_hdratio_boot_diff_ci_ub,
            px_nexthops: string_to_hash_u64(&rec[&format!("r{}_px_nexthops", i)]),
        }))
    }

    pub fn minrtt_median_diff_ci(rt1: &RouteInfo, rt2: &RouteInfo) -> (f32, f32) {
        let med1 = rt1.minrtt_ms_p50;
        let med2 = rt2.minrtt_ms_p50;
        let var1 = (f32::from(rt1.minrtt_ms_p50_ci_halfwidth) / CONFIDENCE_Z).powf(2.0);
        let var2 = (f32::from(rt2.minrtt_ms_p50_ci_halfwidth) / CONFIDENCE_Z).powf(2.0);
        let md: f32 = f32::from(med1) - f32::from(med2);
        let halfwidth: f32 = CONFIDENCE_Z * (var1 + var2).sqrt();
        (md, halfwidth)
    }

    pub fn hdratio_median_diff_ci(rt1: &RouteInfo, rt2: &RouteInfo) -> (f32, f32) {
        let med1 = rt1.hdratio_p50;
        let med2 = rt2.hdratio_p50;
        let var1 = (rt1.hdratio_p50_ci_halfwidth / CONFIDENCE_Z).powf(2.0);
        let var2 = (rt2.hdratio_p50_ci_halfwidth / CONFIDENCE_Z).powf(2.0);
        let md: f32 = med1 - med2;
        let halfwidth: f32 = CONFIDENCE_Z * (var1 + var2).sqrt();
        (md, halfwidth)
    }

    pub fn hdratio_diff_ci_do_not_use(rt1: &RouteInfo, rt2: &RouteInfo) -> (f32, f32) {
        let diff: f32 = rt1.hdratio - rt2.hdratio;
        let var1: f32 = rt1.hdratio_var;
        let var2: f32 = rt2.hdratio_var;
        let n1: f32 = rt1.hdratio_num_samples as f32;
        let n2: f32 = rt2.hdratio_num_samples as f32;
        let halfwidth: f32 = CONFIDENCE_Z * (var1 / n1 + var2 / n2).sqrt();
        (diff, halfwidth)
    }

    pub fn hdratio_boot_diff_ci(bestalt: &RouteInfo, primary: &RouteInfo) -> (f32, f32, f32) {
        let mut diff: f32 = bestalt.hdratio_boot - primary.hdratio_boot;
        assert!(primary.r0_hdratio_boot_diff_ci_lb == 0.0);
        assert!(primary.r0_hdratio_boot_diff_ci_ub == 0.0);
        diff = f32::max(diff, bestalt.r0_hdratio_boot_diff_ci_lb);
        diff = f32::min(diff, bestalt.r0_hdratio_boot_diff_ci_ub);
        (bestalt.r0_hdratio_boot_diff_ci_lb, diff, bestalt.r0_hdratio_boot_diff_ci_ub)
    }

    pub fn compare_median_minrtt(rt1: &RouteInfo, rt2: &RouteInfo) -> Ordering {
        // Return Greater if rt1.minrtt_ms_p50 < rt2.minrtt_ms_p50
        rt2.minrtt_ms_p50.cmp(&rt1.minrtt_ms_p50)
        // rt1.minrtt_ms_p50.cmp(&rt2.minrtt_ms_p50)
    }

    pub fn compare_median_hdratio(rt1: &RouteInfo, rt2: &RouteInfo) -> Ordering {
        // Return Greater if rt1.hdratio_p50 > rt2.hdratio_p50
        rt1.hdratio_p50.partial_cmp(&rt2.hdratio_p50).unwrap_or(Ordering::Equal)
    }

    pub fn compare_hdratio(rt1: &RouteInfo, rt2: &RouteInfo) -> Ordering {
        rt1.hdratio.partial_cmp(&rt2.hdratio).unwrap_or(Ordering::Equal)
    }

    pub fn compare_hdratio_bootstrap(rt1: &RouteInfo, rt2: &RouteInfo) -> Ordering {
        rt1.hdratio_boot.partial_cmp(&rt2.hdratio_boot).unwrap_or(Ordering::Equal)
    }

    pub fn minrtt_valid(rtinfo: &RouteInfo) -> bool {
        rtinfo.minrtt_num_samples >= RouteInfo::MIN_SAMPLES
    }

    pub fn hdratio_valid(rtinfo: &RouteInfo) -> bool {
        rtinfo.hdratio_num_samples >= RouteInfo::MIN_SAMPLES
    }
}

impl FromStr for ClientContinent {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "AF" => Ok(ClientContinent::AF),
            "AS" => Ok(ClientContinent::AS),
            "EU" => Ok(ClientContinent::EU),
            "NA" => Ok(ClientContinent::NA),
            "OC" => Ok(ClientContinent::OC),
            "SA" => Ok(ClientContinent::SA),
            _ => Ok(ClientContinent::Unknown),
        }
    }
}

// fn string_to_bool(s: &str) -> bool {
//     ["ok", "Ok", "OK", "true", "True", "false", "False", "0", "1"].contains(&s)
// }

fn string_to_hash_u64(string: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    string.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    const BIN_DURATION_SECS: u64 = 900;

    pub fn make_path_id() -> PathId {
        PathId {
            vip_metro: String::from("gru"),
            bgp_ip_prefix: "1.0.0.0/24".parse().unwrap(),
            client_continent: ClientContinent::Unknown,
            client_country: ['B', 'R'],
        }
    }

    impl DB {
        pub fn insert(
            &mut self,
            pid: PathId,
            time2bin: BTreeMap<u64, TimeBin>,
        ) -> Option<BTreeMap<u64, TimeBin>> {
            let rcpid: Rc<PathId> = Rc::new(pid);
            let path_traffic: u128 =
                time2bin.values().fold(0u128, |acc, e| acc + u128::from(e.bytes_acked_sum));
            self.total_bins = std::cmp::max(self.total_bins, time2bin.len() as u32);
            self.total_traffic += path_traffic;
            let pinfo: PathInfo = PathInfo {
                time2bin,
                total_traffic: path_traffic,
            };
            if let Some(oldinfo) = self.pathid2info.insert(Rc::clone(&rcpid), pinfo) {
                self.total_traffic -= oldinfo.total_traffic;
                Some(oldinfo.time2bin)
            } else {
                None
            }
        }
    }

    impl TimeBin {
        pub(crate) const MOCK_TOTAL_BYTES: u64 = 1000;

        pub(crate) fn mock_week_minrtt_p50(
            bin_duration_secs: u64,
            pri_minrtt_p50_even: u16,
            alt_minrtt_p50_even: u16,
            minrtt_p50_ci_halfwidth_even: u16,
            pri_minrtt_p50_odd: u16,
            alt_minrtt_p50_odd: u16,
            minrtt_p50_ci_halfwidth_odd: u16,
        ) -> BTreeMap<u64, TimeBin> {
            let mut time2bin: BTreeMap<u64, TimeBin> = BTreeMap::new();
            for time in (0..7 * 86400).step_by(bin_duration_secs as usize) {
                if time % (2 * bin_duration_secs) == 0 {
                    let timebin = TimeBin::mock_minrtt_p50(
                        time,
                        pri_minrtt_p50_even,
                        alt_minrtt_p50_even,
                        minrtt_p50_ci_halfwidth_even,
                    );
                    time2bin.insert(time, timebin);
                } else {
                    let timebin = TimeBin::mock_minrtt_p50(
                        time,
                        pri_minrtt_p50_odd,
                        alt_minrtt_p50_odd,
                        minrtt_p50_ci_halfwidth_odd,
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
            minrtt_p50_ci_halfwidth: u16,
        ) -> TimeBin {
            let mut timebin = TimeBin {
                time_bucket: time,
                bytes_acked_sum: TimeBin::MOCK_TOTAL_BYTES,
                num2route: [None, None, None, None, None, None, None],
            };
            let primary = RouteInfo::mock_minrtt_p50(1, pri_minrtt_p50, minrtt_p50_ci_halfwidth);
            let alternate = RouteInfo::mock_minrtt_p50(2, alt_minrtt_p50, minrtt_p50_ci_halfwidth);
            timebin.num2route[0] = Some(Box::new(primary));
            timebin.num2route[1] = Some(Box::new(alternate));
            timebin
        }

        pub(crate) fn mock_week_hdratio_p50(
            bin_duration_secs: u64,
            pri_hdratio50_even: f32,
            alt_hdratio50_even: f32,
            hdratio50_ci_halfwidth_even: f32,
            pri_hdratio50_odd: f32,
            alt_hdratio50_odd: f32,
            hdratio50_ci_halfwidth_odd: f32,
        ) -> BTreeMap<u64, TimeBin> {
            let mut time2bin: BTreeMap<u64, TimeBin> = BTreeMap::new();
            for time in (0..7 * 86400).step_by(bin_duration_secs as usize) {
                if time % (2 * bin_duration_secs) == 0 {
                    let timebin = TimeBin::mock_hdratio_p50(
                        time,
                        pri_hdratio50_even,
                        alt_hdratio50_even,
                        hdratio50_ci_halfwidth_even,
                    );
                    time2bin.insert(time, timebin);
                } else {
                    let timebin = TimeBin::mock_hdratio_p50(
                        time,
                        pri_hdratio50_odd,
                        alt_hdratio50_odd,
                        hdratio50_ci_halfwidth_odd,
                    );
                    time2bin.insert(time, timebin);
                }
            }
            time2bin
        }

        pub(crate) fn mock_hdratio_p50(
            time: u64,
            pri_hdratio_p50: f32,
            alt_hdratio_p50: f32,
            hdratio_p50_ci_halfwidth: f32,
        ) -> TimeBin {
            let mut timebin = TimeBin {
                time_bucket: time,
                bytes_acked_sum: TimeBin::MOCK_TOTAL_BYTES,
                num2route: [None, None, None, None, None, None, None],
            };
            let primary = RouteInfo::mock_hdratio_p50(1, pri_hdratio_p50, hdratio_p50_ci_halfwidth);
            let alternate =
                RouteInfo::mock_hdratio_p50(2, alt_hdratio_p50, hdratio_p50_ci_halfwidth);
            timebin.num2route[0] = Some(Box::new(primary));
            timebin.num2route[1] = Some(Box::new(alternate));
            timebin
        }

        pub(crate) fn mock_hdratio_boot(
            time: u64,
            pri_hdratio_boot: f32,
            alt_hdratio_boot: f32,
            hdratio_boot_diff_ci_lb: f32,
            hdratio_boot_diff_ci_ub: f32,
        ) -> TimeBin {
            let mut timebin = TimeBin {
                time_bucket: time,
                bytes_acked_sum: TimeBin::MOCK_TOTAL_BYTES,
                num2route: [None, None, None, None, None, None, None],
            };
            let primary = RouteInfo::mock_hdratio_boot(1, pri_hdratio_boot, 0.0, 0.0);
            let alternate = RouteInfo::mock_hdratio_boot(
                2,
                alt_hdratio_boot,
                hdratio_boot_diff_ci_lb,
                hdratio_boot_diff_ci_ub,
            );
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
            minrtt_ms_p50_ci_halfwidth: u16,
        ) -> RouteInfo {
            RouteInfo {
                apm_route_num,
                bgp_as_path_len: 3,
                bgp_as_path_prepends: 1,
                peer_type: PeerType::Transit,
                minrtt_num_samples: 200,
                minrtt_ms_p50,
                minrtt_ms_p50_ci_halfwidth,
                hdratio_num_samples: 200,
                hdratio: 0.9,
                hdratio_var: 0.01,
                hdratio_p50: 1.0,
                hdratio_p50_ci_halfwidth: 0.01,
                hdratio_boot: 0.9,
                r0_hdratio_boot_diff_ci_lb: 0.85,
                r0_hdratio_boot_diff_ci_ub: 0.95,
                px_nexthops: 1,
            }
        }

        pub(crate) fn mock_hdratio_p50(
            apm_route_num: u8,
            hdratio_p50: f32,
            hdratio_p50_ci_halfwidth: f32,
        ) -> RouteInfo {
            RouteInfo {
                apm_route_num,
                bgp_as_path_len: 3,
                bgp_as_path_prepends: 1,
                peer_type: PeerType::Transit,
                minrtt_num_samples: RouteInfo::MOCK_NUM_SAMPLES,
                minrtt_ms_p50: 20,
                minrtt_ms_p50_ci_halfwidth: 1,
                hdratio_num_samples: RouteInfo::MOCK_NUM_SAMPLES,
                hdratio: 0.9,
                hdratio_var: 0.2,
                hdratio_p50,
                hdratio_p50_ci_halfwidth,
                hdratio_boot: 0.9,
                r0_hdratio_boot_diff_ci_lb: 0.85,
                r0_hdratio_boot_diff_ci_ub: 0.95,
                px_nexthops: 1,
            }
        }

        pub(crate) fn mock_hdratio_boot(
            apm_route_num: u8,
            hdratio_boot: f32,
            ci_lb: f32,
            ci_ub: f32,
        ) -> RouteInfo {
            RouteInfo {
                apm_route_num,
                bgp_as_path_len: 3,
                bgp_as_path_prepends: 1,
                peer_type: PeerType::Transit,
                minrtt_num_samples: RouteInfo::MOCK_NUM_SAMPLES,
                minrtt_ms_p50: 20,
                minrtt_ms_p50_ci_halfwidth: 1,
                hdratio_num_samples: RouteInfo::MOCK_NUM_SAMPLES,
                hdratio: 0.9,
                hdratio_var: 0.1,
                hdratio_p50: 1.0,
                hdratio_p50_ci_halfwidth: 0.01,
                hdratio_boot,
                r0_hdratio_boot_diff_ci_lb: ci_lb,
                r0_hdratio_boot_diff_ci_ub: ci_ub,
                px_nexthops: 1,
            }
        }
    }

    #[test]
    fn test_db_insert() {
        let mut database: DB = DB::default();

        let time2bin = TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 50, 51, 100, 50, 51, 100);
        let nbins: u64 = time2bin.len() as u64;

        let pid1 = make_path_id();
        assert!(database.insert(pid1, time2bin).is_none());
        assert!(database.total_traffic == u128::from(nbins * TimeBin::MOCK_TOTAL_BYTES));

        let time2bin = TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 50, 51, 100, 50, 51, 100);
        let pid2 = PathId {
            vip_metro: "gru".to_string(),
            bgp_ip_prefix: "2.0.0.0/24".parse().unwrap(),
            client_continent: ClientContinent::Unknown,
            client_country: ['B', 'R'],
        };
        assert!(database.insert(pid2, time2bin).is_none());
        assert!(database.total_traffic == u128::from(2 * nbins * TimeBin::MOCK_TOTAL_BYTES));

        let time2bin =
            TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS / 2, 50, 51, 100, 50, 51, 100);
        let pid2 = PathId {
            vip_metro: "gru".to_string(),
            bgp_ip_prefix: "2.0.0.0/24".parse().unwrap(),
            client_continent: ClientContinent::Unknown,
            client_country: ['B', 'R'],
        };
        assert!(database.insert(pid2, time2bin).is_some());
        assert!(database.total_traffic == u128::from(3 * nbins * TimeBin::MOCK_TOTAL_BYTES));
    }

    #[test]
    fn test_timebin_mock_week() {
        let time2bin = TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 50, 51, 100, 50, 51, 100);
        assert!(time2bin.len() == (7 * 86400 / BIN_DURATION_SECS) as usize);
        assert!(time2bin.values().fold(true, |_, e| e.num2route[0].is_some()));
        assert!(time2bin.values().fold(true, |_, e| e.num2route[1].is_some()));
        assert!(time2bin.values().fold(true, |_, e| e.num2route[2].is_none()));
        assert!(*time2bin.keys().max().unwrap() == 7 * 86400 - BIN_DURATION_SECS);
    }

    #[test]
    fn test_get_best_alternate() {
        let pri_minrtt: u16 = 50;
        let alt1_minrtt: u16 = 100;
        let alt2_minrtt: u16 = 60;
        let minrtt_ci_halfwidth: u16 = 100;
        let mut timebin: TimeBin =
            TimeBin::mock_minrtt_p50(0, pri_minrtt, alt1_minrtt, minrtt_ci_halfwidth);
        let rtinfo =
            timebin.get_best_alternate_minrtt(RouteInfo::compare_median_minrtt).as_ref().unwrap();
        assert!(rtinfo.minrtt_ms_p50 == alt1_minrtt);
        timebin.num2route[2] = Some(Box::new(RouteInfo::mock_minrtt_p50(3, 60, 100)));
        let rtinfo =
            timebin.get_best_alternate_minrtt(RouteInfo::compare_median_minrtt).as_ref().unwrap();
        assert!(rtinfo.minrtt_ms_p50 == alt2_minrtt);

        let pri_hdratio_boot: f32 = 0.9;
        let alt1_hdratio_boot: f32 = 0.8;
        let alt2_hdratio_boot: f32 = 0.95;
        let alt1_hdratio_boot_diff_ci_lb: f32 = -0.15;
        let alt1_hdratio_boot_diff_ci_ub: f32 = -0.05;
        let alt2_hdratio_boot_diff_ci_lb: f32 = 0.0;
        let alt2_hdratio_boot_diff_ci_ub: f32 = 0.1;
        let mut timebin: TimeBin = TimeBin::mock_hdratio_boot(
            0,
            pri_hdratio_boot,
            alt1_hdratio_boot,
            alt1_hdratio_boot_diff_ci_lb,
            alt1_hdratio_boot_diff_ci_ub,
        );
        let rtinfo = timebin
            .get_best_alternate_hdratio(RouteInfo::compare_hdratio_bootstrap)
            .as_ref()
            .unwrap();
        assert!((rtinfo.hdratio_boot - alt1_hdratio_boot).abs() < 1e-6);
        timebin.num2route[2] = Some(Box::new(RouteInfo::mock_hdratio_boot(
            3,
            alt2_hdratio_boot,
            alt2_hdratio_boot_diff_ci_lb,
            alt2_hdratio_boot_diff_ci_ub,
        )));
        let rtinfo = timebin
            .get_best_alternate_hdratio(RouteInfo::compare_hdratio_bootstrap)
            .as_ref()
            .unwrap();
        assert!((rtinfo.hdratio_boot - alt2_hdratio_boot).abs() < 1e-6);
    }

    #[test]
    fn test_minrtt_median_diff_ci_small() {
        let pri_minrtt: u16 = 50;
        let alt_minrtt: u16 = 60;
        let minrtt_ci_halfwidth: u16 = 2;
        let timebin: TimeBin =
            TimeBin::mock_minrtt_p50(0, pri_minrtt, alt_minrtt, minrtt_ci_halfwidth);
        let pribox: &RouteInfo = timebin.get_primary_route_minrtt().as_ref().unwrap();
        let altbox: &RouteInfo =
            timebin.get_best_alternate_minrtt(RouteInfo::compare_median_minrtt).as_ref().unwrap();
        let (diff, halfwidth) = RouteInfo::minrtt_median_diff_ci(pribox, altbox);
        assert!((diff - (f32::from(pri_minrtt) - f32::from(alt_minrtt))).abs() < 1e-6);
        let var: f32 = (f32::from(minrtt_ci_halfwidth) / 2.0).powf(2.0);
        let interval: f32 = 2.0 * (var + var).sqrt();
        assert!((halfwidth - interval).abs() < 1e-6);
    }

    #[test]
    fn test_minrtt_median_diff_ci_large() {
        let pri_minrtt: u16 = 50;
        let alt_minrtt: u16 = 60;
        let minrtt_ci_halfwidth: u16 = 100;
        let timebin: TimeBin =
            TimeBin::mock_minrtt_p50(0, pri_minrtt, alt_minrtt, minrtt_ci_halfwidth);
        let pribox: &RouteInfo = timebin.get_primary_route_minrtt().as_ref().unwrap();
        let altbox: &RouteInfo =
            timebin.get_best_alternate_minrtt(RouteInfo::compare_median_minrtt).as_ref().unwrap();
        let (diff, halfwidth) = RouteInfo::minrtt_median_diff_ci(pribox, altbox);
        assert!((diff - (f32::from(pri_minrtt) - f32::from(alt_minrtt))).abs() < 1e-6);
        let var: f32 = (f32::from(minrtt_ci_halfwidth) / 2.0).powf(2.0);
        let interval: f32 = 2.0 * (var + var).sqrt();
        assert!((halfwidth - interval).abs() < 1e-6);
    }

    #[test]
    fn test_hdratio_boot_diff_ci_small() {
        let pri_hdratio_boot: f32 = 0.9;
        let alt_hdratio_boot: f32 = 0.8;
        let hdratio_boot_diff_ci_lb: f32 = -0.15;
        let hdratio_boot_diff_ci_ub: f32 = -0.05;
        let timebin: TimeBin = TimeBin::mock_hdratio_boot(
            0,
            pri_hdratio_boot,
            alt_hdratio_boot,
            hdratio_boot_diff_ci_lb,
            hdratio_boot_diff_ci_ub,
        );
        let pribox: &RouteInfo = timebin.get_primary_route_hdratio().as_ref().unwrap();
        let altbox: &RouteInfo = timebin
            .get_best_alternate_hdratio(RouteInfo::compare_hdratio_bootstrap)
            .as_ref()
            .unwrap();
        let (lb, diff, ub) = RouteInfo::hdratio_boot_diff_ci(altbox, pribox);
        assert!((diff - (alt_hdratio_boot - pri_hdratio_boot)).abs() < 1e-6);
        assert!(lb <= ub);
        assert!(lb <= diff);
        assert!(diff <= ub);

        let pri_hdratio_boot: f32 = 0.8;
        let alt_hdratio_boot: f32 = 0.9;
        let hdratio_boot_diff_ci_lb: f32 = 0.05;
        let hdratio_boot_diff_ci_ub: f32 = 0.15;
        let timebin: TimeBin = TimeBin::mock_hdratio_boot(
            0,
            pri_hdratio_boot,
            alt_hdratio_boot,
            hdratio_boot_diff_ci_lb,
            hdratio_boot_diff_ci_ub,
        );
        let pribox: &RouteInfo = timebin.get_primary_route_hdratio().as_ref().unwrap();
        let altbox: &RouteInfo = timebin
            .get_best_alternate_hdratio(RouteInfo::compare_hdratio_bootstrap)
            .as_ref()
            .unwrap();
        let (lb, diff, ub) = RouteInfo::hdratio_boot_diff_ci(altbox, pribox);
        assert!((diff - (alt_hdratio_boot - pri_hdratio_boot)).abs() < 1e-6);
        assert!(lb <= ub);
        assert!(lb <= diff);
        assert!(diff <= ub);
    }

    #[test]
    fn test_peer_type_ord() {
        let transit = PeerType::Transit;
        let paid = PeerType::PeeringPaid;
        let public = PeerType::PeeringPublic;
        let private = PeerType::PeeringPrivate;
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
        let db = DB::from_file(&file, BIN_DURATION_SECS as u32)?;
        assert!(db.rows == 365_909);
        Ok(())
    }
}
