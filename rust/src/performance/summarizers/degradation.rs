use std::collections::HashMap;
use std::rc::Rc;

use crate::performance::db::{PathId, RouteInfo, TimeBin, DB};
use crate::performance::perfstats::{TimeBinStats, TimeBinSummarizer};

/// Summarize MinRTT degradation over time comparing primary routes.
///
/// This struct requires initialization using through a call to `new`,
/// which will find the "best" `TimeBin` for each `PathId` in the
/// dataset. Thresholds below control filters applied to the algorithm
/// to find the best `TimeBin`.
pub struct MinRtt50LowerBoundDegradationSummarizer {
    /// The minimum difference between the best `TimeBin` and other
    /// `TimeBin`s considered degradation.
    min_diff_degradation: u16,
    /// The maximum CI halfwidth of the performance difference between
    /// the best `TimeBin` and other `TimeBin`s. `TimeBin` comparisons
    /// whose CI halfwidth is above this threshold will not be
    /// considered valid. (This parameter is used after initialization,
    /// when computing degradation.)
    max_diff_ci_halfwidth: f32,
    /// During initialization, `TimeBin`s whose primary route's MinRTT
    /// P50 variance is above this threshold will not be considered for
    /// "best". (These `TimeBin`s will still be considered in
    /// comparisons later; if their primary routes have better
    /// performance than that of the `TimeBin` chosen for best, we say
    /// there is no degradation.) This parameter allows ignoring primary
    /// routes with large MinRTT P50 variance, which could lead to later
    /// comparisons having large CI halfwidths (and filtering due to the
    /// CI halfwidth threshold).
    max_minrtt50_var: f32,
    /// This stores the primary `RouteInfo` for the best `TimeBin` for
    /// each `PathId`, chosen based on the thresholds above. `PathId`s
    /// without a valid best `TimeBin` are not included in the mapping.
    pathid2bestroute: HashMap<Rc<PathId>, RouteInfo>,
}

/// Summarize MinRTT degradation over time, comparing only identical
/// primary routes.
///
/// We consider two primary routes identical if their `RouteInfo`
/// instances have the same `px_nexthops`.
///
/// This struct requires initialization using through a call to `new`,
/// which will find the "best" `TimeBin` for each `PathId` in the
/// dataset. Thresholds below control filters applied to the algorithm
/// to find the best `TimeBin`.
pub struct MinRtt50LowerBoundDistinctPathsDegradationSummarizer {
    /// The minimum difference between the best `TimeBin` and other
    /// `TimeBin`s considered degradation.
    min_diff_degradation: u16,
    /// The maximum CI halfwidth of the performance difference between
    /// the best `TimeBin` and other `TimeBin`s. `TimeBin` comparisons
    /// whose CI halfwidth is above this threshold will not be
    /// considered valid. (This parameter is used after initialization,
    /// when computing degradation.)
    max_diff_ci_halfwidth: f32,
    /// During initialization, `TimeBin`s whose primary route's MinRTT
    /// P50 variance is above this threshold will not be considered for
    /// "best". (These `TimeBin`s will still be considered in
    /// comparisons later; if their primary routes have better
    /// performance than that of the `TimeBin` chosen for best, we say
    /// there is no degradation.) This parameter allows ignoring primary
    /// routes with large MinRTT P50 variance, which could lead to later
    /// comparisons having large CI halfwidths (and filtering due to the
    /// CI halfwidth threshold).
    max_minrtt50_var: f32,
    /// During initialization of each `PathId`, we keep track of the
    /// best `TimeBin` for each `px_nexthops` value to find the best
    /// `TimeBin` (across all `px_nexthops`) for the `PathId`. If one
    /// `px_nexthops` value is used by a fraction of bins higher than
    /// this threshold, we pick its best `TimeBin` for this `PathId`. We
    /// assume this threshold to be at least 50% to avoid the need to
    /// break ties among multiple `px_nexthops`.
    min_frac_bins_using_bestbin_path: f32,
    /// This stores the primary `RouteInfo` for the best `TimeBin` for
    /// each `PathId`, chosen based on the thresholds above. `PathId`s
    /// without a valid best `TimeBin` are not included in the mapping.
    pathid2bestroute: HashMap<Rc<PathId>, RouteInfo>,
}

/// Summarize HD-ratio degradation over time comparing primary routes.
///
/// This struct requires initialization using through a call to `new`,
/// which will find the "best" `TimeBin` for each `PathId` in the
/// dataset. Thresholds below control filters applied to the algorithm
/// to find the best `TimeBin`.
pub struct HdRatioLowerBoundDegradationSummarizer {
    /// The minimum difference between the best `TimeBin` and other
    /// `TimeBin`s considered degradation.
    min_diff_degradation: f32,
    /// The maximum CI halfwidth of the performance difference between
    /// the best `TimeBin` and other `TimeBin`s. `TimeBin` comparisons
    /// whose CI halfwidth is above this threshold will not be
    /// considered valid. (This parameter is used after initialization,
    /// when computing degradation.)
    max_diff_ci_halfwidth: f32,
    /// During initialization, `TimeBin`s whose primary route's HD-ratio
    /// variance is above this threshold will not be considered for
    /// "best". (These `TimeBin`s will still be considered in
    /// comparisons later; if their primary routes have better
    /// performance than that of the `TimeBin` chosen for best, we say
    /// there is no degradation.) This parameter allows ignoring primary
    /// routes with large HD-ratio variance, which could lead to later
    /// comparisons having large CI halfwidths (and filtering due to the
    /// CI halfwidth threshold).
    max_hdratio_var: f32,
    /// This stores the primary `RouteInfo` for the best `TimeBin` for
    /// each `PathId`, chosen based on the thresholds above. `PathId`s
    /// without a valid best `TimeBin` are not included in the mapping.
    pathid2bestroute: HashMap<Rc<PathId>, RouteInfo>,
}

impl MinRtt50LowerBoundDegradationSummarizer {
    pub fn new(
        min_diff_degradation: u16,
        max_diff_ci_halfwidth: f32,
        max_minrtt50_var: f32,
        db: &DB,
    ) -> Self {
        let mut sum = Self {
            min_diff_degradation,
            max_diff_ci_halfwidth,
            max_minrtt50_var,
            pathid2bestroute: HashMap::new(),
        };
        for (pathid, time2bin) in &db.pathid2time2bin {
            let mut optrt: Option<RouteInfo> = None;
            for timebin in time2bin.values() {
                match timebin.get_primary_route() {
                    None => continue,
                    Some(primary) => {
                        if primary.minrtt_ms_p50_var >= max_minrtt50_var {
                            continue;
                        }
                        optrt = match optrt {
                            None => Some(**primary),
                            Some(rt) => {
                                if rt.minrtt_ms_p50 > primary.minrtt_ms_p50 {
                                    Some(**primary)
                                } else {
                                    Some(rt)
                                }
                            }
                        };
                    }
                }
            }
            match optrt {
                None => continue,
                Some(rt) => {
                    sum.pathid2bestroute.insert(Rc::clone(&pathid), rt);
                }
            };
        }
        sum
    }
}

impl TimeBinSummarizer for MinRtt50LowerBoundDegradationSummarizer {
    fn summarize(&self, pathid: &PathId, bin: &TimeBin) -> Option<TimeBinStats> {
        match (self.pathid2bestroute.get(pathid), bin.get_primary_route()) {
            (None, _) => None,
            (_, None) => None,
            (Some(bestroute), Some(primary)) => {
                let (diff, halfwidth) = RouteInfo::minrtt_median_diff_ci(bestroute, primary);
                if halfwidth > self.max_diff_ci_halfwidth {
                    None
                } else {
                    Some(TimeBinStats {
                        diff_ci: diff,
                        diff_ci_halfwidth: halfwidth,
                        is_shifted: diff + halfwidth <= -f32::from(self.min_diff_degradation),
                        bytes: bin.bytes_acked_sum,
                    })
                }
            }
        }
    }
    fn prefix(&self) -> String {
        format!(
            "minrtt50--lb-degradation--max-halfwidth-{:0.2}--max-var-{:0.2}--min-degradation-{}",
            self.max_diff_ci_halfwidth, self.max_minrtt50_var, self.min_diff_degradation
        )
    }
}

impl MinRtt50LowerBoundDistinctPathsDegradationSummarizer {
    pub fn new(
        min_diff_degradation: u16,
        max_diff_ci_halfwidth: f32,
        max_minrtt50_var: f32,
        min_frac_bins_using_bestbin_path: f32,
        db: &DB,
    ) -> Self {
        assert!(min_frac_bins_using_bestbin_path >= 0.5);
        let mut sum = Self {
            min_diff_degradation,
            max_diff_ci_halfwidth,
            max_minrtt50_var,
            min_frac_bins_using_bestbin_path,
            pathid2bestroute: HashMap::new(),
        };
        for (pathid, time2bin) in &db.pathid2time2bin {
            let mut id2cnt: HashMap<u64, u32> = HashMap::new();
            let mut id2best: HashMap<u64, &RouteInfo> = HashMap::new();
            for timebin in time2bin.values() {
                match timebin.get_primary_route() {
                    None => continue,
                    Some(primary) => {
                        if primary.minrtt_ms_p50_var >= max_minrtt50_var {
                            continue;
                        }
                        id2cnt.entry(primary.px_nexthops).and_modify(|e| *e += 1).or_insert(1);
                        id2best
                            .entry(primary.px_nexthops)
                            .and_modify(|rt| {
                                if (*rt).minrtt_ms_p50 > primary.minrtt_ms_p50 {
                                    *rt = primary
                                }
                            })
                            .or_insert(primary);
                    }
                }
            }
            if id2cnt.is_empty() {
                continue;
            }
            let nbins: u32 = id2cnt.values().fold(0u32, |acc, e| *e + acc);
            let mut maxid: Option<u64> = None;
            for (id, cnt) in id2cnt {
                if (cnt as f32) / (nbins as f32) > min_frac_bins_using_bestbin_path {
                    maxid = Some(id);
                    break;
                }
            }
            match maxid {
                None => continue,
                Some(id) => {
                    sum.pathid2bestroute.insert(Rc::clone(&pathid), id2best[&id].clone());
                }
            };
        }
        sum
    }
}

impl TimeBinSummarizer for MinRtt50LowerBoundDistinctPathsDegradationSummarizer {
    fn summarize(&self, pathid: &PathId, bin: &TimeBin) -> Option<TimeBinStats> {
        match (self.pathid2bestroute.get(pathid), bin.get_primary_route()) {
            (None, _) => None,
            (_, None) => None,
            (Some(bestroute), Some(primary)) => {
                let (diff, halfwidth) = RouteInfo::minrtt_median_diff_ci(bestroute, primary);
                if halfwidth > self.max_diff_ci_halfwidth {
                    None
                } else {
                    Some(TimeBinStats {
                        diff_ci: diff,
                        diff_ci_halfwidth: halfwidth,
                        is_shifted: diff + halfwidth <= -f32::from(self.min_diff_degradation),
                        bytes: bin.bytes_acked_sum,
                    })
                }
            }
        }
    }
    fn prefix(&self) -> String {
        format!("minrtt50--lb-degradation-identical--max-halfwidth-{:0.2}--max-var-{:0.2}--min-frac-bins-{:0.2}",
            self.max_diff_ci_halfwidth,
            self.max_minrtt50_var,
            self.min_frac_bins_using_bestbin_path,
        )
    }
}

impl HdRatioLowerBoundDegradationSummarizer {
    pub fn new(
        min_diff_degradation: f32,
        max_diff_ci_halfwidth: f32,
        max_hdratio_var: f32,
        db: &DB,
    ) -> Self {
        let mut sum = Self {
            min_diff_degradation,
            max_diff_ci_halfwidth,
            max_hdratio_var,
            pathid2bestroute: HashMap::new(),
        };
        for (pathid, time2bin) in &db.pathid2time2bin {
            let mut optrt: Option<RouteInfo> = None;
            for timebin in time2bin.values() {
                match timebin.get_primary_route() {
                    None => continue,
                    Some(primary) => {
                        if primary.hdratio_var >= max_hdratio_var {
                            continue;
                        }
                        optrt = match optrt {
                            None => Some(**primary),
                            Some(rt) => {
                                if rt.hdratio < primary.hdratio {
                                    Some(**primary)
                                } else {
                                    Some(rt)
                                }
                            }
                        };
                    }
                }
            }
            match optrt {
                None => continue,
                Some(rt) => {
                    sum.pathid2bestroute.insert(Rc::clone(&pathid), rt);
                }
            };
        }
        sum
    }
}

impl TimeBinSummarizer for HdRatioLowerBoundDegradationSummarizer {
    fn summarize(&self, pathid: &PathId, bin: &TimeBin) -> Option<TimeBinStats> {
        match (self.pathid2bestroute.get(pathid), bin.get_primary_route()) {
            (None, _) => None,
            (_, None) => None,
            (Some(bestroute), Some(primary)) => {
                let (diff, halfwidth) = RouteInfo::hdratio_diff_ci(bestroute, primary);
                if halfwidth > self.max_diff_ci_halfwidth {
                    None
                } else {
                    Some(TimeBinStats {
                        diff_ci: diff,
                        diff_ci_halfwidth: halfwidth,
                        is_shifted: diff - halfwidth > self.min_diff_degradation,
                        bytes: bin.bytes_acked_sum,
                    })
                }
            }
        }
    }
    fn prefix(&self) -> String {
        format!(
            "hdratio--lb-degradation--max-halfwidth-{:0.2}--max-var-{:0.2}--min-degradation-{:0.2}",
            self.max_diff_ci_halfwidth, self.max_hdratio_var, self.min_diff_degradation
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BIN_DURATION_SECS: u64 = 900;

    #[test]
    fn test_degradation_new_minrtt_var() {
        let pid1 = PathId {
            vip_metro: "gru".to_string(),
            bgp_ip_prefix: "1.0.0.0/24".parse().unwrap(),
        };

        let mut db: DB = DB::default();
        let time2bin = TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 50, 51, 8.0, 50, 60, 8.0);
        assert!(db.insert(pid1.clone(), time2bin).is_none());
        let sum = MinRtt50LowerBoundDegradationSummarizer::new(0, 10.0, 10.0, &db);
        assert!(sum.pathid2bestroute[&pid1].minrtt_ms_p50 == 50);

        let mut db: DB = DB::default();
        let time2bin = TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 50, 51, 8.0, 60, 51, 8.0);
        assert!(db.insert(pid1.clone(), time2bin).is_none());
        let sum = MinRtt50LowerBoundDegradationSummarizer::new(0, 10.0, 10.0, &db);
        assert!(sum.pathid2bestroute[&pid1].minrtt_ms_p50 == 50);

        let mut db: DB = DB::default();
        let time2bin = TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 50, 51, 12.0, 60, 51, 8.0);
        assert!(db.insert(pid1.clone(), time2bin).is_none());
        let sum = MinRtt50LowerBoundDegradationSummarizer::new(0, 10.0, 10.0, &db);
        assert!(sum.pathid2bestroute[&pid1].minrtt_ms_p50 == 60);

        let mut db: DB = DB::default();
        let time2bin = TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 50, 51, 12.0, 60, 51, 12.0);
        assert!(db.insert(pid1.clone(), time2bin).is_none());
        let sum = MinRtt50LowerBoundDegradationSummarizer::new(0, 10.0, 10.0, &db);
        assert!(!sum.pathid2bestroute.contains_key(&pid1));

        let mut db: DB = DB::default();
        let mut time2bin =
            TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 50, 51, 8.0, 60, 51, 8.0);
        // Will not be used because var is 12.0 and we only allow 10.0 below.
        let timebin = TimeBin::mock_minrtt_p50(BIN_DURATION_SECS, 40, 51, 12.0);
        time2bin.entry(BIN_DURATION_SECS).and_modify(|e| *e = timebin);
        assert!(db.insert(pid1.clone(), time2bin).is_none());
        let sum = MinRtt50LowerBoundDegradationSummarizer::new(0, 10.0, 10.0, &db);
        assert!(sum.pathid2bestroute[&pid1].minrtt_ms_p50 == 50);

        let mut db: DB = DB::default();
        let mut time2bin =
            TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 50, 51, 8.0, 60, 51, 8.0);
        // Will be used because var is 8.0 and we only allow 10.0 below.
        let timebin = TimeBin::mock_minrtt_p50(BIN_DURATION_SECS, 40, 51, 8.0);
        time2bin.entry(BIN_DURATION_SECS).and_modify(|e| *e = timebin);
        assert!(db.insert(pid1.clone(), time2bin).is_none());
        let sum = MinRtt50LowerBoundDegradationSummarizer::new(0, 10.0, 10.0, &db);
        assert!(sum.pathid2bestroute[&pid1].minrtt_ms_p50 == 40);
    }

    #[test]
    fn test_degradation_new_nexthops() {
        let pid1 = PathId {
            vip_metro: "gru".to_string(),
            bgp_ip_prefix: "1.0.0.0/24".parse().unwrap(),
        };

        let mut db: DB = DB::default();
        let mut time2bin =
            TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 50, 51, 8.0, 60, 51, 8.0);
        // Will not be used because px_nexthops shows up only once
        let mut timebin = TimeBin::mock_minrtt_p50(BIN_DURATION_SECS, 40, 51, 8.0);
        let mut rtinfo: Box<RouteInfo> = timebin.num2route[0].clone().unwrap();
        rtinfo.px_nexthops = 1337;
        timebin.num2route[0] = Some(rtinfo);
        time2bin.entry(BIN_DURATION_SECS).and_modify(|e| *e = timebin);
        let nbins: u64 = time2bin.len() as u64;
        assert!(db.insert(pid1.clone(), time2bin).is_none());
        assert!(db.total_traffic == nbins * TimeBin::MOCK_TOTAL_BYTES);
        let sum = MinRtt50LowerBoundDegradationSummarizer::new(0, 10.0, 10.0, &db);
        assert!(sum.pathid2bestroute[&pid1].minrtt_ms_p50 == 40);

        let mut db: DB = DB::default();
        let mut time2bin =
            TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 50, 51, 8.0, 60, 51, 8.0);
        // No route will win, even though we pass a min_frac of 0.5
        for i in (BIN_DURATION_SECS..(7 * 86400)).step_by(2 * BIN_DURATION_SECS as usize) {
            let mut timebin = TimeBin::mock_minrtt_p50(i, 40, 51, 8.0);
            let mut rtinfo: Box<RouteInfo> = timebin.num2route[0].clone().unwrap();
            rtinfo.px_nexthops = 1337;
            timebin.num2route[0] = Some(rtinfo);
            time2bin.entry(i).and_modify(|e| *e = timebin);
        }
        assert!(db.insert(pid1.clone(), time2bin).is_none());
        let sum = MinRtt50LowerBoundDegradationSummarizer::new(0, 10.0, 10.0, &db);
        assert!(sum.pathid2bestroute[&pid1].minrtt_ms_p50 == 40);
    }

    #[test]
    fn test_degradation_distinct_new_minrtt_var() {
        let pid1 = PathId {
            vip_metro: "gru".to_string(),
            bgp_ip_prefix: "1.0.0.0/24".parse().unwrap(),
        };

        let mut db: DB = DB::default();
        let time2bin = TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 50, 51, 8.0, 50, 60, 8.0);
        assert!(db.insert(pid1.clone(), time2bin).is_none());
        let sum =
            MinRtt50LowerBoundDistinctPathsDegradationSummarizer::new(0, 10.0, 10.0, 0.8, &db);
        assert!(sum.pathid2bestroute[&pid1].minrtt_ms_p50 == 50);

        let mut db: DB = DB::default();
        let time2bin = TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 50, 51, 8.0, 60, 51, 8.0);
        assert!(db.insert(pid1.clone(), time2bin).is_none());
        let sum =
            MinRtt50LowerBoundDistinctPathsDegradationSummarizer::new(0, 10.0, 10.0, 0.8, &db);
        assert!(sum.pathid2bestroute[&pid1].minrtt_ms_p50 == 50);

        let mut db: DB = DB::default();
        let time2bin = TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 50, 51, 12.0, 60, 51, 8.0);
        assert!(db.insert(pid1.clone(), time2bin).is_none());
        let sum =
            MinRtt50LowerBoundDistinctPathsDegradationSummarizer::new(0, 10.0, 10.0, 0.8, &db);
        assert!(sum.pathid2bestroute[&pid1].minrtt_ms_p50 == 60);

        let mut db: DB = DB::default();
        let time2bin = TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 50, 51, 12.0, 60, 51, 12.0);
        assert!(db.insert(pid1.clone(), time2bin).is_none());
        let sum =
            MinRtt50LowerBoundDistinctPathsDegradationSummarizer::new(0, 10.0, 10.0, 0.8, &db);
        assert!(!sum.pathid2bestroute.contains_key(&pid1));

        let mut db: DB = DB::default();
        let mut time2bin =
            TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 50, 51, 8.0, 60, 51, 8.0);
        // Will not be used because var is 12.0 and we only allow 10.0 below.
        let timebin = TimeBin::mock_minrtt_p50(BIN_DURATION_SECS, 40, 51, 12.0);
        time2bin.entry(BIN_DURATION_SECS).and_modify(|e| *e = timebin);
        assert!(db.insert(pid1.clone(), time2bin).is_none());
        let sum =
            MinRtt50LowerBoundDistinctPathsDegradationSummarizer::new(0, 10.0, 10.0, 0.8, &db);
        assert!(sum.pathid2bestroute[&pid1].minrtt_ms_p50 == 50);

        let mut db: DB = DB::default();
        let mut time2bin =
            TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 50, 51, 8.0, 60, 51, 8.0);
        // Will be used because var is 8.0 and we only allow 10.0 below.
        let timebin = TimeBin::mock_minrtt_p50(BIN_DURATION_SECS, 40, 51, 8.0);
        time2bin.entry(BIN_DURATION_SECS).and_modify(|e| *e = timebin);
        assert!(db.insert(pid1.clone(), time2bin).is_none());
        let sum =
            MinRtt50LowerBoundDistinctPathsDegradationSummarizer::new(0, 10.0, 10.0, 0.8, &db);
        assert!(sum.pathid2bestroute[&pid1].minrtt_ms_p50 == 40);
    }

    #[test]
    fn test_degradation_distinct_new_nexthops() {
        let pid1 = PathId {
            vip_metro: "gru".to_string(),
            bgp_ip_prefix: "1.0.0.0/24".parse().unwrap(),
        };

        let mut db: DB = DB::default();
        let mut time2bin =
            TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 50, 51, 8.0, 60, 51, 8.0);
        // Will not be used because px_nexthops shows up only once
        let mut timebin = TimeBin::mock_minrtt_p50(BIN_DURATION_SECS, 40, 51, 8.0);
        let mut rtinfo: Box<RouteInfo> = timebin.num2route[0].clone().unwrap();
        rtinfo.px_nexthops = 1337;
        timebin.num2route[0] = Some(rtinfo);
        time2bin.entry(BIN_DURATION_SECS).and_modify(|e| *e = timebin);
        let nbins: u64 = time2bin.len() as u64;
        assert!(db.insert(pid1.clone(), time2bin).is_none());
        assert!(db.total_traffic == nbins * TimeBin::MOCK_TOTAL_BYTES);
        let sum =
            MinRtt50LowerBoundDistinctPathsDegradationSummarizer::new(0, 10.0, 10.0, 0.8, &db);
        assert!(sum.pathid2bestroute[&pid1].minrtt_ms_p50 == 50);

        let mut db: DB = DB::default();
        let mut time2bin =
            TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 50, 51, 8.0, 60, 51, 8.0);
        // No route will win, even though we pass a min_frac of 0.5
        for i in (BIN_DURATION_SECS..(7 * 86400)).step_by(2 * BIN_DURATION_SECS as usize) {
            let mut timebin = TimeBin::mock_minrtt_p50(i, 60, 51, 8.0);
            let mut rtinfo: Box<RouteInfo> = timebin.num2route[0].clone().unwrap();
            rtinfo.px_nexthops = 1337;
            timebin.num2route[0] = Some(rtinfo);
            time2bin.entry(i).and_modify(|e| *e = timebin);
        }
        assert!(db.insert(pid1.clone(), time2bin).is_none());
        let sum =
            MinRtt50LowerBoundDistinctPathsDegradationSummarizer::new(0, 10.0, 10.0, 0.5, &db);
        assert!(!sum.pathid2bestroute.contains_key(&pid1));
    }

    #[test]
    fn test_degradation_distinct_summarize() {
        let pid1 = PathId {
            vip_metro: "gru".to_string(),
            bgp_ip_prefix: "1.0.0.0/24".parse().unwrap(),
        };

        let mut db: DB = DB::default();
        let time2bin = TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 50, 51, 8.0, 50, 60, 8.0);
        assert!(db.insert(pid1.clone(), time2bin).is_none());
        let sum =
            MinRtt50LowerBoundDistinctPathsDegradationSummarizer::new(0, 10.0, 10.0, 0.8, &db);
        assert!(sum.pathid2bestroute[&pid1].minrtt_ms_p50 == 50);

        let timebin = TimeBin::mock_minrtt_p50(0, 51, 51, 8.0);
        let binstats = sum.summarize(&pid1, &timebin);
        assert!(binstats.is_some());
        let binstats = binstats.unwrap();
        assert!(!binstats.is_shifted);
        assert!((binstats.diff_ci + 1.0).abs() < 1e-6);

        let timebin = TimeBin::mock_minrtt_p50(0, 60, 51, 8.0);
        let binstats = sum.summarize(&pid1, &timebin);
        assert!(binstats.is_some());
        let binstats = binstats.unwrap();
        assert!(binstats.is_shifted);
        assert!((binstats.diff_ci + 10.0).abs() < 1e-6);

        let timebin = TimeBin::mock_minrtt_p50(0, 60, 51, 100.0);
        let binstats = sum.summarize(&pid1, &timebin);
        assert!(binstats.is_none());
    }

    #[test]
    fn test_hdratio_summarize() {
        let pid1 = PathId {
            vip_metro: "gru".to_string(),
            bgp_ip_prefix: "1.0.0.0/24".parse().unwrap(),
        };

        let mut db: DB = DB::default();
        let time2bin =
            TimeBin::mock_week_hdratio(BIN_DURATION_SECS, 0.95, 0.8, 0.5, 0.95, 0.8, 0.5);
        assert!(db.insert(pid1.clone(), time2bin).is_none());
        let sum = HdRatioLowerBoundDegradationSummarizer::new(0.0, 0.2, 0.6, &db);
        assert!((sum.pathid2bestroute[&pid1].hdratio - 0.95).abs() < 1e-6);

        // ci_halfwidth = 2 * (0.5/100 + 0.5/100).sqrt() = 0.2
        let timebin = TimeBin::mock_hdratio(0, 0.9, 0.8, 0.5);
        let binstats = sum.summarize(&pid1, &timebin);
        assert!(binstats.is_some());
        let binstats = binstats.unwrap();
        assert!(!binstats.is_shifted);
        assert!((binstats.diff_ci - 0.05).abs() < 1e-6);

        let timebin = TimeBin::mock_hdratio(0, 0.7, 0.8, 0.5);
        let binstats = sum.summarize(&pid1, &timebin);
        assert!(binstats.is_some());
        let binstats = binstats.unwrap();
        assert!(binstats.is_shifted);
        assert!((binstats.diff_ci - 0.25).abs() < 1e-6);

        // ci_halfwidth = 2 * (0.5/100 + 0.8/100).sqrt() = 0.22
        let timebin = TimeBin::mock_hdratio(0, 0.7, 0.8, 0.8);
        let binstats = sum.summarize(&pid1, &timebin);
        assert!(binstats.is_none());
    }

}
