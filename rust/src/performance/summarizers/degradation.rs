use std::collections::HashMap;
use std::rc::Rc;

use crate::performance::db::{PathId, RouteInfo, TimeBin, DB};
use crate::performance::perfstats::{TimeBinStats, TimeBinSummarizer, TimeBinSummary};

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
    pathid2baseroute: HashMap<Rc<PathId>, Box<RouteInfo>>,
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
    pathid2baseroute: HashMap<Rc<PathId>, Box<RouteInfo>>,
}

impl MinRtt50LowerBoundDegradationSummarizer {
    pub fn new(
        baseline_percentile: f32,
        min_diff_degradation: u16,
        max_diff_ci_halfwidth: f32,
        max_minrtt50_var: f32,
        db: &DB,
    ) -> Self {
        let mut sum = Self {
            min_diff_degradation,
            max_diff_ci_halfwidth,
            max_minrtt50_var,
            pathid2baseroute: HashMap::new(),
        };
        for (pathid, time2bin) in &db.pathid2time2bin {
            let mut valid: Vec<RouteInfo> = Vec::default();
            for timebin in time2bin.values() {
                match timebin.get_primary_route() {
                    None => continue,
                    Some(primary) => {
                        if primary.minrtt_ms_p50_var >= max_minrtt50_var {
                            continue;
                        }
                        valid.push(**primary);
                    }
                }
            }
            if valid.is_empty() {
                continue;
            }
            valid.sort_by(RouteInfo::compare_median_minrtt);
            let i: usize = ((valid.len() - 1) as f32 * baseline_percentile).round() as usize;
            sum.pathid2baseroute.insert(Rc::clone(&pathid), Box::new(valid[i]));
        }
        sum
    }
}

impl TimeBinSummarizer for MinRtt50LowerBoundDegradationSummarizer {
    fn summarize(&self, pathid: &PathId, bin: &TimeBin) -> TimeBinSummary {
        match (self.pathid2baseroute.get(pathid), bin.get_primary_route()) {
            (None, _) => TimeBinSummary::WideConfidenceInterval,
            (_, None) => TimeBinSummary::NoRoute,
            (Some(bestroute), Some(primary)) => {
                let (diff, halfwidth) = RouteInfo::minrtt_median_diff_ci(primary, bestroute);
                if halfwidth > self.max_diff_ci_halfwidth {
                    TimeBinSummary::WideConfidenceInterval
                } else {
                    TimeBinSummary::Valid(TimeBinStats {
                        diff_ci: diff,
                        diff_ci_halfwidth: halfwidth,
                        is_shifted: diff - halfwidth > f32::from(self.min_diff_degradation),
                        bytes: bin.bytes_acked_sum,
                    })
                }
            }
        }
    }
    fn get_routes<'s: 'd, 'd>(
        &'s self,
        pathid: &PathId,
        time: u64,
        db: &'d DB,
    ) -> (&'d RouteInfo, &'d RouteInfo) {
        (
            &self.pathid2baseroute[pathid],
            &db.pathid2time2bin[pathid][&time].get_primary_route().as_ref().unwrap(),
        )
    }
    fn prefix(&self) -> String {
        format!(
            "minrtt50--deg--bound-true--halfwidth-{:0.2}--max-var-{:0.2}--min-deg-{}",
            self.max_diff_ci_halfwidth, self.max_minrtt50_var, self.min_diff_degradation
        )
    }
}

impl HdRatioLowerBoundDegradationSummarizer {
    pub fn new(
        baseline_percentile: f32,
        min_diff_degradation: f32,
        max_diff_ci_halfwidth: f32,
        max_hdratio_var: f32,
        db: &DB,
    ) -> Self {
        let mut sum = Self {
            min_diff_degradation,
            max_diff_ci_halfwidth,
            max_hdratio_var,
            pathid2baseroute: HashMap::new(),
        };
        for (pathid, time2bin) in &db.pathid2time2bin {
            let mut valid: Vec<RouteInfo> = Vec::default();
            for timebin in time2bin.values() {
                match timebin.get_primary_route() {
                    None => continue,
                    Some(primary) => {
                        if primary.hdratio_var >= max_hdratio_var {
                            continue;
                        }
                        valid.push(**primary);
                    }
                }
            }
            if valid.is_empty() {
                continue;
            }
            valid.sort_by(RouteInfo::compare_hdratio);
            let i: usize = ((valid.len() - 1) as f32 * baseline_percentile).round() as usize;
            sum.pathid2baseroute.insert(Rc::clone(&pathid), Box::new(valid[i]));
        }
        sum
    }
}

impl TimeBinSummarizer for HdRatioLowerBoundDegradationSummarizer {
    fn summarize(&self, pathid: &PathId, bin: &TimeBin) -> TimeBinSummary {
        match (self.pathid2baseroute.get(pathid), bin.get_primary_route()) {
            (None, _) => TimeBinSummary::WideConfidenceInterval,
            (_, None) => TimeBinSummary::NoRoute,
            (Some(bestroute), Some(primary)) => {
                let (diff, halfwidth) = RouteInfo::hdratio_diff_ci(bestroute, primary);
                if halfwidth > self.max_diff_ci_halfwidth {
                    TimeBinSummary::WideConfidenceInterval
                } else {
                    TimeBinSummary::Valid(TimeBinStats {
                        diff_ci: diff,
                        diff_ci_halfwidth: halfwidth,
                        is_shifted: diff - halfwidth > self.min_diff_degradation,
                        bytes: bin.bytes_acked_sum,
                    })
                }
            }
        }
    }
    fn get_routes<'s: 'd, 'd>(
        &'s self,
        pathid: &PathId,
        time: u64,
        db: &'d DB,
    ) -> (&'d RouteInfo, &'d RouteInfo) {
        (
            &self.pathid2baseroute[pathid],
            &db.pathid2time2bin[pathid][&time].get_primary_route().as_ref().unwrap(),
        )
    }
    fn prefix(&self) -> String {
        format!(
            "hdratio--deg--bound-true--halfwidth-{:0.2}--max-var-{:0.2}--min-deg-{:0.2}",
            self.max_diff_ci_halfwidth, self.max_hdratio_var, self.min_diff_degradation
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::performance::db;

    const BIN_DURATION_SECS: u64 = 900;

    #[test]
    fn test_minrtt_degradation_new_minrtt_var() {
        let pid1 = db::tests::make_path_id();

        let mut db: DB = DB::default();
        let time2bin = TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 50, 51, 8.0, 50, 60, 8.0);
        assert!(db.insert(pid1.clone(), time2bin).is_none());
        let sum = MinRtt50LowerBoundDegradationSummarizer::new(0.0, 0, 10.0, 10.0, &db);
        assert!(sum.pathid2baseroute[&pid1].minrtt_ms_p50 == 50);

        let mut db: DB = DB::default();
        let time2bin = TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 50, 51, 8.0, 60, 51, 8.0);
        assert!(db.insert(pid1.clone(), time2bin).is_none());
        let sum = MinRtt50LowerBoundDegradationSummarizer::new(0.0, 0, 10.0, 10.0, &db);
        assert!(sum.pathid2baseroute[&pid1].minrtt_ms_p50 == 50);

        let mut db: DB = DB::default();
        let time2bin = TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 50, 51, 12.0, 60, 51, 8.0);
        assert!(db.insert(pid1.clone(), time2bin).is_none());
        let sum = MinRtt50LowerBoundDegradationSummarizer::new(0.0, 0, 10.0, 10.0, &db);
        assert!(sum.pathid2baseroute[&pid1].minrtt_ms_p50 == 60);

        let mut db: DB = DB::default();
        let time2bin = TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 50, 51, 12.0, 60, 51, 12.0);
        assert!(db.insert(pid1.clone(), time2bin).is_none());
        let sum = MinRtt50LowerBoundDegradationSummarizer::new(0.0, 0, 10.0, 10.0, &db);
        assert!(!sum.pathid2baseroute.contains_key(&pid1));

        let mut db: DB = DB::default();
        let mut time2bin =
            TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 50, 51, 8.0, 60, 51, 8.0);
        // Will not be used because var is 12.0 and we only allow 10.0 below.
        let timebin = TimeBin::mock_minrtt_p50(BIN_DURATION_SECS, 40, 51, 12.0);
        time2bin.entry(BIN_DURATION_SECS).and_modify(|e| *e = timebin);
        assert!(db.insert(pid1.clone(), time2bin).is_none());
        let sum = MinRtt50LowerBoundDegradationSummarizer::new(0.0, 0, 10.0, 10.0, &db);
        assert!(sum.pathid2baseroute[&pid1].minrtt_ms_p50 == 50);

        let mut db: DB = DB::default();
        let mut time2bin =
            TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 50, 51, 8.0, 60, 51, 8.0);
        // Will be used because var is 8.0 and we only allow 10.0 below.
        let timebin = TimeBin::mock_minrtt_p50(BIN_DURATION_SECS, 40, 51, 8.0);
        time2bin.entry(BIN_DURATION_SECS).and_modify(|e| *e = timebin);
        assert!(db.insert(pid1.clone(), time2bin).is_none());
        let sum = MinRtt50LowerBoundDegradationSummarizer::new(0.0, 0, 10.0, 10.0, &db);
        assert!(sum.pathid2baseroute[&pid1].minrtt_ms_p50 == 40);
    }

    #[test]
    fn test_minrtt_degradation_new_nexthops() {
        let pid1 = db::tests::make_path_id();

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
        let sum = MinRtt50LowerBoundDegradationSummarizer::new(0.0, 0, 10.0, 10.0, &db);
        assert!(sum.pathid2baseroute[&pid1].minrtt_ms_p50 == 40);

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
        let sum = MinRtt50LowerBoundDegradationSummarizer::new(0.0, 0, 10.0, 10.0, &db);
        assert!(sum.pathid2baseroute[&pid1].minrtt_ms_p50 == 40);
    }

    #[test]
    fn test_hdratio_summarize() {
        let pid1 = db::tests::make_path_id();

        let mut db: DB = DB::default();
        let time2bin =
            TimeBin::mock_week_hdratio(BIN_DURATION_SECS, 0.95, 0.8, 0.5, 0.95, 0.8, 0.5);
        assert!(db.insert(pid1.clone(), time2bin).is_none());
        let sum = HdRatioLowerBoundDegradationSummarizer::new(1.0, 0.0, 0.2, 0.6, &db);
        assert!((sum.pathid2baseroute[&pid1].hdratio - 0.95).abs() < 1e-6);

        // ci_halfwidth = 2 * (0.5/100 + 0.5/100).sqrt() = 0.2
        let timebin = TimeBin::mock_hdratio(0, 0.9, 0.8, 0.5);
        let binsum = sum.summarize(&pid1, &timebin);
        if let TimeBinSummary::Valid(binstats) = binsum {
            assert!(!binstats.is_shifted);
            assert!((binstats.diff_ci - 0.05).abs() < 1e-6);
        } else {
            unreachable!();
        }

        let timebin = TimeBin::mock_hdratio(0, 0.7, 0.8, 0.5);
        let binsum = sum.summarize(&pid1, &timebin);
        if let TimeBinSummary::Valid(binstats) = binsum {
            assert!(binstats.is_shifted);
            assert!((binstats.diff_ci - 0.25).abs() < 1e-6);
        } else {
            unreachable!();
        }

        // ci_halfwidth = 2 * (0.5/100 + 0.8/100).sqrt() = 0.22
        let timebin = TimeBin::mock_hdratio(0, 0.7, 0.8, 0.8);
        let binsum = sum.summarize(&pid1, &timebin);
        assert!(binsum == TimeBinSummary::WideConfidenceInterval);
    }

    #[test]
    fn test_minrtt_degradation_new_baseline_percentile() {
        let pid1 = db::tests::make_path_id();

        let mut db: DB = DB::default();
        let mut time2bin =
            TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 50, 51, 8.0, 50, 60, 8.0);
        let nbins: usize = time2bin.len();
        for (i, timebin) in time2bin.values_mut().enumerate() {
            let mut rtinfo: Box<RouteInfo> = timebin.num2route[0].clone().unwrap();
            rtinfo.minrtt_ms_p50 += i as u16;
            timebin.num2route[0] = Some(rtinfo);
        }
        assert!(db.insert(pid1.clone(), time2bin).is_none());

        for i in 0..nbins {
            let pct: f32 = i as f32 / nbins as f32;
            let sum = MinRtt50LowerBoundDegradationSummarizer::new(pct, 0, 10.0, 10.0, &db);
            let offset: usize = usize::from(sum.pathid2baseroute[&pid1].minrtt_ms_p50 - 50);
            println!("----> {} {} {}", i, pct, sum.pathid2baseroute[&pid1].minrtt_ms_p50);
            assert!(offset == i || offset == i + 1 || offset == i - 1);
        }

        let mut db: DB = DB::default();
        let mut time2bin =
            TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 50, 60, 8.0, 50, 60, 12.0);
        let nbins: usize = time2bin.len();
        for (i, timebin) in time2bin.values_mut().enumerate() {
            let mut rtinfo: Box<RouteInfo> = timebin.num2route[0].clone().unwrap();
            rtinfo.minrtt_ms_p50 += i as u16;
            timebin.num2route[0] = Some(rtinfo);
        }
        assert!(db.insert(pid1.clone(), time2bin).is_none());

        for i in 0..nbins {
            // Half the timebins are ignored because of high variance,
            // so we have half the number of valid bins:
            let pct: f32 = i as f32 / nbins as f32;
            let sum = MinRtt50LowerBoundDegradationSummarizer::new(pct, 0, 10.0, 10.0, &db);
            let offset: usize = usize::from(sum.pathid2baseroute[&pid1].minrtt_ms_p50 - 50);
            assert!(offset >= std::cmp::max(i, 2) - 2 && offset <= i + 2);
        }
    }

}
