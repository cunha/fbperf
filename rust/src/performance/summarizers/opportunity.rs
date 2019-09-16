use crate::performance::db::{PathId, RouteInfo, TimeBin, DB};
use crate::performance::perfstats::{TimeBinStats, TimeBinSummarizer, TimeBinSummary};

#[derive(Clone, Copy, Debug)]
pub struct MinRtt50ImprovementSummarizer {
    pub minrtt50_min_improv: u16,
    pub max_minrtt50_diff_ci_halfwidth: f32,
    pub compare_lower_bound: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct HdRatioImprovementSummarizer {
    pub hdratio_min_improv: f32,
    pub max_hdratio_diff_ci_halfwidth: f32,
    pub compare_lower_bound: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct HdRatio50ImprovementSummarizer {
    pub hdratio50_min_improv: f32,
    pub max_hdratio50_diff_ci_halfwidth: f32,
    pub compare_lower_bound: bool,
}

impl TimeBinSummarizer for MinRtt50ImprovementSummarizer {
    fn summarize(&self, _pathid: &PathId, bin: &TimeBin) -> TimeBinSummary {
        match (bin.get_primary_route(), bin.get_best_alternate(RouteInfo::compare_median_minrtt)) {
            (None, _) => TimeBinSummary::NoRoute,
            (_, None) => TimeBinSummary::NoRoute,
            (Some(ref primary), Some(ref bestalt)) => {
                let (diff, halfwidth) = RouteInfo::minrtt_median_diff_ci(primary, bestalt);
                if halfwidth > self.max_minrtt50_diff_ci_halfwidth {
                    TimeBinSummary::WideConfidenceInterval
                } else {
                    let limit: f32 = if self.compare_lower_bound {
                        diff - halfwidth
                    } else {
                        diff
                    };
                    TimeBinSummary::Valid(TimeBinStats {
                        diff_ci: diff,
                        diff_ci_halfwidth: halfwidth,
                        is_shifted: limit >= f32::from(self.minrtt50_min_improv),
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
        let bin = &db.pathid2time2bin[pathid][&time];
        (
            bin.get_primary_route().as_ref().unwrap(),
            bin.get_best_alternate(RouteInfo::compare_median_minrtt).as_ref().unwrap(),
        )
    }
    fn prefix(&self) -> String {
        format!(
            "minrtt50--opp--bound-{}--halfwidth-{:0.2}--min-improv-{}",
            self.compare_lower_bound, self.max_minrtt50_diff_ci_halfwidth, self.minrtt50_min_improv,
        )
    }
}

impl TimeBinSummarizer for HdRatioImprovementSummarizer {
    fn summarize(&self, _pathid: &PathId, bin: &TimeBin) -> TimeBinSummary {
        match (bin.get_primary_route(), bin.get_best_alternate(RouteInfo::compare_hdratio)) {
            (None, _) => TimeBinSummary::NoRoute,
            (_, None) => TimeBinSummary::NoRoute,
            (Some(ref primary), Some(ref bestalt)) => {
                let (diff, halfwidth) = RouteInfo::hdratio_diff_ci(bestalt, primary);
                if halfwidth > self.max_hdratio_diff_ci_halfwidth {
                    TimeBinSummary::WideConfidenceInterval
                } else {
                    let limit: f32 = if self.compare_lower_bound {
                        diff - halfwidth
                    } else {
                        diff
                    };
                    TimeBinSummary::Valid(TimeBinStats {
                        diff_ci: diff,
                        diff_ci_halfwidth: halfwidth,
                        is_shifted: limit >= self.hdratio_min_improv,
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
        let bin = &db.pathid2time2bin[pathid][&time];
        (
            bin.get_primary_route().as_ref().unwrap(),
            bin.get_best_alternate(RouteInfo::compare_hdratio).as_ref().unwrap(),
        )
    }
    fn prefix(&self) -> String {
        format!(
            "hdratio--opp--bound-{}--halfwidth-{:0.2}--min-improv-{:0.2}",
            self.compare_lower_bound, self.max_hdratio_diff_ci_halfwidth, self.hdratio_min_improv,
        )
    }
}

impl TimeBinSummarizer for HdRatio50ImprovementSummarizer {
    fn summarize(&self, _pathid: &PathId, bin: &TimeBin) -> TimeBinSummary {
        match (bin.get_primary_route(), bin.get_best_alternate(RouteInfo::compare_median_minrtt)) {
            (None, _) => TimeBinSummary::NoRoute,
            (_, None) => TimeBinSummary::NoRoute,
            (Some(ref primary), Some(ref bestalt)) => {
                let (diff, halfwidth) = RouteInfo::hdratio_median_diff_ci(primary, bestalt);
                if halfwidth > self.max_hdratio50_diff_ci_halfwidth {
                    TimeBinSummary::WideConfidenceInterval
                } else {
                    let limit: f32 = if self.compare_lower_bound {
                        diff - halfwidth
                    } else {
                        diff
                    };
                    TimeBinSummary::Valid(TimeBinStats {
                        diff_ci: diff,
                        diff_ci_halfwidth: halfwidth,
                        is_shifted: limit >= f32::from(self.hdratio50_min_improv),
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
        let bin = &db.pathid2time2bin[pathid][&time];
        (
            bin.get_primary_route().as_ref().unwrap(),
            bin.get_best_alternate(RouteInfo::compare_median_hdratio).as_ref().unwrap(),
        )
    }
    fn prefix(&self) -> String {
        format!(
            "hdratio50--opp--bound-{}--halfwidth-{:0.2}--min-improv-{:0.2}",
            self.compare_lower_bound, self.max_hdratio50_diff_ci_halfwidth, self.hdratio50_min_improv,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::performance::db;

    #[test]
    fn test_minrtt_p50_lower_bound() {
        let _pathid: db::PathId = db::tests::make_path_id();

        let sum = MinRtt50ImprovementSummarizer {
            minrtt50_min_improv: 0,
            max_minrtt50_diff_ci_halfwidth: 100.0,
            compare_lower_bound: true,
        };

        let mut timebin = db::TimeBin::mock_minrtt_p50(0, 15, 10, 100.0);

        let binsum = sum.summarize(&_pathid, &timebin);
        if let TimeBinSummary::Valid(binstats) = binsum {
            assert!(!binstats.is_shifted);
            assert!((binstats.diff_ci - 5.0).abs() < 1e-6);
        } else {
            unreachable!();
        }

        timebin.num2route[1] = None;

        let binsum = sum.summarize(&_pathid, &timebin);
        assert!(binsum == TimeBinSummary::NoRoute);
    }

    #[test]
    fn test_minrtt_p50_lower_bound_valid() {
        let _pathid: db::PathId = db::tests::make_path_id();

        let sum1 = MinRtt50ImprovementSummarizer {
            minrtt50_min_improv: 0,
            max_minrtt50_diff_ci_halfwidth: 7.0,
            compare_lower_bound: true,
        };
        let sum2 = MinRtt50ImprovementSummarizer {
            minrtt50_min_improv: 0,
            max_minrtt50_diff_ci_halfwidth: 9.0,
            compare_lower_bound: true,
        };

        // ci_halfwidth = 2 * (8 + 8).sqrt() = 8
        let timebin = db::TimeBin::mock_minrtt_p50(0, 20, 10, 8.0);

        let binsum1 = sum1.summarize(&_pathid, &timebin);
        assert!(binsum1 == TimeBinSummary::WideConfidenceInterval);

        let binsum2 = sum2.summarize(&_pathid, &timebin);
        if let TimeBinSummary::Valid(binstats) = binsum2 {
            assert!(binstats.is_shifted);
            assert!((binstats.diff_ci - 10.0).abs() < 1e-6);
        } else {
            unreachable!();
        }

        let timebin = db::TimeBin::mock_minrtt_p50(0, 15, 10, 8.0);

        let binsum2 = sum2.summarize(&_pathid, &timebin);
        if let TimeBinSummary::Valid(binstats) = binsum2 {
            assert!(!binstats.is_shifted);
            assert!((binstats.diff_ci - 5.0).abs() < 1e-6);
        } else {
            unreachable!();
        }
    }

    #[test]
    fn test_hdratio_lower_bound() {
        let _pathid: db::PathId = db::tests::make_path_id();

        let sum = HdRatioImprovementSummarizer {
            hdratio_min_improv: 0.0,
            max_hdratio_diff_ci_halfwidth: 0.5,
            compare_lower_bound: true,
        };

        // ci_halfwidth = 2 * (0.5/100 + 0.5/100).sqrt() = 0.2
        let mut timebin = db::TimeBin::mock_hdratio(0, 0.8, 0.9, 0.5);

        let binsum = sum.summarize(&_pathid, &timebin);
        if let TimeBinSummary::Valid(binstats) = binsum {
            assert!(!binstats.is_shifted);
            assert!((binstats.diff_ci - 0.1).abs() < 1e-6);
            assert!((binstats.diff_ci_halfwidth - 0.2).abs() < 1e-6);
        } else {
            unreachable!();
        }

        timebin.num2route[1] = None;

        let binsum = sum.summarize(&_pathid, &timebin);
        assert!(binsum == TimeBinSummary::NoRoute);
    }

    #[test]
    fn test_hdratio_lower_bound_valid() {
        let _pathid: db::PathId = db::tests::make_path_id();

        let sum1 = HdRatioImprovementSummarizer {
            hdratio_min_improv: 0.0,
            max_hdratio_diff_ci_halfwidth: 0.15,
            compare_lower_bound: true,
        };
        let sum2 = HdRatioImprovementSummarizer {
            hdratio_min_improv: 0.0,
            max_hdratio_diff_ci_halfwidth: 0.25,
            compare_lower_bound: true,
        };

        // ci_halfwidth = 2 * (0.5/100 + 0.5/100).sqrt() = 0.2
        let timebin = db::TimeBin::mock_hdratio(0, 0.70, 0.95, 0.5);

        let binstats1 = sum1.summarize(&_pathid, &timebin);
        assert!(binstats1 == TimeBinSummary::WideConfidenceInterval);

        let binsum = sum2.summarize(&_pathid, &timebin);
        if let TimeBinSummary::Valid(binstats) = binsum {
            assert!(binstats.is_shifted);
            assert!((binstats.diff_ci - 0.25).abs() < 1e-6);
            assert!((binstats.diff_ci_halfwidth - 0.2).abs() < 1e-6);
        } else {
            unreachable!();
        }

        let timebin = db::TimeBin::mock_hdratio(0, 0.8, 0.95, 0.5);

        let binsum = sum2.summarize(&_pathid, &timebin);
        if let TimeBinSummary::Valid(binstats) = binsum {
            assert!(!binstats.is_shifted);
            assert!((binstats.diff_ci - 0.15).abs() < 1e-6);
            assert!((binstats.diff_ci_halfwidth - 0.2).abs() < 1e-6);
        } else {
            unreachable!();
        }
    }
}
