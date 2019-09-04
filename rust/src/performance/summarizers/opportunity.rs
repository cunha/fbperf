use crate::performance::db::{PathId, RouteInfo, TimeBin};
use crate::performance::perfstats::{TimeBinStats, TimeBinSummarizer};

#[derive(Clone, Copy, Debug)]
pub struct MinRtt50ImprovementSummarizer {
    pub minrtt50_min_improv: u16,
    pub max_minrtt50_diff_ci_halfwidth: f32,
    pub no_alternate_is_valid: bool,
    pub compare_lower_bound: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct HdRatioImprovementSummarizer {
    pub hdratio_min_improv: f32,
    pub max_hdratio_diff_ci_halfwidth: f32,
    pub no_alternate_is_valid: bool,
    pub compare_lower_bound: bool,
}

impl TimeBinSummarizer for MinRtt50ImprovementSummarizer {
    fn summarize(&self, _pathid: &PathId, bin: &TimeBin) -> Option<TimeBinStats> {
        match (bin.get_primary_route(), bin.get_best_alternate(RouteInfo::compare_median_minrtt)) {
            (None, _) => None,
            (Some(_), None) => {
                if self.no_alternate_is_valid {
                    Some(TimeBinStats {
                        diff_ci: 0.0,
                        diff_ci_halfwidth: 0.0,
                        is_shifted: false,
                        bytes: bin.bytes_acked_sum,
                    })
                } else {
                    None
                }
            }
            (Some(ref primary), Some(ref bestalt)) => {
                let (diff, halfwidth) = RouteInfo::minrtt_median_diff_ci(primary, bestalt);
                if halfwidth > self.max_minrtt50_diff_ci_halfwidth {
                    None
                } else {
                    let limit: f32 = if self.compare_lower_bound {
                        diff - halfwidth
                    } else {
                        diff
                    };
                    Some(TimeBinStats {
                        diff_ci: diff,
                        diff_ci_halfwidth: halfwidth,
                        is_shifted: limit >= f32::from(self.minrtt50_min_improv),
                        bytes: bin.bytes_acked_sum,
                    })
                }
            }
        }
    }
    fn prefix(&self) -> String {
        format!(
            "minrtt50--opp--bound-{}--no-alt-valid-{}--halfwidth-{:0.2}--min-improv-{}",
            self.compare_lower_bound,
            self.no_alternate_is_valid,
            self.max_minrtt50_diff_ci_halfwidth,
            self.minrtt50_min_improv,
        )
    }
}

impl TimeBinSummarizer for HdRatioImprovementSummarizer {
    fn summarize(&self, _pathid: &PathId, bin: &TimeBin) -> Option<TimeBinStats> {
        match (bin.get_primary_route(), bin.get_best_alternate(RouteInfo::compare_hdratio)) {
            (None, _) => None,
            (Some(_), None) => {
                if self.no_alternate_is_valid {
                    Some(TimeBinStats {
                        diff_ci: 0.0,
                        diff_ci_halfwidth: 0.0,
                        is_shifted: false,
                        bytes: bin.bytes_acked_sum,
                    })
                } else {
                    None
                }
            }
            (Some(ref primary), Some(ref bestalt)) => {
                let (diff, halfwidth) = RouteInfo::hdratio_diff_ci(bestalt, primary);
                if halfwidth > self.max_hdratio_diff_ci_halfwidth {
                    None
                } else {
                    let limit: f32 = if self.compare_lower_bound {
                        diff - halfwidth
                    } else {
                        diff
                    };
                    Some(TimeBinStats {
                        diff_ci: diff,
                        diff_ci_halfwidth: halfwidth,
                        is_shifted: limit >= self.hdratio_min_improv,
                        bytes: bin.bytes_acked_sum,
                    })
                }
            }
        }
    }
    fn prefix(&self) -> String {
        format!(
            "hdratio--opp--bound-{}--no-alt-valid-{}--halfwidth-{:0.2}--min-improv-{:0.2}",
            self.compare_lower_bound,
            self.no_alternate_is_valid,
            self.max_hdratio_diff_ci_halfwidth,
            self.hdratio_min_improv,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::performance::db;

    #[test]
    fn test_minrtt_p50_lower_bound() {
        let _pathid: db::PathId = db::PathId {
            vip_metro: "gru".to_string(),
            bgp_ip_prefix: "1.0.0.0/24".parse().unwrap(),
        };

        let sum_true = MinRtt50ImprovementSummarizer {
            minrtt50_min_improv: 0,
            max_minrtt50_diff_ci_halfwidth: 100.0,
            no_alternate_is_valid: true,
            compare_lower_bound: true,
        };
        let sum_false = MinRtt50ImprovementSummarizer {
            minrtt50_min_improv: 0,
            max_minrtt50_diff_ci_halfwidth: 100.0,
            no_alternate_is_valid: false,
            compare_lower_bound: true,
        };

        let mut timebin = db::TimeBin::mock_minrtt_p50(0, 15, 10, 100.0);

        let binstats_true = sum_true.summarize(&_pathid, &timebin);
        assert!(binstats_true.is_some());
        let binstats_true = binstats_true.unwrap();
        assert!(!binstats_true.is_shifted);
        assert!((binstats_true.diff_ci - 5.0).abs() < 1e-6);

        let binstats_false = sum_false.summarize(&_pathid, &timebin);
        assert!(binstats_false.is_some());
        let binstats_false = binstats_false.unwrap();
        assert!(!binstats_false.is_shifted);
        assert!((binstats_false.diff_ci - 5.0).abs() < 1e-6);

        timebin.num2route[1] = None;

        let binstats_true = sum_true.summarize(&_pathid, &timebin);
        assert!(binstats_true.is_some());
        let binstats_true = binstats_true.unwrap();
        assert!(!binstats_true.is_shifted);
        assert!(binstats_true.diff_ci.abs() < 1e-6);

        let binstats_false = sum_false.summarize(&_pathid, &timebin);
        assert!(binstats_false.is_none());
    }

    #[test]
    fn test_minrtt_p50_lower_bound_valid() {
        let _pathid: db::PathId = db::PathId {
            vip_metro: "gru".to_string(),
            bgp_ip_prefix: "1.0.0.0/24".parse().unwrap(),
        };

        let sum1 = MinRtt50ImprovementSummarizer {
            minrtt50_min_improv: 0,
            max_minrtt50_diff_ci_halfwidth: 7.0,
            no_alternate_is_valid: false,
            compare_lower_bound: true,
        };
        let sum2 = MinRtt50ImprovementSummarizer {
            minrtt50_min_improv: 0,
            max_minrtt50_diff_ci_halfwidth: 9.0,
            no_alternate_is_valid: false,
            compare_lower_bound: true,
        };

        // ci_halfwidth = 2 * (8 + 8).sqrt() = 8
        let timebin = db::TimeBin::mock_minrtt_p50(0, 20, 10, 8.0);

        let binstats1 = sum1.summarize(&_pathid, &timebin);
        assert!(binstats1.is_none());

        let binstats2 = sum2.summarize(&_pathid, &timebin);
        assert!(binstats2.is_some());
        let binstats2 = binstats2.unwrap();
        assert!(binstats2.is_shifted);
        assert!((binstats2.diff_ci - 10.0).abs() < 1e-6);

        let timebin = db::TimeBin::mock_minrtt_p50(0, 15, 10, 8.0);

        let binstats2 = sum2.summarize(&_pathid, &timebin);
        assert!(binstats2.is_some());
        let binstats2 = binstats2.unwrap();
        assert!(!binstats2.is_shifted);
        assert!((binstats2.diff_ci - 5.0).abs() < 1e-6);
    }

    #[test]
    fn test_hdratio_lower_bound() {
        let _pathid: db::PathId = db::PathId {
            vip_metro: "gru".to_string(),
            bgp_ip_prefix: "1.0.0.0/24".parse().unwrap(),
        };

        let sum_true = HdRatioImprovementSummarizer {
            hdratio_min_improv: 0.0,
            max_hdratio_diff_ci_halfwidth: 0.5,
            no_alternate_is_valid: true,
            compare_lower_bound: true,
        };
        let sum_false = HdRatioImprovementSummarizer {
            hdratio_min_improv: 0.0,
            max_hdratio_diff_ci_halfwidth: 0.5,
            no_alternate_is_valid: false,
            compare_lower_bound: true,
        };

        // ci_halfwidth = 2 * (0.5/100 + 0.5/100).sqrt() = 0.2
        let mut timebin = db::TimeBin::mock_hdratio(0, 0.8, 0.9, 0.5);

        let binstats_true = sum_true.summarize(&_pathid, &timebin);
        assert!(binstats_true.is_some());
        let binstats_true = binstats_true.unwrap();
        assert!(!binstats_true.is_shifted);
        assert!((binstats_true.diff_ci - 0.1).abs() < 1e-6);
        assert!((binstats_true.diff_ci_halfwidth - 0.2).abs() < 1e-6);

        let binstats_false = sum_false.summarize(&_pathid, &timebin);
        assert!(binstats_false.is_some());
        let binstats_false = binstats_false.unwrap();
        assert!(!binstats_false.is_shifted);
        assert!((binstats_false.diff_ci - 0.1).abs() < 1e-6);
        assert!((binstats_false.diff_ci_halfwidth - 0.2).abs() < 1e-6);

        timebin.num2route[1] = None;

        let binstats_true = sum_true.summarize(&_pathid, &timebin);
        assert!(binstats_true.is_some());
        let binstats_true = binstats_true.unwrap();
        assert!(!binstats_true.is_shifted);
        assert!(binstats_true.diff_ci.abs() < 1e-6);
        assert!((binstats_false.diff_ci_halfwidth - 0.2).abs() < 1e-6);

        let binstats_false = sum_false.summarize(&_pathid, &timebin);
        assert!(binstats_false.is_none());
    }

    #[test]
    fn test_hdratio_lower_bound_valid() {
        let _pathid: db::PathId = db::PathId {
            vip_metro: "gru".to_string(),
            bgp_ip_prefix: "1.0.0.0/24".parse().unwrap(),
        };

        let sum1 = HdRatioImprovementSummarizer {
            hdratio_min_improv: 0.0,
            max_hdratio_diff_ci_halfwidth: 0.15,
            no_alternate_is_valid: false,
            compare_lower_bound: true,
        };
        let sum2 = HdRatioImprovementSummarizer {
            hdratio_min_improv: 0.0,
            max_hdratio_diff_ci_halfwidth: 0.25,
            no_alternate_is_valid: false,
            compare_lower_bound: true,
        };

        // ci_halfwidth = 2 * (0.5/100 + 0.5/100).sqrt() = 0.2
        let timebin = db::TimeBin::mock_hdratio(0, 0.70, 0.95, 0.5);

        let binstats1 = sum1.summarize(&_pathid, &timebin);
        assert!(binstats1.is_none());

        let binstats2 = sum2.summarize(&_pathid, &timebin);
        assert!(binstats2.is_some());
        let binstats2 = binstats2.unwrap();
        assert!(binstats2.is_shifted);
        assert!((binstats2.diff_ci - 0.25).abs() < 1e-6);
        assert!((binstats2.diff_ci_halfwidth - 0.2).abs() < 1e-6);

        let timebin = db::TimeBin::mock_hdratio(0, 0.8, 0.95, 0.5);

        let binstats2 = sum2.summarize(&_pathid, &timebin);
        assert!(binstats2.is_some());
        let binstats2 = binstats2.unwrap();
        assert!(!binstats2.is_shifted);
        assert!((binstats2.diff_ci - 0.15).abs() < 1e-6);
        assert!((binstats2.diff_ci_halfwidth - 0.2).abs() < 1e-6);
    }
}
