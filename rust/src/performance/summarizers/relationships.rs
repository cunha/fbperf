use crate::performance::db;
use crate::performance::perfstats;
use crate::performance::perfstats::TimeBinSummarizer;

#[derive(Clone, Copy, Debug)]
pub struct MinRtt50RelationshipSummarizer {
    pub primary_bitmask: u32,
    pub alternate_bitmask: u32,
    pub minrtt50_min_improv: f32,
    pub max_minrtt50_diff_ci_halfwidth: f32,
    pub compare_lower_bound: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct HdRatio50RelationshipSummarizer {
    pub primary_bitmask: u32,
    pub alternate_bitmask: u32,
    pub hdratio50_min_improv: f32,
    pub max_hdratio50_diff_ci_halfwidth: f32,
    pub compare_lower_bound: bool,
}

fn check_valid<F>(rtinfo: &db::RouteInfo, bitmask: u32, metric_valid: F) -> bool
where
    F: Fn(&db::RouteInfo) -> bool,
{
    ((bitmask & (1 << rtinfo.peer_type as u8)) != 0) && metric_valid(rtinfo)
}

impl TimeBinSummarizer for MinRtt50RelationshipSummarizer {
    fn summarize(&self, _pathid: &db::PathId, bin: &db::TimeBin) -> perfstats::TimeBinSummary {
        match (
            bin.get_primary_route(|r| {
                check_valid(r, self.primary_bitmask, db::RouteInfo::minrtt_valid)
            }),
            bin.get_first_alternate(|r| {
                check_valid(r, self.alternate_bitmask, db::RouteInfo::minrtt_valid)
            }),
        ) {
            (None, _) => perfstats::TimeBinSummary::NoRoute,
            (_, None) => perfstats::TimeBinSummary::NoRoute,
            (Some(ref primary), Some(ref bestalt)) => {
                let (diff, halfwidth) = db::RouteInfo::minrtt_median_diff_ci(primary, bestalt);
                if halfwidth > self.max_minrtt50_diff_ci_halfwidth {
                    perfstats::TimeBinSummary::WideConfidenceInterval
                } else {
                    let limit: f32 = if self.compare_lower_bound {
                        diff - halfwidth
                    } else {
                        diff
                    };
                    perfstats::TimeBinSummary::Valid(perfstats::TimeBinStats {
                        bytes: bin.bytes_acked_sum,
                        diff_ci: diff,
                        diff_ci_halfwidth: halfwidth,
                        primary_peer_type: primary.peer_type,
                        alternate_peer_type: bestalt.peer_type,
                        bitmask: 0,
                        is_shifted: limit >= self.minrtt50_min_improv,
                    })
                }
            }
        }
    }
    fn get_routes<'s: 'd, 'd>(
        &'s self,
        pathid: &db::PathId,
        time: u64,
        db: &'d db::DB,
    ) -> (&'d db::RouteInfo, &'d db::RouteInfo) {
        let bin = &db.pathid2info[pathid].time2bin[&time];
        (
            bin.get_primary_route(|r| {
                check_valid(r, self.primary_bitmask, db::RouteInfo::minrtt_valid)
            })
            .as_ref()
            .unwrap(),
            bin.get_first_alternate(|r| {
                check_valid(r, self.alternate_bitmask, db::RouteInfo::minrtt_valid)
            })
            .as_ref()
            .unwrap(),
        )
    }
    fn prefix(&self) -> String {
        format!(
            "minrtt50--relationships-{}-{}--bound-{}--diff-thresh-{:0.2}--diff-ci-{:0.2}",
            self.primary_bitmask,
            self.alternate_bitmask,
            self.compare_lower_bound,
            self.minrtt50_min_improv,
            self.max_minrtt50_diff_ci_halfwidth
        )
    }
}

impl TimeBinSummarizer for HdRatio50RelationshipSummarizer {
    fn summarize(&self, _pathid: &db::PathId, bin: &db::TimeBin) -> perfstats::TimeBinSummary {
        match (
            bin.get_primary_route(|r| {
                check_valid(r, self.primary_bitmask, db::RouteInfo::hdratio_valid)
            }),
            bin.get_first_alternate(|r| {
                check_valid(r, self.alternate_bitmask, db::RouteInfo::hdratio_valid)
            }),
        ) {
            (None, _) => perfstats::TimeBinSummary::NoRoute,
            (_, None) => perfstats::TimeBinSummary::NoRoute,
            (Some(ref primary), Some(ref bestalt)) => {
                let (diff, halfwidth) = db::RouteInfo::hdratio_median_diff_ci(bestalt, primary);
                if halfwidth > self.max_hdratio50_diff_ci_halfwidth {
                    perfstats::TimeBinSummary::WideConfidenceInterval
                } else {
                    let limit: f32 = if self.compare_lower_bound {
                        diff - halfwidth
                    } else {
                        diff
                    };
                    perfstats::TimeBinSummary::Valid(perfstats::TimeBinStats {
                        bytes: bin.bytes_acked_sum,
                        diff_ci: diff,
                        diff_ci_halfwidth: halfwidth,
                        primary_peer_type: primary.peer_type,
                        alternate_peer_type: bestalt.peer_type,
                        bitmask: 0,
                        is_shifted: limit >= self.hdratio50_min_improv,
                    })
                }
            }
        }
    }
    fn get_routes<'s: 'd, 'd>(
        &'s self,
        pathid: &db::PathId,
        time: u64,
        db: &'d db::DB,
    ) -> (&'d db::RouteInfo, &'d db::RouteInfo) {
        let bin = &db.pathid2info[pathid].time2bin[&time];
        (
            bin.get_primary_route(|r| {
                check_valid(r, self.primary_bitmask, db::RouteInfo::hdratio_valid)
            })
            .as_ref()
            .unwrap(),
            bin.get_first_alternate(|r| {
                check_valid(r, self.alternate_bitmask, db::RouteInfo::hdratio_valid)
            })
            .as_ref()
            .unwrap(),
        )
    }
    fn prefix(&self) -> String {
        format!(
            "hdratio50--relationships-{}-{}--bound-{}--diff-thresh-{}--diff-ci-{:0.2}",
            self.primary_bitmask,
            self.alternate_bitmask,
            self.compare_lower_bound,
            self.hdratio50_min_improv,
            self.max_hdratio50_diff_ci_halfwidth
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::performance::db;

    #[test]
    fn test_minrtt_p50_relationships() {
        let _pathid: db::PathId = db::tests::make_path_id();

        let sum = MinRtt50RelationshipSummarizer {
            primary_bitmask: 7,
            alternate_bitmask: 8,
            minrtt50_min_improv: 5.0,
            max_minrtt50_diff_ci_halfwidth: 10.0,
            compare_lower_bound: true,
        };

        let mut timebin = db::TimeBin::mock_minrtt_p50(0, 40, 20, 5);

        let binsum = sum.summarize(&_pathid, &timebin);
        if let perfstats::TimeBinSummary::Valid(binstats) = binsum {
            assert!(binstats.is_shifted);
            assert!((binstats.diff_ci - 20.0).abs() < 1e-6);
        } else {
            unreachable!();
        }

        let mut rtinfo = timebin.num2route[1].clone().unwrap();
        rtinfo.peer_type = db::PeerType::PeeringPublic;
        timebin.num2route[1] = Some(rtinfo);

        let binsum = sum.summarize(&_pathid, &timebin);
        assert!(binsum == perfstats::TimeBinSummary::NoRoute);

        let sum = MinRtt50RelationshipSummarizer {
            primary_bitmask: 1,
            alternate_bitmask: 2,
            minrtt50_min_improv: 5.0,
            max_minrtt50_diff_ci_halfwidth: 10.0,
            compare_lower_bound: true,
        };

        let mut timebin = db::TimeBin::mock_minrtt_p50(0, 40, 20, 5);

        let binsum = sum.summarize(&_pathid, &timebin);
        assert!(binsum == perfstats::TimeBinSummary::NoRoute);

        let mut rtinfo = timebin.num2route[1].clone().unwrap();
        rtinfo.peer_type = db::PeerType::PeeringPublic;
        timebin.num2route[1] = Some(rtinfo);

        let binsum = sum.summarize(&_pathid, &timebin);
        if let perfstats::TimeBinSummary::Valid(binstats) = binsum {
            assert!(binstats.is_shifted);
            assert!((binstats.diff_ci - 20.0).abs() < 1e-6);
        } else {
            unreachable!();
        }
    }

    #[test]
    fn test_hdratio_p50_relationships() {
        let _pathid: db::PathId = db::tests::make_path_id();

        let sum = HdRatio50RelationshipSummarizer {
            primary_bitmask: 7,
            alternate_bitmask: 8,
            hdratio50_min_improv: 0.05,
            max_hdratio50_diff_ci_halfwidth: 0.2,
            compare_lower_bound: true,
        };

        let mut timebin = db::TimeBin::mock_hdratio_p50(0, 0.7, 0.9, 0.08);

        let binsum = sum.summarize(&_pathid, &timebin);
        if let perfstats::TimeBinSummary::Valid(binstats) = binsum {
            assert!(binstats.is_shifted);
            assert!((binstats.diff_ci - 0.2).abs() < 1e-6);
        } else {
            unreachable!();
        }

        let mut rtinfo = timebin.num2route[1].clone().unwrap();
        rtinfo.peer_type = db::PeerType::PeeringPublic;
        timebin.num2route[1] = Some(rtinfo);

        let binsum = sum.summarize(&_pathid, &timebin);
        assert!(binsum == perfstats::TimeBinSummary::NoRoute);

        let sum = HdRatio50RelationshipSummarizer {
            primary_bitmask: 1,
            alternate_bitmask: 2,
            hdratio50_min_improv: 0.05,
            max_hdratio50_diff_ci_halfwidth: 0.2,
            compare_lower_bound: true,
        };

        let mut timebin = db::TimeBin::mock_hdratio_p50(0, 0.7, 0.9, 0.08);

        let binsum = sum.summarize(&_pathid, &timebin);
        assert!(binsum == perfstats::TimeBinSummary::NoRoute);

        let mut rtinfo = timebin.num2route[1].clone().unwrap();
        rtinfo.peer_type = db::PeerType::PeeringPublic;
        timebin.num2route[1] = Some(rtinfo);

        let binsum = sum.summarize(&_pathid, &timebin);
        if let perfstats::TimeBinSummary::Valid(binstats) = binsum {
            assert!(binstats.is_shifted);
            assert!((binstats.diff_ci - 0.2).abs() < 1e-6);
        } else {
            unreachable!();
        }
    }
}
