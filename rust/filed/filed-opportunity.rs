#[derive(Clone, Copy, Debug)]
pub struct HdRatioImprovementSummarizer {
    pub hdratio_min_improv: f32,
    pub max_hdratio_diff_ci_halfwidth: f32,
    pub compare_lower_bound: bool,
}

impl TimeBinSummarizer for HdRatioImprovementSummarizer {
    fn summarize(&self, _pathid: &db::PathId, bin: &db::TimeBin) -> perfstats::TimeBinSummary {
        match (
            bin.get_primary_route_hdratio(),
            bin.get_best_alternate_hdratio(db::RouteInfo::compare_hdratio),
        ) {
            (None, _) => perfstats::TimeBinSummary::NoRoute,
            (_, None) => perfstats::TimeBinSummary::NoRoute,
            (Some(ref primary), Some(ref bestalt)) => {
                let (diff, halfwidth) = db::RouteInfo::hdratio_diff_ci(bestalt, primary);
                if halfwidth > self.max_hdratio_diff_ci_halfwidth {
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
                        bitmask: compute_bitmask(primary, bestalt),
                        is_shifted: limit >= self.hdratio_min_improv,
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
            bin.get_primary_route_hdratio().as_ref().unwrap(),
            bin.get_best_alternate_hdratio(db::RouteInfo::compare_hdratio).as_ref().unwrap(),
        )
    }
    fn prefix(&self) -> String {
        format!(
            "hdratio--opp--bound-{}--halfwidth-{:0.2}--min-improv-{:0.2}",
            self.compare_lower_bound, self.max_hdratio_diff_ci_halfwidth, self.hdratio_min_improv,
        )
    }
}