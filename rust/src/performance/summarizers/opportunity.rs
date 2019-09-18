use std::collections::HashMap;
use std::fs;
use std::io;
use std::io::Write;
use std::path::PathBuf;

use log::{error, info};
use serde_pickle;

use crate::performance::db;
use crate::performance::perfstats;
use crate::performance::perfstats::TimeBinSummarizer;

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

#[derive(Clone, Copy, Debug)]
pub struct HdRatioBootstrapDifferenceImprovementSummarizer {
    pub hdratio_boot_min_improv: f32,
    pub max_hdratio_boot_diff_ci_fullwidth: f32,
    pub compare_lower_bound: bool,
}

impl TimeBinSummarizer for MinRtt50ImprovementSummarizer {
    fn summarize(&self, _pathid: &db::PathId, bin: &db::TimeBin) -> perfstats::TimeBinSummary {
        match (
            bin.get_primary_route(),
            bin.get_best_alternate(db::RouteInfo::compare_median_minrtt),
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
                        bitmask: compute_bitmask(primary, bestalt),
                        is_shifted: limit >= f32::from(self.minrtt50_min_improv),
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
            bin.get_primary_route().as_ref().unwrap(),
            bin.get_best_alternate(db::RouteInfo::compare_median_minrtt).as_ref().unwrap(),
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
    fn summarize(&self, _pathid: &db::PathId, bin: &db::TimeBin) -> perfstats::TimeBinSummary {
        match (bin.get_primary_route(), bin.get_best_alternate(db::RouteInfo::compare_hdratio)) {
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
            bin.get_primary_route().as_ref().unwrap(),
            bin.get_best_alternate(db::RouteInfo::compare_hdratio).as_ref().unwrap(),
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
    fn summarize(&self, _pathid: &db::PathId, bin: &db::TimeBin) -> perfstats::TimeBinSummary {
        match (
            bin.get_primary_route(),
            bin.get_best_alternate(db::RouteInfo::compare_median_minrtt),
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
                        bitmask: compute_bitmask(primary, bestalt),
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
            bin.get_primary_route().as_ref().unwrap(),
            bin.get_best_alternate(db::RouteInfo::compare_median_hdratio).as_ref().unwrap(),
        )
    }
    fn prefix(&self) -> String {
        format!(
            "hdratio50--opp--bound-{}--halfwidth-{:0.2}--min-improv-{:0.2}",
            self.compare_lower_bound,
            self.max_hdratio50_diff_ci_halfwidth,
            self.hdratio50_min_improv,
        )
    }
}

impl TimeBinSummarizer for HdRatioBootstrapDifferenceImprovementSummarizer {
    fn summarize(&self, _pathid: &db::PathId, bin: &db::TimeBin) -> perfstats::TimeBinSummary {
        match (
            bin.get_primary_route(),
            bin.get_best_alternate(db::RouteInfo::compare_hdratio_bootstrap),
        ) {
            (None, _) => perfstats::TimeBinSummary::NoRoute,
            (_, None) => perfstats::TimeBinSummary::NoRoute,
            (Some(ref primary), Some(ref bestalt)) => {
                let (lb, diff, ub) = db::RouteInfo::hdratio_boot_diff_ci(bestalt, primary);
                let fullwidth = ub - lb;
                if fullwidth > self.max_hdratio_boot_diff_ci_fullwidth {
                    perfstats::TimeBinSummary::WideConfidenceInterval
                } else {
                    let limit: f32 = if self.compare_lower_bound {
                        lb
                    } else {
                        diff
                    };
                    perfstats::TimeBinSummary::Valid(perfstats::TimeBinStats {
                        bytes: bin.bytes_acked_sum,
                        diff_ci: diff,
                        // BUG: Next line assumes symmetry; will impact shaded areas in CDFs.
                        diff_ci_halfwidth: fullwidth / 2.0,
                        primary_peer_type: primary.peer_type,
                        alternate_peer_type: bestalt.peer_type,
                        bitmask: compute_bitmask(primary, bestalt),
                        is_shifted: limit >= self.hdratio_boot_min_improv,
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
            bin.get_primary_route().as_ref().unwrap(),
            bin.get_best_alternate(db::RouteInfo::compare_hdratio_bootstrap).as_ref().unwrap(),
        )
    }
    fn prefix(&self) -> String {
        format!(
            "hdratioboot--opp--bound-{}--fullwidth-{:0.2}--min-improv-{:0.2}",
            self.compare_lower_bound,
            self.max_hdratio_boot_diff_ci_fullwidth,
            self.hdratio_boot_min_improv,
        )
    }
}

fn compute_bitmask(primary: &db::RouteInfo, bestalt: &db::RouteInfo) -> u8 {
    let mut bitmask: u8 = 0;
    if bestalt.apm_route_num == 1 {
        bitmask |= perfstats::TimeBinStats::BEST_ALTERNATE_IS_BGP_PREFERRED;
    }
    if bestalt.bgp_as_path_len > primary.bgp_as_path_len {
        bitmask |= perfstats::TimeBinStats::ALTERNATE_IS_LONGER;
    }
    if bestalt.bgp_as_path_prepends > primary.bgp_as_path_prepends {
        bitmask |= perfstats::TimeBinStats::ALTERNATE_IS_PREPENDED_MORE;
    }
    bitmask
}

// Return triple is total_shifted, total_shifted_longer, total_shifted_alt_is_prepended_more
pub(crate) fn compute_opportunity_vs_relationship(
    dbsum: &perfstats::DBSummary,
) -> HashMap<(db::PeerType, db::PeerType), (u128, u128, u128)> {
    let mut peering2counters: HashMap<(db::PeerType, db::PeerType), (u128, u128, u128)> =
        HashMap::new();
    for psum in dbsum.pathid2summary.values() {
        for binstats in psum.time2binstats.values() {
            if !binstats.is_shifted {
                continue;
            }
            let bytes: u128 = u128::from(binstats.bytes);
            let longer: u128 =
                if binstats.bitmask & perfstats::TimeBinStats::ALTERNATE_IS_LONGER != 0 {
                    bytes
                } else {
                    0
                };
            let prepended_more: u128 =
                if binstats.bitmask & perfstats::TimeBinStats::ALTERNATE_IS_PREPENDED_MORE != 0 {
                    bytes
                } else {
                    0
                };
            peering2counters
                .entry((binstats.primary_peer_type, binstats.alternate_peer_type))
                .and_modify(|e| {
                    e.0 += bytes;
                    e.1 += longer;
                    e.2 += prepended_more;
                })
                .or_insert((bytes, longer, prepended_more));
        }
    }
    peering2counters
}

pub fn dump_opportunity_vs_relationship(
    dbsum: &perfstats::DBSummary,
    path: &PathBuf,
) -> Result<(), io::Error> {
    let peering2counters = compute_opportunity_vs_relationship(dbsum);

    let mut filepath = path.clone();
    filepath.push("opp-vs-relationship.txt");
    let file =
        fs::OpenOptions::new().read(true).write(true).truncate(true).create(true).open(filepath)?;
    let mut bw = io::BufWriter::new(file);
    for (peering, counters) in &peering2counters {
        writeln!(
            bw,
            "{:?} {:?} --- {} {} {}",
            peering.0, peering.1, counters.0, counters.1, counters.2
        )?;
    }

    let mut peering2bigint: HashMap<(u8, u8), (String, String, String)> = HashMap::new();
    for ((pritype, alttype), (valid, longer, prepended_more)) in peering2counters {
        peering2bigint.insert(
            (pritype as u8, alttype as u8),
            (valid.to_string(), longer.to_string(), prepended_more.to_string()),
        );
    }

    let mut filepath = path.clone();
    filepath.push("opp-vs-relationship.pickle");
    let file =
        fs::OpenOptions::new().read(true).write(true).truncate(true).create(true).open(filepath)?;
    let mut bw = io::BufWriter::new(file);
    serde_pickle::to_writer(&mut bw, &peering2bigint, true).unwrap_or_else(|e| {
        error!("{}", e);
        info!("could not dump opp-vs-relationship table as pickle");
    });

    Ok(())
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

        let mut timebin = db::TimeBin::mock_minrtt_p50(0, 15, 10, 10);

        let binsum = sum.summarize(&_pathid, &timebin);
        if let perfstats::TimeBinSummary::Valid(binstats) = binsum {
            assert!(!binstats.is_shifted);
            assert!((binstats.diff_ci - 5.0).abs() < 1e-6);
        } else {
            unreachable!();
        }

        timebin.num2route[1] = None;

        let binsum = sum.summarize(&_pathid, &timebin);
        assert!(binsum == perfstats::TimeBinSummary::NoRoute);
    }

    #[test]
    fn test_minrtt_p50_lower_bound_valid() {
        let _pathid: db::PathId = db::tests::make_path_id();

        let sum1 = MinRtt50ImprovementSummarizer {
            minrtt50_min_improv: 0,
            max_minrtt50_diff_ci_halfwidth: 5.0,
            compare_lower_bound: true,
        };
        let sum2 = MinRtt50ImprovementSummarizer {
            minrtt50_min_improv: 0,
            max_minrtt50_diff_ci_halfwidth: 6.0,
            compare_lower_bound: true,
        };

        // ci_halfwidth = 2 * (4 + 4).sqrt() = 2*2.83
        let timebin = db::TimeBin::mock_minrtt_p50(0, 20, 10, 4);

        let binsum1 = sum1.summarize(&_pathid, &timebin);
        assert!(binsum1 == perfstats::TimeBinSummary::WideConfidenceInterval);

        let binsum2 = sum2.summarize(&_pathid, &timebin);
        if let perfstats::TimeBinSummary::Valid(binstats) = binsum2 {
            assert!(binstats.is_shifted);
            assert!((binstats.diff_ci - 10.0).abs() < 1e-6);
        } else {
            unreachable!();
        }

        let timebin = db::TimeBin::mock_minrtt_p50(0, 15, 10, 4);

        let binsum2 = sum2.summarize(&_pathid, &timebin);
        if let perfstats::TimeBinSummary::Valid(binstats) = binsum2 {
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
        if let perfstats::TimeBinSummary::Valid(binstats) = binsum {
            assert!(!binstats.is_shifted);
            assert!((binstats.diff_ci - 0.1).abs() < 1e-6);
            assert!((binstats.diff_ci_halfwidth - 0.2).abs() < 1e-6);
        } else {
            unreachable!();
        }

        timebin.num2route[1] = None;

        let binsum = sum.summarize(&_pathid, &timebin);
        assert!(binsum == perfstats::TimeBinSummary::NoRoute);
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
        assert!(binstats1 == perfstats::TimeBinSummary::WideConfidenceInterval);

        let binsum = sum2.summarize(&_pathid, &timebin);
        if let perfstats::TimeBinSummary::Valid(binstats) = binsum {
            assert!(binstats.is_shifted);
            assert!((binstats.diff_ci - 0.25).abs() < 1e-6);
            assert!((binstats.diff_ci_halfwidth - 0.2).abs() < 1e-6);
        } else {
            unreachable!();
        }

        let timebin = db::TimeBin::mock_hdratio(0, 0.8, 0.95, 0.5);

        let binsum = sum2.summarize(&_pathid, &timebin);
        if let perfstats::TimeBinSummary::Valid(binstats) = binsum {
            assert!(!binstats.is_shifted);
            assert!((binstats.diff_ci - 0.15).abs() < 1e-6);
            assert!((binstats.diff_ci_halfwidth - 0.2).abs() < 1e-6);
        } else {
            unreachable!();
        }
    }

    #[test]
    fn test_hdratio_boot_diff() {
        let _pathid: db::PathId = db::tests::make_path_id();

        let sum1 = HdRatioBootstrapDifferenceImprovementSummarizer {
            hdratio_boot_min_improv: 0.2,
            max_hdratio_boot_diff_ci_fullwidth: 0.05,
            compare_lower_bound: true,
        };
        let sum2 = HdRatioBootstrapDifferenceImprovementSummarizer {
            hdratio_boot_min_improv: 0.2,
            max_hdratio_boot_diff_ci_fullwidth: 0.15,
            compare_lower_bound: true,
        };

        let timebin = db::TimeBin::mock_hdratio_boot(0, 0.70, 0.95, 0.2, 0.3);

        let binstats1 = sum1.summarize(&_pathid, &timebin);
        assert!(binstats1 == perfstats::TimeBinSummary::WideConfidenceInterval);

        let binsum = sum2.summarize(&_pathid, &timebin);
        if let perfstats::TimeBinSummary::Valid(binstats) = binsum {
            assert!(binstats.is_shifted);
            assert!((binstats.diff_ci - 0.25).abs() < 1e-6);
            assert!((binstats.diff_ci_halfwidth - 0.05).abs() < 1e-6);
        } else {
            unreachable!();
        }

        let timebin = db::TimeBin::mock_hdratio_boot(0, 0.8, 0.95, 0.1, 0.2);

        let binsum = sum2.summarize(&_pathid, &timebin);
        if let perfstats::TimeBinSummary::Valid(binstats) = binsum {
            assert!(!binstats.is_shifted);
            assert!((binstats.diff_ci - 0.15).abs() < 1e-6);
            assert!((binstats.diff_ci_halfwidth - 0.05).abs() < 1e-6);
        } else {
            unreachable!();
        }

        let timebin = db::TimeBin::mock_hdratio_boot(0, 0.95, 0.70, -0.3, -0.2);

        let binstats1 = sum1.summarize(&_pathid, &timebin);
        assert!(binstats1 == perfstats::TimeBinSummary::WideConfidenceInterval);

        let binsum = sum2.summarize(&_pathid, &timebin);
        if let perfstats::TimeBinSummary::Valid(binstats) = binsum {
            assert!(!binstats.is_shifted);
            assert!((binstats.diff_ci - (-0.25)).abs() < 1e-6);
            assert!((binstats.diff_ci_halfwidth - 0.05).abs() < 1e-6);
        } else {
            unreachable!();
        }
    }

    #[test]
    fn test_opportunity_vs_peering_relationship() {}
}
