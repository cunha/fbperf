use std::collections::{BTreeMap, HashMap};
use std::convert::TryFrom;
use std::io;
use std::path::PathBuf;
use std::rc::Rc;

use num_enum::TryFromPrimitive;

use crate::cdf;
use crate::performance::db;

pub trait TimeBinSummarizer {
    fn summarize(&self, bin: &db::TimeBin) -> Option<TimeBinStats>;
    fn prefix(&self) -> String;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, TryFromPrimitive)]
#[repr(u8)]
pub enum TemporalBehavior {
    Uneventful = 0,
    Continuous = 1,
    Diurnal = 2,
    Episodic = 3,
    Undersampled = 4,
    Uninitialized = 5,
    SIZE = 6,
}

#[derive(Clone, Copy, Debug)]
pub struct TemporalConfig {
    pub bin_duration_secs: u32,
    pub min_days: u32,
    pub min_frac_valid_bins: f32,
    pub continuous_min_frac_shifted_bins: f32,
    pub diurnal_min_frac_bad_bins: f32,
    pub diurnal_bad_bin_min_prob_shift: f32,
    pub uneventful_max_frac_shifted_bins: f32,
}

#[derive(Clone, Copy, Debug)]
pub struct MinRtt50ImprovementSummarizer {
    pub minrtt50_diff_min_improv: u16,
    pub max_minrtt50_diff_ci_halfwidth: u16,
}

#[derive(Debug, Default)]
pub struct DBSummary {
    pub pathid2summary: HashMap<Rc<db::PathId>, PathSummary>,
    pub valid_bytes: u64,
    behavior_valid_bytes: [u64; TemporalBehavior::SIZE as usize],
    behavior_total_bytes: [u64; TemporalBehavior::SIZE as usize],
}

#[derive(Debug, Default)]
pub struct PathSummary {
    time2binstats: BTreeMap<u64, TimeBinStats>,
    day2shifts: HashMap<u32, u32>,
    distinct_shifts: u32,
    shifted_bins: u32,
    shifted_bytes: u64,
    valid_bytes: u64,
    frac_bad_bins: f32,
    temporal_behavior: TemporalBehavior,
}

#[derive(Debug, Default)]
pub struct TimeBinStats {
    pub diff_ci: f32,
    pub diff_ci_halfwidth: f32,
    pub is_shifted: bool,
}

impl DBSummary {
    pub fn build<T>(db: &db::DB, summarizer: &T, tempconfig: &TemporalConfig) -> DBSummary
    where
        T: TimeBinSummarizer,
    {
        let mut dbsum = DBSummary::default();
        for (pid, time2bin) in &db.pathid2time2bin {
            let psum = PathSummary::build(time2bin, summarizer, tempconfig);
            dbsum.valid_bytes += psum.valid_bytes;
            dbsum.behavior_valid_bytes[psum.temporal_behavior as usize] += psum.valid_bytes;
            dbsum.behavior_total_bytes[psum.temporal_behavior as usize] += db.pathid2traffic[pid];
            dbsum.pathid2summary.insert(Rc::clone(&pid), psum);
        }
        dbsum
    }

    pub fn dump(&self, path: &PathBuf) -> Result<(), io::Error> {
        self.dump_cdfs(path)?;
        self.dump_temporal_tables(&mut std::io::stdout())?;
        Ok(())
    }

    pub fn dump_cdfs(&self, path: &PathBuf) -> Result<(), io::Error> {
        // ci-{lb,diff,up}/weighted
        let mut data: Vec<(f32, f32)> = Vec::new();
        for psum in self.pathid2summary.values() {

        }


        // frac-improved-bins/weighted
        // frac-improved-bytes/weighted
        // num-distinct-shifts/weighted
        // num-shifts-per-day/weighted
        Ok(())
    }

    pub fn dump_temporal_tables(&self, writer: &mut dyn io::Write) -> Result<(), io::Error> {
        let normalized_behaviors: [u64; TemporalBehavior::SIZE as usize] = [0, 1, 1, 1, 0, 0];
        let total: u64 = self.behavior_total_bytes.iter().sum::<u64>();
        let norm_total: u64 = self
            .behavior_total_bytes
            .iter()
            .enumerate()
            .fold(0u64, |acc, (i, v)| acc + normalized_behaviors[i] * (*v));
        writeln!(writer, "behavior bytes perc norm")?;
        for (i, valid) in self.behavior_valid_bytes.iter().enumerate() {
            let behavior: TemporalBehavior = TemporalBehavior::try_from(i as u8).unwrap();
            let frac_valid: f32 = 100.0 * (*valid as f32) / (total as f32);
            write!(writer, "{:?} {} {:0.2}%", behavior, valid, frac_valid)?;
            if normalized_behaviors[i] == 1 {
                let norm_valid: f32 = 100.0 * (*valid as f32) / (norm_total as f32);
                writeln!(writer, " {:0.2}%", norm_valid)?;
            } else {
                writeln!(writer, " ---")?;
            }
        }
        Ok(())
    }
}

impl PathSummary {
    fn build<T>(
        time2bin: &BTreeMap<u64, db::TimeBin>,
        summarizer: &T,
        tempconfig: &TemporalConfig,
    ) -> PathSummary
    where
        T: TimeBinSummarizer,
    {
        let mut psum = PathSummary::default();
        let mut is_shifted = false;
        for (time, timebin) in time2bin {
            // This requires that time2bin is a BTreeMap as
            // computing the number of distinct shift events
            // requires processing TimeBins in time order.
            if let Some(binstats) = summarizer.summarize(timebin) {
                psum.valid_bytes += timebin.bytes_acked_sum;
                let e = psum.day2shifts.entry((time / 86400) as u32);
                if binstats.is_shifted {
                    psum.shifted_bins += 1;
                    psum.shifted_bytes += timebin.bytes_acked_sum;
                    if !is_shifted {
                        psum.distinct_shifts += 1;
                    }
                    e.and_modify(|e| *e += 1).or_insert(1);
                } else {
                    e.or_insert(0);
                }
                is_shifted = binstats.is_shifted;
                psum.time2binstats.insert(*time, binstats);
            }
        }
        psum.compute_frac_bad_bins(tempconfig);
        psum.classify(time2bin.len() as u32, tempconfig);
        psum
    }

    fn classify(&mut self, bins: u32, config: &TemporalConfig) {
        let valid_bins: f32 = self.time2binstats.len() as f32;
        let frac_valid: f32 = valid_bins / bins as f32;
        if frac_valid < config.min_frac_valid_bins {
            self.temporal_behavior = TemporalBehavior::Undersampled;
        } else {
            let frac_shift: f32 = self.shifted_bins as f32 / valid_bins;
            if frac_shift < config.uneventful_max_frac_shifted_bins {
                self.temporal_behavior = TemporalBehavior::Uneventful;
            } else if frac_shift >= config.continuous_min_frac_shifted_bins {
                self.temporal_behavior = TemporalBehavior::Continuous;
            } else if self.frac_bad_bins >= config.diurnal_min_frac_bad_bins {
                self.temporal_behavior = TemporalBehavior::Diurnal;
            } else {
                self.temporal_behavior = TemporalBehavior::Episodic;
            }
        }
    }

    fn compute_frac_bad_bins(&mut self, config: &TemporalConfig) {
        let num_days: u32 = self.day2shifts.len() as u32;
        if num_days < config.min_days {
            self.frac_bad_bins = 0.0;
            return;
        }
        let min_shifts: u32 = (config.diurnal_bad_bin_min_prob_shift * num_days as f32) as u32;
        let bins_per_day: usize = (60 * 60 * 24 / config.bin_duration_secs) as usize;
        let mut offset_shift_counts = vec![0u32; bins_per_day];
        self.time2binstats.iter().for_each(|(t, bs)| {
            if bs.is_shifted {
                offset_shift_counts[compute_offset(*t, config.bin_duration_secs)] += 1;
            }
        });
        let num_bad_bins: u32 = compute_num_bad_bins(&offset_shift_counts, min_shifts);
        self.frac_bad_bins = num_bad_bins as f32 / bins_per_day as f32;
    }
}

impl Default for TemporalBehavior {
    fn default() -> TemporalBehavior {
        TemporalBehavior::Uninitialized
    }
}

impl TimeBinSummarizer for MinRtt50ImprovementSummarizer {
    fn summarize(&self, bin: &db::TimeBin) -> Option<TimeBinStats> {
        match (
            bin.get_primary_route(),
            bin.get_best_alternate(db::RouteInfo::compare_median_minrtt),
        ) {
            (None, _) => None,
            (Some(_), None) => Some(TimeBinStats::default()),
            (Some(ref primary), Some(ref bestalt)) => {
                let (diff, halfwidth) = db::RouteInfo::minrtt_median_diff_ci(primary, bestalt);
                if halfwidth > f32::from(self.max_minrtt50_diff_ci_halfwidth) {
                    None
                } else {
                    Some(TimeBinStats {
                        diff_ci: diff,
                        diff_ci_halfwidth: halfwidth,
                        is_shifted: diff <= -f32::from(self.minrtt50_diff_min_improv),
                    })
                }
            }
        }
    }
    fn prefix(&self) -> String {
        format!(
            "minrtt50--min-improv-{}--max-diff-ci-size-{}",
            self.minrtt50_diff_min_improv, self.max_minrtt50_diff_ci_halfwidth
        )
    }
}

fn compute_num_bad_bins(offset_shift_counts: &[u32], min_shifts: u32) -> u32 {
    offset_shift_counts.iter().fold(0u32, |acc, e| {
        if *e >= min_shifts {
            acc + 1
        } else {
            acc
        }
    })
}

fn compute_offset(time: u64, bin_duration_secs: u32) -> usize {
    ((time % 86400) / u64::from(bin_duration_secs)) as usize
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::performance::db;

    const BIN_DURATION_SECS: u64 = 900;
    const NULL_TEMPCONFIG: TemporalConfig = TemporalConfig {
        bin_duration_secs: 900,
        min_days: 7,
        min_frac_valid_bins: 1.0,
        continuous_min_frac_shifted_bins: 1.0,
        diurnal_min_frac_bad_bins: 1.0,
        diurnal_bad_bin_min_prob_shift: 1.0,
        uneventful_max_frac_shifted_bins: 1.0,
    };
    const DEFAULT_TEMPCONFIG: TemporalConfig = TemporalConfig {
        bin_duration_secs: 900,
        min_days: 7,
        min_frac_valid_bins: 0.8,
        continuous_min_frac_shifted_bins: 0.8,
        diurnal_min_frac_bad_bins: 0.25,
        diurnal_bad_bin_min_prob_shift: 0.8,
        uneventful_max_frac_shifted_bins: 0.25,
    };

    #[test]
    fn test_path_summary_no_valid() {
        let summarizer = MinRtt50ImprovementSummarizer {
            minrtt50_diff_min_improv: 2,
            max_minrtt50_diff_ci_halfwidth: 15,
        };

        let time2bin = db::TimeBin::mock_week(BIN_DURATION_SECS, 50, 51, 100.0, 50, 51, 100.0);
        let psum = PathSummary::build(&time2bin, &summarizer, &NULL_TEMPCONFIG);
        assert!(psum.time2binstats.is_empty());
        assert!(psum.day2shifts.is_empty());
        assert!(psum.day2shifts.values().fold(true, |_, e| *e == 0));
        assert!(psum.distinct_shifts == 0);
        assert!(psum.shifted_bins == 0);
        assert!(psum.shifted_bytes == 0);
        assert!(psum.valid_bytes == 0);
        assert!(psum.frac_bad_bins == 0.0);
        assert!(psum.temporal_behavior == TemporalBehavior::Undersampled);

        let time2bin = db::TimeBin::mock_week(BIN_DURATION_SECS, 50, 55, 100.0, 50, 55, 100.0);
        let psum = PathSummary::build(&time2bin, &summarizer, &NULL_TEMPCONFIG);
        assert!(psum.time2binstats.is_empty());
        assert!(psum.day2shifts.is_empty());
        assert!(psum.day2shifts.values().fold(true, |_, e| *e == 0));
        assert!(psum.distinct_shifts == 0);
        assert!(psum.shifted_bins == 0);
        assert!(psum.shifted_bytes == 0);
        assert!(psum.valid_bytes == 0);
        assert!(psum.frac_bad_bins == 0.0);
        assert!(psum.temporal_behavior == TemporalBehavior::Undersampled);
    }

    #[test]
    fn test_path_summary_all_valid_no_shifts() {
        let time2bin = db::TimeBin::mock_week(BIN_DURATION_SECS, 50, 51, 0.001, 50, 51, 0.001);
        let nbins = time2bin.len();
        let summarizer = MinRtt50ImprovementSummarizer {
            minrtt50_diff_min_improv: 2,
            max_minrtt50_diff_ci_halfwidth: 5,
        };
        let psum = PathSummary::build(&time2bin, &summarizer, &NULL_TEMPCONFIG);
        assert!(psum.time2binstats.len() == nbins);
        assert!(psum.day2shifts.len() == 7);
        assert!(psum.day2shifts.values().fold(true, |_, e| *e == 0));
        assert!(psum.distinct_shifts == 0);
        assert!(psum.shifted_bins == 0);
        assert!(psum.shifted_bytes == 0);
        assert!(psum.valid_bytes == db::TimeBin::MOCK_TOTAL_BYTES * (time2bin.len() as u64));
        assert!(psum.frac_bad_bins == 0.0);
        assert!(psum.temporal_behavior == TemporalBehavior::Uneventful);

        let time2bin = db::TimeBin::mock_week(BIN_DURATION_SECS, 50, 100, 0.001, 50, 100, 0.001);
        let summarizer = MinRtt50ImprovementSummarizer {
            minrtt50_diff_min_improv: 51,
            max_minrtt50_diff_ci_halfwidth: 5,
        };
        let psum = PathSummary::build(&time2bin, &summarizer, &NULL_TEMPCONFIG);
        assert!(psum.time2binstats.len() == nbins);
        assert!(psum.day2shifts.len() == 7);
        assert!(psum.day2shifts.values().fold(true, |_, e| *e == 0));
        assert!(psum.distinct_shifts == 0);
        assert!(psum.shifted_bins == 0);
        assert!(psum.shifted_bytes == 0);
        assert!(psum.valid_bytes == db::TimeBin::MOCK_TOTAL_BYTES * (time2bin.len() as u64));
        assert!(psum.frac_bad_bins == 0.0);
        assert!(psum.temporal_behavior == TemporalBehavior::Uneventful);
    }

    #[test]
    fn test_path_summary_all_valid_all_shifts() {
        let time2bin = db::TimeBin::mock_week(BIN_DURATION_SECS, 50, 51, 0.001, 50, 51, 0.001);
        let nbins = time2bin.len();
        let summarizer = MinRtt50ImprovementSummarizer {
            minrtt50_diff_min_improv: 1,
            max_minrtt50_diff_ci_halfwidth: 5,
        };
        let psum = PathSummary::build(&time2bin, &summarizer, &NULL_TEMPCONFIG);
        assert!(psum.time2binstats.len() == nbins);
        assert!(psum.day2shifts.len() == 7);
        assert!(psum.day2shifts.values().fold(true, |_, e| *e == 86400 / BIN_DURATION_SECS as u32));
        assert!(psum.distinct_shifts == 1);
        assert!(psum.shifted_bins == nbins as u32);
        assert!(psum.shifted_bytes == psum.valid_bytes);
        assert!(psum.valid_bytes == db::TimeBin::MOCK_TOTAL_BYTES * (time2bin.len() as u64));
        assert!(psum.frac_bad_bins >= 0.0);
        assert!(psum.temporal_behavior == TemporalBehavior::Continuous);

        let time2bin = db::TimeBin::mock_week(BIN_DURATION_SECS, 50, 55, 0.001, 50, 55, 0.001);
        let summarizer = MinRtt50ImprovementSummarizer {
            minrtt50_diff_min_improv: 5,
            max_minrtt50_diff_ci_halfwidth: 5,
        };
        let psum = PathSummary::build(&time2bin, &summarizer, &NULL_TEMPCONFIG);
        assert!(psum.time2binstats.len() == nbins);
        assert!(psum.day2shifts.len() == 7);
        assert!(psum.day2shifts.values().fold(true, |_, e| *e == 86400 / BIN_DURATION_SECS as u32));
        assert!(psum.distinct_shifts == 1);
        assert!(psum.shifted_bins == nbins as u32);
        assert!(psum.shifted_bytes == psum.valid_bytes);
        assert!(psum.valid_bytes == db::TimeBin::MOCK_TOTAL_BYTES * (time2bin.len() as u64));
        assert!(psum.frac_bad_bins >= 0.0);
        assert!(psum.temporal_behavior == TemporalBehavior::Continuous);
    }

    #[test]
    fn test_path_summary_half_valid_no_shifts() {
        let time2bin = db::TimeBin::mock_week(BIN_DURATION_SECS, 50, 51, 0.001, 50, 51, 100.0);
        let nbins = time2bin.len();
        let summarizer = MinRtt50ImprovementSummarizer {
            minrtt50_diff_min_improv: 2,
            max_minrtt50_diff_ci_halfwidth: 5,
        };
        let psum = PathSummary::build(&time2bin, &summarizer, &NULL_TEMPCONFIG);
        assert!(psum.time2binstats.len() == nbins / 2);
        assert!(psum.day2shifts.len() == 7);
        assert!(psum.day2shifts.values().fold(true, |_, e| *e == 0));
        assert!(psum.distinct_shifts == 0);
        assert!(psum.shifted_bins == 0);
        assert!(psum.shifted_bytes == 0);
        assert!(psum.valid_bytes == db::TimeBin::MOCK_TOTAL_BYTES * (nbins as u64) / 2);
        assert!(psum.frac_bad_bins == 0.0);
        assert!(psum.temporal_behavior == TemporalBehavior::Undersampled);

        let time2bin = db::TimeBin::mock_week(BIN_DURATION_SECS, 50, 100, 0.001, 50, 100, 100.0);
        let summarizer = MinRtt50ImprovementSummarizer {
            minrtt50_diff_min_improv: 51,
            max_minrtt50_diff_ci_halfwidth: 5,
        };
        let psum = PathSummary::build(&time2bin, &summarizer, &NULL_TEMPCONFIG);
        assert!(psum.time2binstats.len() == nbins / 2);
        assert!(psum.day2shifts.len() == 7);
        assert!(psum.day2shifts.values().fold(true, |_, e| *e == 0));
        assert!(psum.distinct_shifts == 0);
        assert!(psum.shifted_bins == 0);
        assert!(psum.shifted_bytes == 0);
        assert!(psum.valid_bytes == db::TimeBin::MOCK_TOTAL_BYTES * (nbins as u64) / 2);
        assert!(psum.frac_bad_bins == 0.0);
        assert!(psum.temporal_behavior == TemporalBehavior::Undersampled);
    }

    #[test]
    fn test_path_summary_half_valid_all_shifts() {
        let time2bin = db::TimeBin::mock_week(BIN_DURATION_SECS, 50, 51, 0.001, 50, 51, 100.0);
        let nbins = time2bin.len();
        let summarizer = MinRtt50ImprovementSummarizer {
            minrtt50_diff_min_improv: 1,
            max_minrtt50_diff_ci_halfwidth: 5,
        };
        let psum = PathSummary::build(&time2bin, &summarizer, &NULL_TEMPCONFIG);
        assert!(psum.time2binstats.len() == nbins / 2);
        assert!(psum.day2shifts.len() == 7);
        assert!(psum
            .day2shifts
            .values()
            .fold(true, |_, e| *e == 86400 / 2 / BIN_DURATION_SECS as u32));
        assert!(psum.distinct_shifts == 1);
        assert!(psum.shifted_bins == (nbins / 2) as u32);
        assert!(psum.shifted_bytes == psum.valid_bytes);
        assert!(psum.valid_bytes == db::TimeBin::MOCK_TOTAL_BYTES * (nbins as u64) / 2);
        assert!(psum.frac_bad_bins >= 0.0);
        assert!(psum.temporal_behavior == TemporalBehavior::Undersampled);

        let time2bin = db::TimeBin::mock_week(BIN_DURATION_SECS, 50, 55, 0.001, 50, 55, 100.0);
        let summarizer = MinRtt50ImprovementSummarizer {
            minrtt50_diff_min_improv: 5,
            max_minrtt50_diff_ci_halfwidth: 5,
        };
        let psum = PathSummary::build(&time2bin, &summarizer, &NULL_TEMPCONFIG);
        assert!(psum.time2binstats.len() == nbins / 2);
        assert!(psum.day2shifts.len() == 7);
        assert!(psum
            .day2shifts
            .values()
            .fold(true, |_, e| *e == 86400 / 2 / BIN_DURATION_SECS as u32));
        assert!(psum.distinct_shifts == 1);
        assert!(psum.shifted_bins == (nbins / 2) as u32);
        assert!(psum.shifted_bytes == psum.valid_bytes);
        assert!(psum.valid_bytes == db::TimeBin::MOCK_TOTAL_BYTES * (nbins as u64) / 2);
        assert!(psum.frac_bad_bins >= 0.0);
        assert!(psum.temporal_behavior == TemporalBehavior::Undersampled);
    }

    #[test]
    fn test_continuous() {
        let summarizer = MinRtt50ImprovementSummarizer {
            minrtt50_diff_min_improv: 5,
            max_minrtt50_diff_ci_halfwidth: 5,
        };
        let time2bin = db::TimeBin::mock_week(BIN_DURATION_SECS, 50, 55, 0.001, 50, 55, 0.001);
        let psum = PathSummary::build(&time2bin, &summarizer, &DEFAULT_TEMPCONFIG);
        assert!(psum.temporal_behavior == TemporalBehavior::Continuous);

        let time2bin = db::TimeBin::mock_week(BIN_DURATION_SECS, 50, 55, 0.001, 50, 55, 100.0);
        let psum = PathSummary::build(&time2bin, &summarizer, &DEFAULT_TEMPCONFIG);
        assert!(psum.temporal_behavior == TemporalBehavior::Undersampled);

        let mut config = DEFAULT_TEMPCONFIG;
        config.min_frac_valid_bins = 0.4;
        let psum = PathSummary::build(&time2bin, &summarizer, &config);
        assert!(psum.temporal_behavior == TemporalBehavior::Continuous);
    }

    #[test]
    fn test_diurnal_num_bad_bins() {
        let summarizer = MinRtt50ImprovementSummarizer {
            minrtt50_diff_min_improv: 5,
            max_minrtt50_diff_ci_halfwidth: 5,
        };
        let time2bin = db::TimeBin::mock_week(BIN_DURATION_SECS, 50, 51, 0.001, 50, 55, 0.001);
        let psum = PathSummary::build(&time2bin, &summarizer, &DEFAULT_TEMPCONFIG);
        assert!(psum.temporal_behavior == TemporalBehavior::Diurnal);

        let mut config = DEFAULT_TEMPCONFIG;
        config.diurnal_min_frac_bad_bins = 0.6;
        let psum = PathSummary::build(&time2bin, &summarizer, &config);
        assert!(psum.temporal_behavior == TemporalBehavior::Episodic);

        let mut config = DEFAULT_TEMPCONFIG;
        config.continuous_min_frac_shifted_bins = 0.4;
        let psum = PathSummary::build(&time2bin, &summarizer, &config);
        assert!(psum.temporal_behavior == TemporalBehavior::Continuous);
    }

    #[test]
    fn test_diurnal_min_prob_shift() {
        let summarizer = MinRtt50ImprovementSummarizer {
            minrtt50_diff_min_improv: 5,
            max_minrtt50_diff_ci_halfwidth: 5,
        };
        let mut time2bin = db::TimeBin::mock_week(BIN_DURATION_SECS, 50, 51, 0.001, 50, 51, 0.001);
        let time2bin2 = db::TimeBin::mock_week(BIN_DURATION_SECS, 50, 51, 0.001, 50, 55, 0.001);
        time2bin.extend(time2bin2.into_iter().map(|(t, bin)| (t + 7 * 86400, bin)));
        let bins = time2bin.len();
        assert!(bins == 2 * 7 * 86400 / BIN_DURATION_SECS as usize);

        let psum = PathSummary::build(&time2bin, &summarizer, &DEFAULT_TEMPCONFIG);
        assert!(psum.temporal_behavior == TemporalBehavior::Episodic);

        let mut config = DEFAULT_TEMPCONFIG;
        config.diurnal_bad_bin_min_prob_shift = 0.5;
        let psum = PathSummary::build(&time2bin, &summarizer, &config);
        assert!(psum.temporal_behavior == TemporalBehavior::Diurnal);
    }

    #[test]
    fn test_undersampled() {
        let summarizer = MinRtt50ImprovementSummarizer {
            minrtt50_diff_min_improv: 5,
            max_minrtt50_diff_ci_halfwidth: 5,
        };

        let time2bin = db::TimeBin::mock_week(BIN_DURATION_SECS, 50, 55, 100.0, 50, 55, 0.001);
        let psum = PathSummary::build(&time2bin, &summarizer, &DEFAULT_TEMPCONFIG);
        assert!(psum.temporal_behavior == TemporalBehavior::Undersampled);

        let mut config = DEFAULT_TEMPCONFIG;
        config.min_frac_valid_bins = 0.5;
        let psum = PathSummary::build(&time2bin, &summarizer, &config);
        assert!(psum.temporal_behavior == TemporalBehavior::Continuous);

        let time2bin = db::TimeBin::mock_week(BIN_DURATION_SECS, 50, 51, 100.0, 50, 51, 0.001);
        let psum = PathSummary::build(&time2bin, &summarizer, &DEFAULT_TEMPCONFIG);
        assert!(psum.temporal_behavior == TemporalBehavior::Undersampled);
        let mut config = DEFAULT_TEMPCONFIG;
        config.min_frac_valid_bins = 0.5;
        let psum = PathSummary::build(&time2bin, &summarizer, &config);
        assert!(psum.temporal_behavior == TemporalBehavior::Uneventful);
    }

    #[test]
    fn test_compute_offset() {
        for bin_duration_secs in (300..=1200).step_by(300) {
            let bins_per_day = 86400 / bin_duration_secs;
            for day in 0..10 {
                for bin in 0..bins_per_day {
                    let time: u64 = (day * 86400 + bin * bin_duration_secs) as u64;
                    let offset: usize = compute_offset(time, bin_duration_secs as u32);
                    assert!(offset == bin as usize);
                }
            }
        }
    }

    #[test]
    fn test_compute_num_bad_bins() {
        let min_shifts = 10;
        for bin_duration_secs in (300..=1200).step_by(300) {
            let bins_per_day = 86400 / bin_duration_secs;
            for num_bad_bins in 0..bins_per_day {
                let mut offset_shift_counts = vec![0u32; bins_per_day];
                for i in 0..num_bad_bins {
                    offset_shift_counts[i] = min_shifts;
                }
                for i in num_bad_bins..bins_per_day {
                    offset_shift_counts[i] = 0;
                }
                let computed_num_bad_bins = compute_num_bad_bins(&offset_shift_counts, min_shifts);
                assert!(computed_num_bad_bins as usize == num_bad_bins);
            }
        }
    }
}
