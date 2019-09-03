use std::collections::{BTreeMap, HashMap};
use std::convert::TryFrom;
use std::fs;
use std::io;
use std::io::Write;
use std::path::PathBuf;
use std::rc::Rc;

use num_enum::TryFromPrimitive;

use crate::cdf;
use crate::performance::db;

pub trait TimeBinSummarizer {
    fn summarize(&self, pathid: &db::PathId, bin: &db::TimeBin) -> Option<TimeBinStats>;
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
    pub diurnal_min_bad_bins: u32,
    pub diurnal_bad_bin_min_prob_shift: f32,
    pub uneventful_max_frac_shifted_bins: f32,
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
    // valid_bins = time2binstats.len()
    // total_bins needs to come from db::DB
    shifted_bytes: u64,
    valid_bytes: u64,
    num_bad_bins: u32,
    temporal_behavior: TemporalBehavior,
}

#[derive(Debug, Default)]
pub struct TimeBinStats {
    pub bytes: u64,
    pub diff_ci: f32,
    pub diff_ci_halfwidth: f32,
    pub is_shifted: bool,
}

impl TemporalConfig {
    pub fn dump(&self, dir: &PathBuf) -> Result<(), io::Error> {
        let mut filename = dir.clone();
        filename.push("temporal-config.txt");
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(true)
            .create(true)
            .open(filename)?;
        let mut bw = io::BufWriter::new(file);
        writeln!(bw, "{:?}", self)
    }
    pub fn prefix(&self) -> String {
        format!(
            "tempconfig--bin-{}--days-{}--fracValid-{:0.2}--cont-{:0.2}--minBadBins-{}--badBinPrev-{:0.2}--uneventful-{:0.2}",
            self.bin_duration_secs,
            self.min_days,
            self.min_frac_valid_bins,
            self.continuous_min_frac_shifted_bins,
            self.diurnal_min_bad_bins,
            self.diurnal_bad_bin_min_prob_shift,
            self.uneventful_max_frac_shifted_bins
        )
    }
}

impl DBSummary {
    pub fn build(
        db: &db::DB,
        summarizer: &dyn TimeBinSummarizer,
        tempconfig: &TemporalConfig,
    ) -> DBSummary {
        let mut dbsum = DBSummary::default();
        for (pid, time2bin) in &db.pathid2time2bin {
            let psum = PathSummary::build(&pid, time2bin, summarizer, tempconfig);
            if psum.valid_bytes == 0 {
                continue;
            }
            dbsum.valid_bytes += psum.valid_bytes;
            dbsum.behavior_valid_bytes[psum.temporal_behavior as usize] += psum.valid_bytes;
            dbsum.behavior_total_bytes[psum.temporal_behavior as usize] += db.pathid2traffic[pid];
            dbsum.pathid2summary.insert(Rc::clone(&pid), psum);
        }
        dbsum
    }

    pub fn reclassify(&mut self, db: &db::DB, tempconfig: &TemporalConfig) {
        self.behavior_valid_bytes = [0u64; TemporalBehavior::SIZE as usize];
        self.behavior_total_bytes = [0u64; TemporalBehavior::SIZE as usize];
        for (pid, psum) in self.pathid2summary.iter_mut() {
            psum.classify(db.pathid2time2bin[pid].len() as u32, tempconfig);
            self.behavior_valid_bytes[psum.temporal_behavior as usize] += psum.valid_bytes;
            self.behavior_total_bytes[psum.temporal_behavior as usize] += db.pathid2traffic[pid];
        }
    }

    pub fn dump(&self, path: &PathBuf) -> Result<(), io::Error> {
        self.dump_cdfs(path)?;
        self.dump_temporal_tables(path)?;
        Ok(())
    }

    fn dump_cdfs(&self, path: &PathBuf) -> Result<(), io::Error> {
        std::fs::create_dir_all(path)?;

        let mut fpath = path.clone();
        fpath.push("diff_ci_bins.cdf");
        self.dump_bin_cdf(&fpath, |bs: &TimeBinStats| (bs.diff_ci, 1.0))?;
        let mut fpath = path.clone();
        fpath.push("diff_ci_bins_weighted.cdf");
        self.dump_bin_cdf(&fpath, |bs: &TimeBinStats| (bs.diff_ci, bs.bytes as f32))?;

        let mut fpath = path.clone();
        fpath.push("diff_ci_lb_bins.cdf");
        self.dump_bin_cdf(&fpath, |bs: &TimeBinStats| (bs.diff_ci - bs.diff_ci_halfwidth, 1.0))?;
        let mut fpath = path.clone();
        fpath.push("diff_ci_lb_bins_weighted.cdf");
        self.dump_bin_cdf(&fpath, |bs: &TimeBinStats| {
            (bs.diff_ci - bs.diff_ci_halfwidth, bs.bytes as f32)
        })?;

        let mut fpath = path.clone();
        fpath.push("diff_ci_ub_bins.cdf");
        self.dump_bin_cdf(&fpath, |bs: &TimeBinStats| (bs.diff_ci + bs.diff_ci_halfwidth, 1.0))?;
        let mut fpath = path.clone();
        fpath.push("diff_ci_ub_bins_weighted.cdf");
        self.dump_bin_cdf(&fpath, |bs: &TimeBinStats| {
            (bs.diff_ci + bs.diff_ci_halfwidth, bs.bytes as f32)
        })?;

        let mut fpath = path.clone();
        fpath.push("frac_shifted_bins_paths.cdf");
        self.dump_path_cdf(&fpath, |ps: &PathSummary| {
            ((ps.shifted_bins as f32) / (ps.time2binstats.len() as f32), 1.0)
        })?;
        let mut fpath = path.clone();
        fpath.push("frac_shifted_bins_paths_weighted.cdf");
        self.dump_path_cdf(&fpath, |ps: &PathSummary| {
            ((ps.shifted_bins as f32) / (ps.time2binstats.len() as f32), ps.valid_bytes as f32)
        })?;

        // This code assumes that the dataset has "full days" (otherwise
        // the number of shifts per day will be underestimated).
        let mut fpath = path.clone();
        fpath.push("average_shifts_per_day_paths.cdf");
        self.dump_path_cdf(&fpath, |ps: &PathSummary| {
            ((ps.distinct_shifts as f32) / (ps.day2shifts.len() as f32), 1.0)
        })?;
        let mut fpath = path.clone();
        fpath.push("average_shifts_per_day_paths_weighted.cdf");
        self.dump_path_cdf(&fpath, |ps: &PathSummary| {
            ((ps.distinct_shifts as f32) / (ps.day2shifts.len() as f32), ps.valid_bytes as f32)
        })?;

        Ok(())
    }

    fn dump_bin_cdf<F>(&self, file: &PathBuf, getdata: F) -> Result<(), io::Error>
    where
        F: Fn(&TimeBinStats) -> (f32, f32),
    {
        let mut data: Vec<(f32, f32)> = Vec::new();
        for psum in self.pathid2summary.values() {
            for binstats in psum.time2binstats.values() {
                data.push(getdata(binstats));
            }
        }
        cdf::dump(&cdf::build(&mut data, 0.001), file)?;
        Ok(())
    }

    fn dump_path_cdf<F>(&self, file: &PathBuf, getdata: F) -> Result<(), io::Error>
    where
        F: Fn(&PathSummary) -> (f32, f32),
    {
        let mut data: Vec<(f32, f32)> = Vec::new();
        for psum in self.pathid2summary.values() {
            data.push(getdata(psum));
        }
        cdf::dump(&cdf::build(&mut data, 0.001), file)?;
        Ok(())
    }

    fn dump_temporal_tables(&self, path: &PathBuf) -> Result<(), io::Error> {
        let mut filepath = path.clone();
        filepath.push("temporal-behavior-table.txt");
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(true)
            .create(true)
            .open(filepath)?;
        let mut bw = io::BufWriter::new(file);

        let normalized_behaviors: [u64; TemporalBehavior::SIZE as usize] = [0, 1, 1, 1, 0, 0];
        let global: u64 = self.behavior_total_bytes.iter().sum::<u64>();
        let norm_total: u64 = self
            .behavior_total_bytes
            .iter()
            .enumerate()
            .fold(0u64, |acc, (i, v)| acc + normalized_behaviors[i] * (*v));

        writeln!(bw, "behavior valid total valid/total valid/global total/global norm")?;
        for (i, valid) in self.behavior_valid_bytes.iter().enumerate() {
            let behavior: TemporalBehavior = TemporalBehavior::try_from(i as u8).unwrap();
            let total: u64 = self.behavior_total_bytes[i];
            write!(bw, "{:?} {} {}", behavior, valid, total)?;
            let frac_valid_total: f32 = 100.0 * (*valid as f32) / (total as f32);
            let frac_valid: f32 = 100.0 * (*valid as f32) / (global as f32);
            let frac_total: f32 = 100.0 * (total as f32) / (global as f32);
            write!(bw, " {:0.2} {:0.2} {:0.2}", frac_valid_total, frac_valid, frac_total)?;
            if normalized_behaviors[i] == 1 {
                let norm_valid: f32 = 100.0 * (*valid as f32) / (norm_total as f32);
                writeln!(bw, " {:0.2}%", norm_valid)?;
            } else {
                writeln!(bw, " ---")?;
            }
        }
        Ok(())
    }
}

impl PathSummary {
    fn build(
        pathid: &db::PathId,
        time2bin: &BTreeMap<u64, db::TimeBin>,
        summarizer: &dyn TimeBinSummarizer,
        tempconfig: &TemporalConfig,
    ) -> PathSummary {
        let mut psum = PathSummary::default();
        let mut is_shifted = false;
        for (time, timebin) in time2bin {
            // This requires that time2bin is a BTreeMap as
            // computing the number of distinct shift events
            // requires processing TimeBins in time order.
            if let Some(binstats) = summarizer.summarize(pathid, timebin) {
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
        psum.classify(time2bin.len() as u32, tempconfig);
        psum
    }

    fn classify(&mut self, total_bins: u32, config: &TemporalConfig) {
        self.compute_num_bad_bins(config);
        let valid_bins: f32 = self.time2binstats.len() as f32;
        let frac_valid: f32 = valid_bins / total_bins as f32;
        if frac_valid < config.min_frac_valid_bins {
            self.temporal_behavior = TemporalBehavior::Undersampled;
        } else {
            let frac_shift: f32 = self.shifted_bins as f32 / valid_bins;
            if frac_shift < config.uneventful_max_frac_shifted_bins {
                self.temporal_behavior = TemporalBehavior::Uneventful;
            } else if frac_shift >= config.continuous_min_frac_shifted_bins {
                self.temporal_behavior = TemporalBehavior::Continuous;
            } else if self.num_bad_bins >= config.diurnal_min_bad_bins {
                self.temporal_behavior = TemporalBehavior::Diurnal;
            } else {
                self.temporal_behavior = TemporalBehavior::Episodic;
            }
        }
    }

    fn compute_num_bad_bins(&mut self, config: &TemporalConfig) {
        let num_days: u32 = self.day2shifts.len() as u32;
        if num_days < config.min_days {
            self.num_bad_bins = 0;
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
        self.num_bad_bins = compute_num_bad_bins(&offset_shift_counts, min_shifts);
    }
}

impl Default for TemporalBehavior {
    fn default() -> TemporalBehavior {
        TemporalBehavior::Uninitialized
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
    use crate::performance::summarizers::opportunity::MinRtt50ImprovementSummarizer;

    const BIN_DURATION_SECS: u64 = 900;
    const NULL_TEMPCONFIG: TemporalConfig = TemporalConfig {
        bin_duration_secs: 900,
        min_days: 7,
        min_frac_valid_bins: 1.0,
        continuous_min_frac_shifted_bins: 1.0,
        diurnal_min_bad_bins: 96,
        diurnal_bad_bin_min_prob_shift: 1.0,
        uneventful_max_frac_shifted_bins: 1.0,
    };
    const DEFAULT_TEMPCONFIG: TemporalConfig = TemporalConfig {
        bin_duration_secs: 900,
        min_days: 7,
        min_frac_valid_bins: 0.8,
        continuous_min_frac_shifted_bins: 0.8,
        diurnal_min_bad_bins: 24,
        diurnal_bad_bin_min_prob_shift: 0.8,
        uneventful_max_frac_shifted_bins: 0.25,
    };

    #[test]
    fn test_path_summary_no_valid() {
        let _pathid: db::PathId = db::PathId {
            vip_metro: String::from("gru"),
            bgp_ip_prefix: "1.0.0.0/24".parse().unwrap(),
        };

        let summarizer = MinRtt50ImprovementSummarizer {
            minrtt50_min_improv: 2,
            max_minrtt50_diff_ci_halfwidth: 15.0,
            no_alternate_is_valid: true,
            compare_lower_bound: false,
        };

        let time2bin =
            db::TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 51, 50, 100.0, 51, 50, 100.0);
        let psum = PathSummary::build(&_pathid, &time2bin, &summarizer, &NULL_TEMPCONFIG);
        assert!(psum.time2binstats.is_empty());
        assert!(psum.day2shifts.is_empty());
        assert!(psum.day2shifts.values().fold(true, |_, e| *e == 0));
        assert!(psum.distinct_shifts == 0);
        assert!(psum.shifted_bins == 0);
        assert!(psum.shifted_bytes == 0);
        assert!(psum.valid_bytes == 0);
        assert!(psum.num_bad_bins == 0);
        assert!(psum.temporal_behavior == TemporalBehavior::Undersampled);

        let time2bin =
            db::TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 55, 50, 100.0, 55, 50, 100.0);
        let psum = PathSummary::build(&_pathid, &time2bin, &summarizer, &NULL_TEMPCONFIG);
        assert!(psum.time2binstats.is_empty());
        assert!(psum.day2shifts.is_empty());
        assert!(psum.day2shifts.values().fold(true, |_, e| *e == 0));
        assert!(psum.distinct_shifts == 0);
        assert!(psum.shifted_bins == 0);
        assert!(psum.shifted_bytes == 0);
        assert!(psum.valid_bytes == 0);
        assert!(psum.num_bad_bins == 0);
        assert!(psum.temporal_behavior == TemporalBehavior::Undersampled);
    }

    #[test]
    fn test_path_summary_all_valid_no_shifts() {
        let _pathid: db::PathId = db::PathId {
            vip_metro: String::from("gru"),
            bgp_ip_prefix: "1.0.0.0/24".parse().unwrap(),
        };

        let time2bin =
            db::TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 51, 50, 0.001, 51, 50, 0.001);
        let nbins = time2bin.len();
        let summarizer = MinRtt50ImprovementSummarizer {
            minrtt50_min_improv: 2,
            max_minrtt50_diff_ci_halfwidth: 5.0,
            no_alternate_is_valid: true,
            compare_lower_bound: false,
        };
        let psum = PathSummary::build(&_pathid, &time2bin, &summarizer, &NULL_TEMPCONFIG);
        assert!(psum.time2binstats.len() == nbins);
        assert!(psum.day2shifts.len() == 7);
        assert!(psum.day2shifts.values().fold(true, |_, e| *e == 0));
        assert!(psum.distinct_shifts == 0);
        assert!(psum.shifted_bins == 0);
        assert!(psum.shifted_bytes == 0);
        assert!(psum.valid_bytes == db::TimeBin::MOCK_TOTAL_BYTES * (time2bin.len() as u64));
        assert!(psum.num_bad_bins == 0);
        assert!(psum.temporal_behavior == TemporalBehavior::Uneventful);

        let time2bin =
            db::TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 100, 50, 0.001, 100, 50, 0.001);
        let summarizer = MinRtt50ImprovementSummarizer {
            minrtt50_min_improv: 51,
            max_minrtt50_diff_ci_halfwidth: 5.0,
            no_alternate_is_valid: true,
            compare_lower_bound: false,
        };
        let psum = PathSummary::build(&_pathid, &time2bin, &summarizer, &NULL_TEMPCONFIG);
        assert!(psum.time2binstats.len() == nbins);
        assert!(psum.day2shifts.len() == 7);
        assert!(psum.day2shifts.values().fold(true, |_, e| *e == 0));
        assert!(psum.distinct_shifts == 0);
        assert!(psum.shifted_bins == 0);
        assert!(psum.shifted_bytes == 0);
        assert!(psum.valid_bytes == db::TimeBin::MOCK_TOTAL_BYTES * (time2bin.len() as u64));
        assert!(psum.num_bad_bins == 0);
        assert!(psum.temporal_behavior == TemporalBehavior::Uneventful);
    }

    #[test]
    fn test_path_summary_all_valid_all_shifts() {
        let _pathid: db::PathId = db::PathId {
            vip_metro: String::from("gru"),
            bgp_ip_prefix: "1.0.0.0/24".parse().unwrap(),
        };

        let time2bin =
            db::TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 51, 50, 0.001, 51, 50, 0.001);
        let nbins = time2bin.len();
        let summarizer = MinRtt50ImprovementSummarizer {
            minrtt50_min_improv: 1,
            max_minrtt50_diff_ci_halfwidth: 5.0,
            no_alternate_is_valid: true,
            compare_lower_bound: false,
        };
        let psum = PathSummary::build(&_pathid, &time2bin, &summarizer, &NULL_TEMPCONFIG);
        assert!(psum.time2binstats.len() == nbins);
        assert!(psum.day2shifts.len() == 7);
        assert!(psum.day2shifts.values().fold(true, |_, e| *e == 86400 / BIN_DURATION_SECS as u32));
        assert!(psum.distinct_shifts == 1);
        assert!(psum.shifted_bins == nbins as u32);
        assert!(psum.shifted_bytes == psum.valid_bytes);
        assert!(psum.valid_bytes == db::TimeBin::MOCK_TOTAL_BYTES * (time2bin.len() as u64));
        assert!(psum.num_bad_bins == 86400 / (BIN_DURATION_SECS as u32));
        assert!(psum.temporal_behavior == TemporalBehavior::Continuous);

        let time2bin =
            db::TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 55, 50, 0.001, 55, 50, 0.001);
        let summarizer = MinRtt50ImprovementSummarizer {
            minrtt50_min_improv: 5,
            max_minrtt50_diff_ci_halfwidth: 5.0,
            no_alternate_is_valid: true,
            compare_lower_bound: false,
        };
        let psum = PathSummary::build(&_pathid, &time2bin, &summarizer, &NULL_TEMPCONFIG);
        assert!(psum.time2binstats.len() == nbins);
        assert!(psum.day2shifts.len() == 7);
        assert!(psum.day2shifts.values().fold(true, |_, e| *e == 86400 / BIN_DURATION_SECS as u32));
        assert!(psum.distinct_shifts == 1);
        assert!(psum.shifted_bins == nbins as u32);
        assert!(psum.shifted_bytes == psum.valid_bytes);
        assert!(psum.valid_bytes == db::TimeBin::MOCK_TOTAL_BYTES * (time2bin.len() as u64));
        assert!(psum.num_bad_bins == 86400 / (BIN_DURATION_SECS as u32));
        assert!(psum.temporal_behavior == TemporalBehavior::Continuous);
    }

    #[test]
    fn test_path_summary_half_valid_no_shifts() {
        let _pathid: db::PathId = db::PathId {
            vip_metro: String::from("gru"),
            bgp_ip_prefix: "1.0.0.0/24".parse().unwrap(),
        };

        let time2bin =
            db::TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 51, 50, 0.001, 51, 50, 100.0);
        let nbins = time2bin.len();
        let summarizer = MinRtt50ImprovementSummarizer {
            minrtt50_min_improv: 2,
            max_minrtt50_diff_ci_halfwidth: 5.0,
            no_alternate_is_valid: true,
            compare_lower_bound: false,
        };
        let psum = PathSummary::build(&_pathid, &time2bin, &summarizer, &NULL_TEMPCONFIG);
        assert!(psum.time2binstats.len() == nbins / 2);
        assert!(psum.day2shifts.len() == 7);
        assert!(psum.day2shifts.values().fold(true, |_, e| *e == 0));
        assert!(psum.distinct_shifts == 0);
        assert!(psum.shifted_bins == 0);
        assert!(psum.shifted_bytes == 0);
        assert!(psum.valid_bytes == db::TimeBin::MOCK_TOTAL_BYTES * (nbins as u64) / 2);
        assert!(psum.num_bad_bins == 0);
        assert!(psum.temporal_behavior == TemporalBehavior::Undersampled);

        let time2bin =
            db::TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 100, 50, 0.001, 100, 50, 100.0);
        let summarizer = MinRtt50ImprovementSummarizer {
            minrtt50_min_improv: 51,
            max_minrtt50_diff_ci_halfwidth: 5.0,
            no_alternate_is_valid: true,
            compare_lower_bound: false,
        };
        let psum = PathSummary::build(&_pathid, &time2bin, &summarizer, &NULL_TEMPCONFIG);
        assert!(psum.time2binstats.len() == nbins / 2);
        assert!(psum.day2shifts.len() == 7);
        assert!(psum.day2shifts.values().fold(true, |_, e| *e == 0));
        assert!(psum.distinct_shifts == 0);
        assert!(psum.shifted_bins == 0);
        assert!(psum.shifted_bytes == 0);
        assert!(psum.valid_bytes == db::TimeBin::MOCK_TOTAL_BYTES * (nbins as u64) / 2);
        assert!(psum.num_bad_bins == 0);
        assert!(psum.temporal_behavior == TemporalBehavior::Undersampled);
    }

    #[test]
    fn test_path_summary_half_valid_all_shifts() {
        let _pathid: db::PathId = db::PathId {
            vip_metro: String::from("gru"),
            bgp_ip_prefix: "1.0.0.0/24".parse().unwrap(),
        };

        let time2bin =
            db::TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 51, 50, 0.001, 51, 50, 100.0);
        let nbins = time2bin.len();
        let summarizer = MinRtt50ImprovementSummarizer {
            minrtt50_min_improv: 1,
            max_minrtt50_diff_ci_halfwidth: 5.0,
            no_alternate_is_valid: true,
            compare_lower_bound: false,
        };
        let psum = PathSummary::build(&_pathid, &time2bin, &summarizer, &NULL_TEMPCONFIG);
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
        assert!(psum.num_bad_bins == 48);
        assert!(psum.temporal_behavior == TemporalBehavior::Undersampled);

        let time2bin =
            db::TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 55, 50, 0.001, 55, 50, 100.0);
        let summarizer = MinRtt50ImprovementSummarizer {
            minrtt50_min_improv: 5,
            max_minrtt50_diff_ci_halfwidth: 5.0,
            no_alternate_is_valid: true,
            compare_lower_bound: false,
        };
        let psum = PathSummary::build(&_pathid, &time2bin, &summarizer, &NULL_TEMPCONFIG);
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
        assert!(psum.num_bad_bins == 48);
        assert!(psum.temporal_behavior == TemporalBehavior::Undersampled);
    }

    #[test]
    fn test_continuous() {
        let _pathid: db::PathId = db::PathId {
            vip_metro: String::from("gru"),
            bgp_ip_prefix: "1.0.0.0/24".parse().unwrap(),
        };

        let summarizer = MinRtt50ImprovementSummarizer {
            minrtt50_min_improv: 5,
            max_minrtt50_diff_ci_halfwidth: 5.0,
            no_alternate_is_valid: true,
            compare_lower_bound: false,
        };
        let time2bin =
            db::TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 55, 50, 0.001, 55, 50, 0.001);
        let psum = PathSummary::build(&_pathid, &time2bin, &summarizer, &DEFAULT_TEMPCONFIG);
        assert!(psum.temporal_behavior == TemporalBehavior::Continuous);

        let time2bin =
            db::TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 55, 50, 0.001, 55, 50, 100.0);
        let mut psum = PathSummary::build(&_pathid, &time2bin, &summarizer, &DEFAULT_TEMPCONFIG);
        assert!(psum.temporal_behavior == TemporalBehavior::Undersampled);

        let mut config = DEFAULT_TEMPCONFIG;
        config.min_frac_valid_bins = 0.4;
        psum.classify(time2bin.len() as u32, &config);
        assert!(psum.temporal_behavior == TemporalBehavior::Continuous);
    }

    #[test]
    fn test_diurnal_num_bad_bins() {
        let _pathid: db::PathId = db::PathId {
            vip_metro: String::from("gru"),
            bgp_ip_prefix: "1.0.0.0/24".parse().unwrap(),
        };

        let summarizer = MinRtt50ImprovementSummarizer {
            minrtt50_min_improv: 5,
            max_minrtt50_diff_ci_halfwidth: 5.0,
            no_alternate_is_valid: true,
            compare_lower_bound: false,
        };
        let time2bin =
            db::TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 51, 50, 0.001, 55, 50, 0.001);
        let mut psum = PathSummary::build(&_pathid, &time2bin, &summarizer, &DEFAULT_TEMPCONFIG);
        assert!(psum.temporal_behavior == TemporalBehavior::Diurnal);

        let mut config = DEFAULT_TEMPCONFIG;
        config.diurnal_min_bad_bins = 56; // 0.6 * 96
        psum.classify(time2bin.len() as u32, &config);
        assert!(psum.temporal_behavior == TemporalBehavior::Episodic);

        let mut config = DEFAULT_TEMPCONFIG;
        config.continuous_min_frac_shifted_bins = 0.4;
        psum.classify(time2bin.len() as u32, &config);
        assert!(psum.temporal_behavior == TemporalBehavior::Continuous);
    }

    #[test]
    fn test_diurnal_min_prob_shift() {
        let _pathid: db::PathId = db::PathId {
            vip_metro: String::from("gru"),
            bgp_ip_prefix: "1.0.0.0/24".parse().unwrap(),
        };

        let summarizer = MinRtt50ImprovementSummarizer {
            minrtt50_min_improv: 5,
            max_minrtt50_diff_ci_halfwidth: 5.0,
            no_alternate_is_valid: true,
            compare_lower_bound: false,
        };
        let mut time2bin =
            db::TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 51, 50, 0.001, 51, 50, 0.001);
        let time2bin2 =
            db::TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 51, 50, 0.001, 55, 50, 0.001);
        time2bin.extend(time2bin2.into_iter().map(|(t, bin)| (t + 7 * 86400, bin)));
        let bins = time2bin.len();
        assert!(bins == 2 * 7 * 86400 / BIN_DURATION_SECS as usize);

        let mut psum = PathSummary::build(&_pathid, &time2bin, &summarizer, &DEFAULT_TEMPCONFIG);
        assert!(psum.temporal_behavior == TemporalBehavior::Episodic);

        let mut config = DEFAULT_TEMPCONFIG;
        config.diurnal_bad_bin_min_prob_shift = 0.5;
        psum.classify(time2bin.len() as u32, &config);
        assert!(psum.temporal_behavior == TemporalBehavior::Diurnal);
    }

    #[test]
    fn test_undersampled() {
        let _pathid: db::PathId = db::PathId {
            vip_metro: String::from("gru"),
            bgp_ip_prefix: "1.0.0.0/24".parse().unwrap(),
        };

        let summarizer = MinRtt50ImprovementSummarizer {
            minrtt50_min_improv: 5,
            max_minrtt50_diff_ci_halfwidth: 5.0,
            no_alternate_is_valid: true,
            compare_lower_bound: false,
        };

        let time2bin =
            db::TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 55, 50, 100.0, 55, 50, 0.001);
        let mut psum = PathSummary::build(&_pathid, &time2bin, &summarizer, &DEFAULT_TEMPCONFIG);
        assert!(psum.temporal_behavior == TemporalBehavior::Undersampled);

        let mut config = DEFAULT_TEMPCONFIG;
        config.min_frac_valid_bins = 0.5;
        psum.classify(time2bin.len() as u32, &config);
        assert!(psum.temporal_behavior == TemporalBehavior::Continuous);

        let time2bin =
            db::TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 51, 50, 100.0, 51, 50, 0.001);
        let mut psum = PathSummary::build(&_pathid, &time2bin, &summarizer, &DEFAULT_TEMPCONFIG);
        assert!(psum.temporal_behavior == TemporalBehavior::Undersampled);

        let mut config = DEFAULT_TEMPCONFIG;
        config.min_frac_valid_bins = 0.5;
        psum.classify(time2bin.len() as u32, &config);
        assert!(psum.temporal_behavior == TemporalBehavior::Uneventful);
    }

    #[test]
    fn test_db_reclassify() {
        let summarizer = MinRtt50ImprovementSummarizer {
            minrtt50_min_improv: 5,
            max_minrtt50_diff_ci_halfwidth: 5.0,
            no_alternate_is_valid: true,
            compare_lower_bound: false,
        };

        let mut database: db::DB = db::DB::default();
        let time2bin1 =
            db::TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 51, 50, 0.001, 55, 50, 0.001);
        let nbins: u64 = time2bin1.len() as u64;
        let pid1 = db::PathId {
            vip_metro: "gru".to_string(),
            bgp_ip_prefix: "1.0.0.0/24".parse().unwrap(),
        };
        let time2bin2 =
            db::TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 51, 50, 100.0, 55, 50, 0.001);
        let pid2 = db::PathId {
            vip_metro: "gru".to_string(),
            bgp_ip_prefix: "2.0.0.0/24".parse().unwrap(),
        };
        assert!(database.insert(pid1.clone(), time2bin1).is_none());
        assert!(database.insert(pid2.clone(), time2bin2).is_none());

        let mut dbsum: DBSummary = DBSummary::build(&database, &summarizer, &DEFAULT_TEMPCONFIG);
        assert!(dbsum.pathid2summary.len() == 2);
        assert!(dbsum.valid_bytes == nbins * db::TimeBin::MOCK_TOTAL_BYTES * 3 / 2);
        assert!(dbsum.pathid2summary[&pid1].temporal_behavior == TemporalBehavior::Diurnal);
        assert!(dbsum.pathid2summary[&pid2].temporal_behavior == TemporalBehavior::Undersampled);
        assert!(
            dbsum.behavior_valid_bytes[TemporalBehavior::Diurnal as usize]
                == nbins * db::TimeBin::MOCK_TOTAL_BYTES
        );
        assert!(
            dbsum.behavior_valid_bytes[TemporalBehavior::Undersampled as usize]
                == nbins * db::TimeBin::MOCK_TOTAL_BYTES / 2
        );
        assert!(
            dbsum.behavior_total_bytes[TemporalBehavior::Undersampled as usize]
                == nbins * db::TimeBin::MOCK_TOTAL_BYTES
        );

        let mut config = DEFAULT_TEMPCONFIG;
        config.continuous_min_frac_shifted_bins = 0.5;
        config.min_frac_valid_bins = 0.4;
        dbsum.reclassify(&database, &config);
        assert!(dbsum.pathid2summary.len() == 2);
        assert!(dbsum.valid_bytes == nbins * db::TimeBin::MOCK_TOTAL_BYTES * 3 / 2);
        assert!(dbsum.pathid2summary[&pid1].temporal_behavior == TemporalBehavior::Continuous);
        assert!(dbsum.pathid2summary[&pid2].temporal_behavior == TemporalBehavior::Continuous);
        assert!(
            dbsum.behavior_valid_bytes[TemporalBehavior::Continuous as usize]
                == nbins * db::TimeBin::MOCK_TOTAL_BYTES * 3 / 2
        );
        assert!(dbsum.behavior_valid_bytes[TemporalBehavior::Diurnal as usize] == 0);
        assert!(dbsum.behavior_valid_bytes[TemporalBehavior::Undersampled as usize] == 0);
        assert!(
            dbsum.behavior_total_bytes[TemporalBehavior::Continuous as usize]
                == nbins * db::TimeBin::MOCK_TOTAL_BYTES * 2
        );
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
