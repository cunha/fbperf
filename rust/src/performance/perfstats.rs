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
    fn summarize(&self, pathid: &db::PathId, bin: &db::TimeBin) -> TimeBinSummary;
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
    NoRoute = 5,
    MissingBins = 6,
    Uninitialized = 7,
    SIZE = 8,
}

#[derive(Clone, Copy, Debug)]
pub struct TemporalConfig {
    pub bin_duration_secs: u32,
    pub min_days: u32,                         // rando class
    pub min_frac_existing_bins: f32,           // rando class
    pub min_frac_bins_with_alternate: f32,     // no alternate class
    pub min_frac_valid_bins: f32,              // undersampled class
    pub continuous_min_frac_shifted_bins: f32, // persistent class
    pub diurnal_min_bad_bins: u32,             // diurnal class
    pub diurnal_bad_bin_min_prob_shift: f32,   // diurnal class
    pub uneventful_max_frac_shifted_bins: f32, // uneventful class
}

#[derive(Default)]
pub struct DBSummary {
    pub pathid2summary: HashMap<Rc<db::PathId>, PathSummary>,
    pub total_shifted_bytes: u64,
    pub total_valid_bytes: u64,
    shifted_bytes: [[u64; db::ClientContinent::SIZE as usize]; TemporalBehavior::SIZE as usize],
    valid_bytes: [[u64; db::ClientContinent::SIZE as usize]; TemporalBehavior::SIZE as usize],
    total_bytes: [[u64; db::ClientContinent::SIZE as usize]; TemporalBehavior::SIZE as usize],
}

#[derive(Debug, Default)]
pub struct PathSummary {
    time2binstats: BTreeMap<u64, TimeBinStats>,
    day2shifts: HashMap<u32, u32>,
    distinct_shifts: u32,
    bad_bins: u32,
    // existing_bins needs to come from db::DB's pathid2time2bin[pathid].len()
    noroute_bins: u32,
    shifted_bins: u32,
    // valid_bins = time2binstats.len()
    wideci_bins: u32,
    // existing_bytes needs to come from db::DB's pathid2traffic[pathid]
    noroute_bytes: u64,
    shifted_bytes: u64,
    valid_bytes: u64,
    wideci_bytes: u64,
    temporal_behavior: TemporalBehavior,
}

#[derive(Debug, Default, PartialEq)]
pub struct TimeBinStats {
    pub bytes: u64,
    pub diff_ci: f32,
    pub diff_ci_halfwidth: f32,
    pub is_shifted: bool,
}

#[derive(Debug, PartialEq)]
pub enum TimeBinSummary {
    NoRoute,
    WideConfidenceInterval,
    Valid(TimeBinStats),
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
            "tempconfig--bin-{}--days-{}--fracExisting-{:0.2}--fracWithAlternate-{:0.2}--fracValid-{:0.2}--cont-{:0.2}--minBadBins-{}--badBinPrev-{:0.2}--uneventful-{:0.2}",
            self.bin_duration_secs,
            self.min_days,
            self.min_frac_existing_bins,
            self.min_frac_bins_with_alternate,
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
            let psum = PathSummary::build(&pid, time2bin, db.total_bins, summarizer, tempconfig);
            if psum.valid_bytes == 0 {
                continue;
            }
            // dbsum.valid_bytes += psum.valid_bytes;
            dbsum.shifted_bytes[psum.temporal_behavior as usize][pid.client_continent as usize] +=
                psum.shifted_bytes;
            dbsum.valid_bytes[psum.temporal_behavior as usize][pid.client_continent as usize] +=
                psum.valid_bytes;
            dbsum.total_bytes[psum.temporal_behavior as usize][pid.client_continent as usize] +=
                db.pathid2traffic[pid];
            dbsum.pathid2summary.insert(Rc::clone(&pid), psum);
        }
        dbsum
    }

    pub fn reclassify(&mut self, db: &db::DB, tempconfig: &TemporalConfig) {
        self.shifted_bytes =
            [[0u64; db::ClientContinent::SIZE as usize]; TemporalBehavior::SIZE as usize];
        self.valid_bytes =
            [[0u64; db::ClientContinent::SIZE as usize]; TemporalBehavior::SIZE as usize];
        self.total_bytes =
            [[0u64; db::ClientContinent::SIZE as usize]; TemporalBehavior::SIZE as usize];
        for (pid, psum) in self.pathid2summary.iter_mut() {
            psum.classify(db.total_bins, db.pathid2time2bin[pid].len() as u32, tempconfig);
            self.shifted_bytes[psum.temporal_behavior as usize][pid.client_continent as usize] +=
                psum.shifted_bytes;
            self.valid_bytes[psum.temporal_behavior as usize][pid.client_continent as usize] +=
                psum.valid_bytes;
            self.total_bytes[psum.temporal_behavior as usize][pid.client_continent as usize] +=
                db.pathid2traffic[pid];
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

        writeln!(bw, "key shifted valid total shifted/global valid/global total/global")?;
        writeln!(bw)?;

        let mut continent_shifted: Vec<u64> =
            Vec::with_capacity(db::ClientContinent::SIZE as usize);
        let mut continent_valid: Vec<u64> = Vec::with_capacity(db::ClientContinent::SIZE as usize);
        let mut continent_total: Vec<u64> = Vec::with_capacity(db::ClientContinent::SIZE as usize);
        for i in 0..(db::ClientContinent::SIZE as usize) {
            continent_shifted.push(self.shifted_bytes.iter().fold(0u64, |acc, a| acc + a[i]));
            continent_valid.push(self.valid_bytes.iter().fold(0u64, |acc, a| acc + a[i]));
            continent_total.push(self.total_bytes.iter().fold(0u64, |acc, a| acc + a[i]));
        }

        let global_total: u64 = continent_total.iter().sum::<u64>();
        assert!(continent_total.len() == db::ClientContinent::SIZE as usize);

        for i in 0..(db::ClientContinent::SIZE as usize) {
            let cont: db::ClientContinent = db::ClientContinent::try_from(i as u8).unwrap();
            writeln!(
                bw,
                "{:?} {} {} {} {:0.3} {:0.3} {:0.3}",
                cont,
                continent_shifted[i],
                continent_valid[i],
                continent_total[i],
                continent_shifted[i] as f64 / global_total as f64,
                continent_valid[i] as f64 / global_total as f64,
                continent_total[i] as f64 / global_total as f64
            )?;
        }
        writeln!(bw)?;

        for i in 0..(TemporalBehavior::SIZE as usize) {
            let behavior: TemporalBehavior = TemporalBehavior::try_from(i as u8).unwrap();
            let shifted: u64 = self.shifted_bytes[i].iter().sum::<u64>();
            let valid: u64 = self.valid_bytes[i].iter().sum::<u64>();
            let total: u64 = self.total_bytes[i].iter().sum::<u64>();
            writeln!(
                bw,
                "{:?} {} {} {} {:0.3} {:0.3} {:0.3}",
                behavior,
                shifted,
                valid,
                total,
                shifted as f64 / global_total as f64,
                valid as f64 / global_total as f64,
                total as f64 / global_total as f64
            )?;
        }
        writeln!(bw)?;

        for i in 0..(TemporalBehavior::SIZE as usize) {
            let behavior: TemporalBehavior = TemporalBehavior::try_from(i as u8).unwrap();
            for j in 0..(db::ClientContinent::SIZE as usize) {
                let cont: db::ClientContinent = db::ClientContinent::try_from(j as u8).unwrap();
                writeln!(
                    bw,
                    "{:?}+{:?} {} {} {} {:0.3} {:0.3} {:0.3}",
                    behavior,
                    cont,
                    self.shifted_bytes[i][j],
                    self.valid_bytes[i][j],
                    self.total_bytes[i][j],
                    self.shifted_bytes[i][j] as f64 / global_total as f64,
                    self.valid_bytes[i][j] as f64 / global_total as f64,
                    self.total_bytes[i][j] as f64 / global_total as f64
                )?;
            }
            writeln!(bw)?;
        }

        Ok(())
    }
}

impl PathSummary {
    fn build(
        pathid: &db::PathId,
        time2bin: &BTreeMap<u64, db::TimeBin>,
        total_bins: u32,
        summarizer: &dyn TimeBinSummarizer,
        tempconfig: &TemporalConfig,
    ) -> PathSummary {
        let mut psum = PathSummary::default();
        let mut is_shifted = false;
        for (time, timebin) in time2bin {
            // This requires that time2bin is a BTreeMap as
            // computing the number of distinct shift events
            // requires processing TimeBins in time order.
            match summarizer.summarize(pathid, timebin) {
                TimeBinSummary::NoRoute => {
                    psum.noroute_bins += 1;
                    psum.noroute_bytes += timebin.bytes_acked_sum;
                }
                TimeBinSummary::WideConfidenceInterval => {
                    psum.wideci_bins += 1;
                    psum.wideci_bytes += timebin.bytes_acked_sum;
                }
                TimeBinSummary::Valid(binstats) => {
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
        }
        psum.classify(total_bins, time2bin.len() as u32, tempconfig);
        psum
    }

    fn classify(&mut self, total_bins: u32, existing_bins: u32, config: &TemporalConfig) {
        self.compute_num_bad_bins(config);
        let frac_existing = existing_bins as f32 / total_bins as f32;
        if frac_existing < config.min_frac_existing_bins {
            self.temporal_behavior = TemporalBehavior::MissingBins;
            return;
        }
        let frac_with_alt = 1.0 - (self.noroute_bins as f32 / existing_bins as f32);
        if frac_with_alt < config.min_frac_bins_with_alternate {
            self.temporal_behavior = TemporalBehavior::NoRoute;
            return;
        }
        let valid_bins: f32 = self.time2binstats.len() as f32;
        let frac_valid: f32 = valid_bins / total_bins as f32;
        if frac_valid < config.min_frac_valid_bins {
            self.temporal_behavior = TemporalBehavior::Undersampled;
            return;
        }
        let frac_shift: f32 = self.shifted_bins as f32 / valid_bins;
        if frac_shift <= config.uneventful_max_frac_shifted_bins {
            self.temporal_behavior = TemporalBehavior::Uneventful;
        } else if frac_shift >= config.continuous_min_frac_shifted_bins {
            self.temporal_behavior = TemporalBehavior::Continuous;
        } else if self.bad_bins >= config.diurnal_min_bad_bins {
            self.temporal_behavior = TemporalBehavior::Diurnal;
        } else {
            self.temporal_behavior = TemporalBehavior::Episodic;
        }
    }

    fn compute_num_bad_bins(&mut self, config: &TemporalConfig) {
        let num_days: u32 = self.day2shifts.len() as u32;
        if num_days < config.min_days {
            self.bad_bins = 0;
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
        self.bad_bins = compute_num_bad_bins(&offset_shift_counts, min_shifts);
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
    const BINS_IN_WEEK: u32 = 7 * 86400 / BIN_DURATION_SECS as u32;
    const NULL_TEMPCONFIG: TemporalConfig = TemporalConfig {
        bin_duration_secs: 900,
        min_days: 7,
        min_frac_existing_bins: 1.0,
        min_frac_bins_with_alternate: 1.0,
        min_frac_valid_bins: 1.0,
        continuous_min_frac_shifted_bins: 1.0,
        diurnal_min_bad_bins: 96,
        diurnal_bad_bin_min_prob_shift: 1.0,
        uneventful_max_frac_shifted_bins: 0.20,
    };
    const DEFAULT_TEMPCONFIG: TemporalConfig = TemporalConfig {
        bin_duration_secs: 900,
        min_days: 7,
        min_frac_existing_bins: 1.0,
        min_frac_bins_with_alternate: 1.0,
        min_frac_valid_bins: 0.8,
        continuous_min_frac_shifted_bins: 0.8,
        diurnal_min_bad_bins: 24,
        diurnal_bad_bin_min_prob_shift: 0.8,
        uneventful_max_frac_shifted_bins: 0.20,
    };

    #[test]
    fn test_path_summary_no_valid() {
        let _pathid: db::PathId = db::tests::make_path_id();

        let summarizer = MinRtt50ImprovementSummarizer {
            minrtt50_min_improv: 2,
            max_minrtt50_diff_ci_halfwidth: 15.0,
            compare_lower_bound: false,
        };

        let time2bin =
            db::TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 51, 50, 100.0, 51, 50, 100.0);
        let psum =
            PathSummary::build(&_pathid, &time2bin, BINS_IN_WEEK, &summarizer, &NULL_TEMPCONFIG);
        assert!(psum.time2binstats.is_empty());
        assert!(psum.day2shifts.is_empty());
        assert!(psum.day2shifts.values().fold(true, |_, e| *e == 0));
        assert!(psum.distinct_shifts == 0);
        assert!(psum.shifted_bins == 0);
        assert!(psum.shifted_bytes == 0);
        assert!(psum.valid_bytes == 0);
        assert!(psum.bad_bins == 0);
        assert!(psum.temporal_behavior == TemporalBehavior::Undersampled);

        let time2bin =
            db::TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 55, 50, 100.0, 55, 50, 100.0);
        let psum =
            PathSummary::build(&_pathid, &time2bin, BINS_IN_WEEK, &summarizer, &NULL_TEMPCONFIG);
        assert!(psum.time2binstats.is_empty());
        assert!(psum.day2shifts.is_empty());
        assert!(psum.day2shifts.values().fold(true, |_, e| *e == 0));
        assert!(psum.distinct_shifts == 0);
        assert!(psum.shifted_bins == 0);
        assert!(psum.shifted_bytes == 0);
        assert!(psum.valid_bytes == 0);
        assert!(psum.bad_bins == 0);
        assert!(psum.temporal_behavior == TemporalBehavior::Undersampled);
    }

    #[test]
    fn test_path_summary_all_valid_no_shifts() {
        let _pathid: db::PathId = db::tests::make_path_id();

        let time2bin =
            db::TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 51, 50, 0.001, 51, 50, 0.001);
        let nbins = time2bin.len();
        let summarizer = MinRtt50ImprovementSummarizer {
            minrtt50_min_improv: 2,
            max_minrtt50_diff_ci_halfwidth: 5.0,
            compare_lower_bound: false,
        };
        let psum =
            PathSummary::build(&_pathid, &time2bin, BINS_IN_WEEK, &summarizer, &NULL_TEMPCONFIG);
        assert!(psum.time2binstats.len() == nbins);
        assert!(psum.day2shifts.len() == 7);
        assert!(psum.day2shifts.values().fold(true, |_, e| *e == 0));
        assert!(psum.distinct_shifts == 0);
        assert!(psum.shifted_bins == 0);
        assert!(psum.shifted_bytes == 0);
        assert!(psum.valid_bytes == db::TimeBin::MOCK_TOTAL_BYTES * (time2bin.len() as u64));
        assert!(psum.bad_bins == 0);
        assert!(psum.temporal_behavior == TemporalBehavior::Uneventful);

        let time2bin =
            db::TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 100, 50, 0.001, 100, 50, 0.001);
        let summarizer = MinRtt50ImprovementSummarizer {
            minrtt50_min_improv: 51,
            max_minrtt50_diff_ci_halfwidth: 5.0,
            compare_lower_bound: false,
        };
        let psum =
            PathSummary::build(&_pathid, &time2bin, BINS_IN_WEEK, &summarizer, &NULL_TEMPCONFIG);
        assert!(psum.time2binstats.len() == nbins);
        assert!(psum.day2shifts.len() == 7);
        assert!(psum.day2shifts.values().fold(true, |_, e| *e == 0));
        assert!(psum.distinct_shifts == 0);
        assert!(psum.shifted_bins == 0);
        assert!(psum.shifted_bytes == 0);
        assert!(psum.valid_bytes == db::TimeBin::MOCK_TOTAL_BYTES * (time2bin.len() as u64));
        assert!(psum.bad_bins == 0);
        assert!(psum.temporal_behavior == TemporalBehavior::Uneventful);
    }

    #[test]
    fn test_path_summary_all_valid_all_shifts() {
        let _pathid: db::PathId = db::tests::make_path_id();

        let time2bin =
            db::TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 51, 50, 0.001, 51, 50, 0.001);
        let nbins = time2bin.len();
        assert!(nbins == BINS_IN_WEEK as usize);
        let summarizer = MinRtt50ImprovementSummarizer {
            minrtt50_min_improv: 1,
            max_minrtt50_diff_ci_halfwidth: 5.0,
            compare_lower_bound: false,
        };
        let psum =
            PathSummary::build(&_pathid, &time2bin, BINS_IN_WEEK, &summarizer, &NULL_TEMPCONFIG);
        assert!(psum.time2binstats.len() == nbins);
        assert!(psum.day2shifts.len() == 7);
        assert!(psum.day2shifts.values().fold(true, |_, e| *e == 86400 / BIN_DURATION_SECS as u32));
        assert!(psum.distinct_shifts == 1);
        assert!(psum.shifted_bins == nbins as u32);
        assert!(psum.shifted_bytes == psum.valid_bytes);
        assert!(psum.valid_bytes == db::TimeBin::MOCK_TOTAL_BYTES * (time2bin.len() as u64));
        assert!(psum.bad_bins == 86400 / (BIN_DURATION_SECS as u32));
        assert!(psum.temporal_behavior == TemporalBehavior::Continuous);

        let time2bin =
            db::TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 55, 50, 0.001, 55, 50, 0.001);
        let summarizer = MinRtt50ImprovementSummarizer {
            minrtt50_min_improv: 5,
            max_minrtt50_diff_ci_halfwidth: 5.0,
            compare_lower_bound: false,
        };
        let psum =
            PathSummary::build(&_pathid, &time2bin, BINS_IN_WEEK, &summarizer, &NULL_TEMPCONFIG);
        assert!(psum.time2binstats.len() == nbins);
        assert!(psum.day2shifts.len() == 7);
        assert!(psum.day2shifts.values().fold(true, |_, e| *e == 86400 / BIN_DURATION_SECS as u32));
        assert!(psum.distinct_shifts == 1);
        assert!(psum.shifted_bins == nbins as u32);
        assert!(psum.shifted_bytes == psum.valid_bytes);
        assert!(psum.valid_bytes == db::TimeBin::MOCK_TOTAL_BYTES * (time2bin.len() as u64));
        assert!(psum.bad_bins == 86400 / (BIN_DURATION_SECS as u32));
        assert!(psum.temporal_behavior == TemporalBehavior::Continuous);
    }

    #[test]
    fn test_path_summary_half_valid_no_shifts() {
        let _pathid: db::PathId = db::tests::make_path_id();

        let time2bin =
            db::TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 51, 50, 0.001, 51, 50, 100.0);
        let nbins = time2bin.len();
        let summarizer = MinRtt50ImprovementSummarizer {
            minrtt50_min_improv: 2,
            max_minrtt50_diff_ci_halfwidth: 5.0,
            compare_lower_bound: false,
        };
        let psum =
            PathSummary::build(&_pathid, &time2bin, BINS_IN_WEEK, &summarizer, &NULL_TEMPCONFIG);
        assert!(psum.time2binstats.len() == nbins / 2);
        assert!(psum.day2shifts.len() == 7);
        assert!(psum.day2shifts.values().fold(true, |_, e| *e == 0));
        assert!(psum.distinct_shifts == 0);
        assert!(psum.shifted_bins == 0);
        assert!(psum.shifted_bytes == 0);
        assert!(psum.valid_bytes == db::TimeBin::MOCK_TOTAL_BYTES * (nbins as u64) / 2);
        assert!(psum.bad_bins == 0);
        assert!(psum.temporal_behavior == TemporalBehavior::Undersampled);

        let time2bin =
            db::TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 100, 50, 0.001, 100, 50, 100.0);
        let summarizer = MinRtt50ImprovementSummarizer {
            minrtt50_min_improv: 51,
            max_minrtt50_diff_ci_halfwidth: 5.0,
            compare_lower_bound: false,
        };
        let psum =
            PathSummary::build(&_pathid, &time2bin, BINS_IN_WEEK, &summarizer, &NULL_TEMPCONFIG);
        assert!(psum.time2binstats.len() == nbins / 2);
        assert!(psum.day2shifts.len() == 7);
        assert!(psum.day2shifts.values().fold(true, |_, e| *e == 0));
        assert!(psum.distinct_shifts == 0);
        assert!(psum.shifted_bins == 0);
        assert!(psum.shifted_bytes == 0);
        assert!(psum.valid_bytes == db::TimeBin::MOCK_TOTAL_BYTES * (nbins as u64) / 2);
        assert!(psum.bad_bins == 0);
        assert!(psum.temporal_behavior == TemporalBehavior::Undersampled);
    }

    #[test]
    fn test_path_summary_half_valid_all_shifts() {
        let _pathid: db::PathId = db::tests::make_path_id();

        let time2bin =
            db::TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 51, 50, 0.001, 51, 50, 100.0);
        let nbins = time2bin.len();
        let summarizer = MinRtt50ImprovementSummarizer {
            minrtt50_min_improv: 1,
            max_minrtt50_diff_ci_halfwidth: 5.0,
            compare_lower_bound: false,
        };
        let psum =
            PathSummary::build(&_pathid, &time2bin, BINS_IN_WEEK, &summarizer, &NULL_TEMPCONFIG);
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
        assert!(psum.bad_bins == 48);
        assert!(psum.temporal_behavior == TemporalBehavior::Undersampled);

        let time2bin =
            db::TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 55, 50, 0.001, 55, 50, 100.0);
        let summarizer = MinRtt50ImprovementSummarizer {
            minrtt50_min_improv: 5,
            max_minrtt50_diff_ci_halfwidth: 5.0,
            compare_lower_bound: false,
        };
        let psum =
            PathSummary::build(&_pathid, &time2bin, BINS_IN_WEEK, &summarizer, &NULL_TEMPCONFIG);
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
        assert!(psum.bad_bins == 48);
        assert!(psum.temporal_behavior == TemporalBehavior::Undersampled);
    }

    #[test]
    fn test_continuous() {
        let _pathid: db::PathId = db::tests::make_path_id();

        let summarizer = MinRtt50ImprovementSummarizer {
            minrtt50_min_improv: 5,
            max_minrtt50_diff_ci_halfwidth: 5.0,
            compare_lower_bound: false,
        };
        let time2bin =
            db::TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 55, 50, 0.001, 55, 50, 0.001);
        let psum =
            PathSummary::build(&_pathid, &time2bin, BINS_IN_WEEK, &summarizer, &DEFAULT_TEMPCONFIG);
        assert!(psum.temporal_behavior == TemporalBehavior::Continuous);

        let time2bin =
            db::TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 55, 50, 0.001, 55, 50, 100.0);
        let mut psum =
            PathSummary::build(&_pathid, &time2bin, BINS_IN_WEEK, &summarizer, &DEFAULT_TEMPCONFIG);
        assert!(psum.temporal_behavior == TemporalBehavior::Undersampled);

        let mut config = DEFAULT_TEMPCONFIG;
        config.min_frac_valid_bins = 0.4;
        psum.classify(BINS_IN_WEEK, time2bin.len() as u32, &config);
        assert!(psum.temporal_behavior == TemporalBehavior::Continuous);
    }

    #[test]
    fn test_diurnal_num_bad_bins() {
        let _pathid: db::PathId = db::tests::make_path_id();

        let summarizer = MinRtt50ImprovementSummarizer {
            minrtt50_min_improv: 5,
            max_minrtt50_diff_ci_halfwidth: 5.0,
            compare_lower_bound: false,
        };
        let time2bin =
            db::TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 51, 50, 0.001, 55, 50, 0.001);
        let mut psum =
            PathSummary::build(&_pathid, &time2bin, BINS_IN_WEEK, &summarizer, &DEFAULT_TEMPCONFIG);
        assert!(psum.temporal_behavior == TemporalBehavior::Diurnal);

        let mut config = DEFAULT_TEMPCONFIG;
        config.diurnal_min_bad_bins = 56; // 0.6 * 96
        psum.classify(BINS_IN_WEEK, time2bin.len() as u32, &config);
        assert!(psum.temporal_behavior == TemporalBehavior::Episodic);

        let mut config = DEFAULT_TEMPCONFIG;
        config.continuous_min_frac_shifted_bins = 0.4;
        psum.classify(BINS_IN_WEEK, time2bin.len() as u32, &config);
        assert!(psum.temporal_behavior == TemporalBehavior::Continuous);
    }

    #[test]
    fn test_diurnal_min_prob_shift() {
        let _pathid: db::PathId = db::tests::make_path_id();

        let summarizer = MinRtt50ImprovementSummarizer {
            minrtt50_min_improv: 5,
            max_minrtt50_diff_ci_halfwidth: 5.0,
            compare_lower_bound: false,
        };
        let mut time2bin =
            db::TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 51, 50, 0.001, 51, 50, 0.001);
        let time2bin2 =
            db::TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 51, 50, 0.001, 55, 50, 0.001);
        time2bin.extend(time2bin2.into_iter().map(|(t, bin)| (t + 7 * 86400, bin)));
        let bins = time2bin.len();
        assert!(bins == 2 * BINS_IN_WEEK as usize);

        let mut psum = PathSummary::build(
            &_pathid,
            &time2bin,
            2 * BINS_IN_WEEK,
            &summarizer,
            &DEFAULT_TEMPCONFIG,
        );
        assert!(psum.temporal_behavior == TemporalBehavior::Episodic);

        let mut config = DEFAULT_TEMPCONFIG;
        config.diurnal_bad_bin_min_prob_shift = 0.5;
        psum.classify(2 * BINS_IN_WEEK, time2bin.len() as u32, &config);
        assert!(psum.temporal_behavior == TemporalBehavior::Diurnal);
    }

    #[test]
    fn test_undersampled() {
        let _pathid: db::PathId = db::tests::make_path_id();

        let summarizer = MinRtt50ImprovementSummarizer {
            minrtt50_min_improv: 5,
            max_minrtt50_diff_ci_halfwidth: 5.0,
            compare_lower_bound: false,
        };

        let time2bin =
            db::TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 55, 50, 100.0, 55, 50, 0.001);
        let mut psum =
            PathSummary::build(&_pathid, &time2bin, BINS_IN_WEEK, &summarizer, &DEFAULT_TEMPCONFIG);
        assert!(psum.temporal_behavior == TemporalBehavior::Undersampled);

        let mut config = DEFAULT_TEMPCONFIG;
        config.min_frac_valid_bins = 0.5;
        psum.classify(BINS_IN_WEEK, time2bin.len() as u32, &config);
        assert!(psum.temporal_behavior == TemporalBehavior::Continuous);

        let time2bin =
            db::TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 51, 50, 100.0, 51, 50, 0.001);
        let mut psum =
            PathSummary::build(&_pathid, &time2bin, BINS_IN_WEEK, &summarizer, &DEFAULT_TEMPCONFIG);
        assert!(psum.temporal_behavior == TemporalBehavior::Undersampled);

        let mut config = DEFAULT_TEMPCONFIG;
        config.min_frac_valid_bins = 0.5;
        psum.classify(BINS_IN_WEEK, time2bin.len() as u32, &config);
        assert!(psum.temporal_behavior == TemporalBehavior::Uneventful);
    }

    #[test]
    fn test_db_reclassify() {
        let summarizer = MinRtt50ImprovementSummarizer {
            minrtt50_min_improv: 5,
            max_minrtt50_diff_ci_halfwidth: 5.0,
            compare_lower_bound: false,
        };

        let mut database: db::DB = db::DB::default();
        let time2bin1 =
            db::TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 51, 50, 0.001, 55, 50, 0.001);
        let nbins: u64 = time2bin1.len() as u64;
        let pid1: db::PathId = db::tests::make_path_id();
        let time2bin2 =
            db::TimeBin::mock_week_minrtt_p50(BIN_DURATION_SECS, 51, 50, 100.0, 55, 50, 0.001);
        let pid2 = db::PathId {
            vip_metro: "gru".to_string(),
            bgp_ip_prefix: "2.0.0.0/24".parse().unwrap(),
            client_continent: db::ClientContinent::Unknown,
        };
        assert!(database.insert(pid1.clone(), time2bin1).is_none());
        assert!(database.insert(pid2.clone(), time2bin2).is_none());

        let mut dbsum: DBSummary = DBSummary::build(&database, &summarizer, &DEFAULT_TEMPCONFIG);
        assert!(dbsum.pathid2summary.len() == 2);
        assert!(dbsum.pathid2summary[&pid1].temporal_behavior == TemporalBehavior::Diurnal);
        assert!(dbsum.pathid2summary[&pid2].temporal_behavior == TemporalBehavior::Undersampled);
        assert!(
            dbsum.valid_bytes[TemporalBehavior::Diurnal as usize].iter().sum::<u64>()
                == nbins * db::TimeBin::MOCK_TOTAL_BYTES
        );
        assert!(
            dbsum.valid_bytes[TemporalBehavior::Undersampled as usize].iter().sum::<u64>()
                == nbins * db::TimeBin::MOCK_TOTAL_BYTES / 2
        );
        assert!(
            dbsum.total_bytes[TemporalBehavior::Undersampled as usize].iter().sum::<u64>()
                == nbins * db::TimeBin::MOCK_TOTAL_BYTES
        );

        let mut config = DEFAULT_TEMPCONFIG;
        config.continuous_min_frac_shifted_bins = 0.5;
        config.min_frac_valid_bins = 0.4;
        dbsum.reclassify(&database, &config);
        assert!(dbsum.pathid2summary.len() == 2);
        assert!(dbsum.pathid2summary[&pid1].temporal_behavior == TemporalBehavior::Continuous);
        assert!(dbsum.pathid2summary[&pid2].temporal_behavior == TemporalBehavior::Continuous);
        assert!(
            dbsum.valid_bytes[TemporalBehavior::Continuous as usize].iter().sum::<u64>()
                == nbins * db::TimeBin::MOCK_TOTAL_BYTES * 3 / 2
        );
        assert!(dbsum.valid_bytes[TemporalBehavior::Diurnal as usize].iter().sum::<u64>() == 0);
        assert!(
            dbsum.valid_bytes[TemporalBehavior::Undersampled as usize].iter().sum::<u64>() == 0
        );
        assert!(
            dbsum.total_bytes[TemporalBehavior::Continuous as usize].iter().sum::<u64>()
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
