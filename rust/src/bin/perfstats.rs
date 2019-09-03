use std::borrow::Borrow;
use std::error::Error;
use std::path::PathBuf;

use log::info;
use structopt::StructOpt;

use fbperf::performance::db;
use fbperf::performance::perfstats;
use fbperf::performance::perfstats::TimeBinSummarizer;
use fbperf::performance::summarizers;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "perfagg",
    about = "Compute performance stats on FB CSV exports.",
    rename_all = "kebab-case"
)]
struct Opt {
    #[structopt(parse(from_os_str))]
    /// The input CSV file
    input: PathBuf,
    #[structopt(long, parse(from_os_str))]
    /// The output directory where to store files
    outdir: PathBuf,
}

fn build_summarizers(db: &db::DB) -> Vec<Box<dyn TimeBinSummarizer>> {
    let mut summarizers: Vec<Box<dyn TimeBinSummarizer>> = Vec::new();
    for &no_alternate_is_valid in [true, false].iter() {
        for &minrtt50_min_improv in [0, 5, 10].iter() {
            let ml = Box::new(summarizers::opportunity::MinRtt50ImprovementSummarizer {
                minrtt50_min_improv,
                max_minrtt50_diff_ci_halfwidth: 10.0,
                no_alternate_is_valid,
                compare_lower_bound: true,
            });
            summarizers.push(ml);
        }
        for &hdratio_min_improv in [0.0, 0.02, 0.05].iter() {
            let hl = Box::new(summarizers::opportunity::HdRatioImprovementSummarizer {
                hdratio_min_improv,
                max_hdratio_diff_ci_halfwidth: 0.05,
                no_alternate_is_valid,
                compare_upper_bound: true,
            });
            summarizers.push(hl);
        }
    }
    let max_minrtt50_diff_ci_halfwidth: f32 = 10.0;
    let max_minrtt50_var: f32 = 25.0;
    for &min_minrtt50_diff_degradation in [0, 5, 10].iter() {
        let ml = Box::new(summarizers::degradation::MinRtt50LowerBoundDegradationSummarizer::new(
            min_minrtt50_diff_degradation,
            max_minrtt50_diff_ci_halfwidth,
            max_minrtt50_var,
            db,
        ));
        summarizers.push(ml);
    }
    let max_hdratio_diff_ci_halfwidth: f32 = 10.0;
    let max_hdratio_var: f32 = 25.0;
    for &min_hdratio_diff_degradation in [0.0, 0.02, 0.05].iter() {
        let hl = Box::new(summarizers::degradation::HdRatioLowerBoundDegradationSummarizer::new(
            min_hdratio_diff_degradation,
            max_hdratio_diff_ci_halfwidth,
            max_hdratio_var,
            db,
        ));
        summarizers.push(hl);
    }
    summarizers
}

fn build_temporal_configs() -> Vec<perfstats::TemporalConfig> {
    let mut configs: Vec<perfstats::TemporalConfig> = Vec::new();
    configs.push(perfstats::TemporalConfig {
        bin_duration_secs: 900,
        min_days: 2,
        min_frac_valid_bins: 0.6,
        continuous_min_frac_shifted_bins: 0.8,
        diurnal_min_frac_bad_bins: 0.1,
        diurnal_bad_bin_min_prob_shift: 0.8,
        uneventful_max_frac_shifted_bins: 0.02,
    });
    configs.push(perfstats::TemporalConfig {
        bin_duration_secs: 900,
        min_days: 2,
        min_frac_valid_bins: 0.8,
        continuous_min_frac_shifted_bins: 0.8,
        diurnal_min_frac_bad_bins: 0.1,
        diurnal_bad_bin_min_prob_shift: 0.8,
        uneventful_max_frac_shifted_bins: 0.02,
    });
    configs.push(perfstats::TemporalConfig {
        bin_duration_secs: 900,
        min_days: 2,
        min_frac_valid_bins: 0.6,
        continuous_min_frac_shifted_bins: 0.8,
        diurnal_min_frac_bad_bins: 0.1,
        diurnal_bad_bin_min_prob_shift: 0.8,
        uneventful_max_frac_shifted_bins: 0.05,
    });
    configs.push(perfstats::TemporalConfig {
        bin_duration_secs: 900,
        min_days: 2,
        min_frac_valid_bins: 0.8,
        continuous_min_frac_shifted_bins: 0.8,
        diurnal_min_frac_bad_bins: 0.1,
        diurnal_bad_bin_min_prob_shift: 0.8,
        uneventful_max_frac_shifted_bins: 0.05,
    });
    configs.push(perfstats::TemporalConfig {
        bin_duration_secs: 900,
        min_days: 2,
        min_frac_valid_bins: 0.6,
        continuous_min_frac_shifted_bins: 0.8,
        diurnal_min_frac_bad_bins: 0.05,
        diurnal_bad_bin_min_prob_shift: 0.75,
        uneventful_max_frac_shifted_bins: 0.02,
    });
    configs.push(perfstats::TemporalConfig {
        bin_duration_secs: 900,
        min_days: 2,
        min_frac_valid_bins: 0.8,
        continuous_min_frac_shifted_bins: 0.8,
        diurnal_min_frac_bad_bins: 0.05,
        diurnal_bad_bin_min_prob_shift: 0.75,
        uneventful_max_frac_shifted_bins: 0.02,
    });
    configs
}

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();
    let opts = Opt::from_args();

    let db = db::DB::from_file(&opts.input)?;
    info!("loaded DB with {} rows", db.rows);
    info!("db has {} paths, {} total traffic", db.pathid2traffic.len(), db.total_traffic);

    let tempconfigs: Vec<perfstats::TemporalConfig> = build_temporal_configs();
    let summarizers: Vec<Box<dyn perfstats::TimeBinSummarizer>> = build_summarizers(&db);

    for summarizer in summarizers.iter() {
        let mut dbsum: perfstats::DBSummary =
            perfstats::DBSummary::build(&db, summarizer.borrow(), &tempconfigs[0]);
        for (i, tempcfg) in tempconfigs.iter().enumerate() {
            let mut dir: PathBuf = opts.outdir.clone();
            dir.push(summarizer.prefix());
            dir.push(tempcfg.prefix());
            info!("processing {}", dir.to_str().unwrap());
            if i > 0 {
                dbsum.reclassify(&db, tempcfg);
            }
            dbsum.dump(&dir)?;
            tempcfg.dump(&dir)?;
        }
    }
    Ok(())
}
