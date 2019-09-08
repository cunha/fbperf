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
    #[structopt(long, default_value = "900")]
    bin_duration_secs: u32,
}

fn build_summarizers(db: &db::DB) -> Vec<Box<dyn TimeBinSummarizer>> {
    let max_minrtt50_diff_ci_halfwidth: f32 = 20.0;
    let max_minrtt50_var: f32 = 25.0;
    let max_hdratio_diff_ci_halfwidth: f32 = 0.1;
    let max_hdratio_var: f32 = 0.5;
    let mut summarizers: Vec<Box<dyn TimeBinSummarizer>> = Vec::new();
    for &min_minrtt50_diff in [0, 5, 10, 20, 50].iter() {
        let ml = Box::new(summarizers::opportunity::MinRtt50ImprovementSummarizer {
            minrtt50_min_improv: min_minrtt50_diff,
            max_minrtt50_diff_ci_halfwidth,
            compare_lower_bound: true,
        });
        summarizers.push(ml);
        let ml = Box::new(summarizers::degradation::MinRtt50LowerBoundDegradationSummarizer::new(
            0.1,
            min_minrtt50_diff,
            max_minrtt50_diff_ci_halfwidth,
            max_minrtt50_var,
            db,
        ));
        summarizers.push(ml);
    }
    for &min_hdratio_diff in [0.0, 0.02, 0.05, 0.1, 0.2].iter() {
        let hl = Box::new(summarizers::opportunity::HdRatioImprovementSummarizer {
            hdratio_min_improv: min_hdratio_diff,
            max_hdratio_diff_ci_halfwidth,
            compare_lower_bound: true,
        });
        summarizers.push(hl);
        let hl = Box::new(summarizers::degradation::HdRatioLowerBoundDegradationSummarizer::new(
            0.9,
            min_hdratio_diff,
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
        min_frac_existing_bins: 0.6,
        min_frac_bins_with_alternate: 0.6,
        min_frac_valid_bins: 0.6,
        continuous_min_frac_shifted_bins: 0.5,
        diurnal_min_bad_bins: 1,
        diurnal_bad_bin_min_prob_shift: 0.75,
        uneventful_max_frac_shifted_bins: 0.01,
    });
    configs.push(perfstats::TemporalConfig {
        bin_duration_secs: 900,
        min_days: 2,
        min_frac_existing_bins: 0.6,
        min_frac_bins_with_alternate: 0.6,
        min_frac_valid_bins: 0.6,
        continuous_min_frac_shifted_bins: 0.5,
        diurnal_min_bad_bins: 1,
        diurnal_bad_bin_min_prob_shift: 0.75,
        uneventful_max_frac_shifted_bins: 0.0,
    });
    configs.push(perfstats::TemporalConfig {
        bin_duration_secs: 900,
        min_days: 2,
        min_frac_existing_bins: 0.6,
        min_frac_bins_with_alternate: 0.6,
        min_frac_valid_bins: 0.6,
        continuous_min_frac_shifted_bins: 0.75,
        diurnal_min_bad_bins: 1,
        diurnal_bad_bin_min_prob_shift: 0.75,
        uneventful_max_frac_shifted_bins: 0.01,
    });
    configs.push(perfstats::TemporalConfig {
        bin_duration_secs: 900,
        min_days: 2,
        min_frac_existing_bins: 0.6,
        min_frac_bins_with_alternate: 0.6,
        min_frac_valid_bins: 0.6,
        continuous_min_frac_shifted_bins: 0.75,
        diurnal_min_bad_bins: 1,
        diurnal_bad_bin_min_prob_shift: 0.75,
        uneventful_max_frac_shifted_bins: 0.0,
    });
    configs
}

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();
    let opts = Opt::from_args();

    let db = db::DB::from_file(&opts.input, opts.bin_duration_secs)?;
    info!("loaded DB with {} rows", db.rows);
    info!("db has {} paths, {} total traffic", db.pathid2traffic.len(), db.total_traffic);

    let tempconfigs: Vec<perfstats::TemporalConfig> = build_temporal_configs();
    let summarizers: Vec<Box<dyn perfstats::TimeBinSummarizer>> = build_summarizers(&db);

    for summarizer in summarizers.iter() {
        let mut dbsum: perfstats::DBSummary =
            perfstats::DBSummary::build(&db, summarizer.borrow(), &tempconfigs[0]);
        for (i, tempcfg) in tempconfigs.iter().enumerate() {
            let mut dir: PathBuf = opts.outdir.clone();
            dir.push(tempcfg.prefix());
            dir.push(summarizer.prefix());
            info!("processing {}", dir.to_str().unwrap());
            if i > 0 {
                dbsum.reclassify(&db, tempcfg);
            }
            dbsum.dump(&dir, &db, summarizer.borrow())?;
            tempcfg.dump(&dir)?;
        }
    }
    Ok(())
}
