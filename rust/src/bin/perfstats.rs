use std::borrow::Borrow;
use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;

use crossbeam::sync::WaitGroup;
use log::{error, info};
use rayon;
use structopt::StructOpt;

use fbperf::performance::db;
use fbperf::performance::perfstats;
use fbperf::performance::perfstats::TimeBinSummarizer;
use fbperf::performance::summarizers;

#[derive(Clone, Debug, StructOpt)]
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
    #[structopt(long, default_value = "4")]
    threads: usize,
}

fn build_summarizers(db: &db::DB) -> Vec<Arc<dyn TimeBinSummarizer>> {
    let max_minrtt50_diff_ci_halfwidth: f32 = 25.0;
    let max_minrtt50_ci_halfwidth: u16 = 25;
    let max_hdratio50_diff_ci_halfwidth: f32 = 0.20;
    let max_hdratio50_ci_halfwidth: f32 = 0.20;
    let max_hdratio_boot_diff_ci_fullwidth: f32 = 0.20;
    let mut summarizers: Vec<Arc<dyn TimeBinSummarizer>> = Vec::new();
    for &min_minrtt50_diff in [0, 5, 10, 20, 50].iter() {
        let ml = Arc::new(summarizers::opportunity::MinRtt50ImprovementSummarizer {
            minrtt50_min_improv: min_minrtt50_diff,
            max_minrtt50_diff_ci_halfwidth,
            compare_lower_bound: true,
        });
        summarizers.push(ml);
        let ml = Arc::new(summarizers::degradation::MinRtt50LowerBoundDegradationSummarizer::new(
            0.1,
            min_minrtt50_diff,
            max_minrtt50_diff_ci_halfwidth,
            max_minrtt50_ci_halfwidth,
            db,
        ));
        summarizers.push(ml);
    }
    for &min_hdratio_diff in [0.0, 0.02, 0.05, 0.1, 0.2].iter() {
        let hl = Arc::new(summarizers::opportunity::HdRatio50ImprovementSummarizer {
            hdratio50_min_improv: min_hdratio_diff,
            max_hdratio50_diff_ci_halfwidth,
            compare_lower_bound: true,
        });
        summarizers.push(hl);
        let hl =
            Arc::new(summarizers::opportunity::HdRatioBootstrapDifferenceImprovementSummarizer {
                hdratio_boot_min_improv: min_hdratio_diff,
                max_hdratio_boot_diff_ci_fullwidth,
                compare_lower_bound: true,
            });
        summarizers.push(hl);
        let hl = Arc::new(summarizers::degradation::HdRatio50LowerBoundDegradationSummarizer::new(
            0.9,
            min_hdratio_diff,
            max_hdratio50_diff_ci_halfwidth,
            max_hdratio50_ci_halfwidth,
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

    let db_arc = Arc::new(db::DB::from_file(&opts.input, opts.bin_duration_secs)?);
    info!("loaded DB with {} rows", db_arc.rows);
    info!("db has {} paths, {} total traffic", db_arc.pathid2info.len(), db_arc.total_traffic);

    let tempconfigs: Vec<perfstats::TemporalConfig> = build_temporal_configs();
    let summarizers: Vec<Arc<dyn perfstats::TimeBinSummarizer>> = build_summarizers(&db_arc);

    let pool = rayon::ThreadPoolBuilder::new().num_threads(opts.threads).build().unwrap();
    let wg = WaitGroup::new();

    for summarizer_arc in summarizers.iter() {
        let db = Arc::clone(&db_arc);
        let summarizer = Arc::clone(&summarizer_arc);
        let wg = wg.clone();
        let opts = opts.clone();
        let tempconfigs = tempconfigs.clone();
        pool.spawn(move || {
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
                dbsum.dump(&dir, &db, &*summarizer).unwrap_or_else(|e| {
                    error!("{}: could not dump DBSummary", summarizer.prefix());
                    error!("{:?}", e);
                });
                tempcfg.dump(&dir).unwrap_or_else(|e| {
                    error!("{}: could not dump TemporalConfig", summarizer.prefix());
                    error!("{:?}", e);
                });
                summarizers::opportunity::dump_opportunity_vs_relationship(&dbsum, &dir)
                    .unwrap_or_else(|e| {
                        error!(
                            "{}: could not dump opportunity_vs_relationship",
                            summarizer.prefix()
                        );
                        error!("{:?}", e);
                    });
            }
            drop(db);
            drop(summarizer);
            drop(wg);
        });
    }

    wg.wait();
    Ok(())
}
