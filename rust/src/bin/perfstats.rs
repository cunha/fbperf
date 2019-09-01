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

fn build_summarizers() -> Vec<Box<dyn TimeBinSummarizer>> {
    let mut summarizers: Vec<Box<dyn TimeBinSummarizer>> = Vec::new();
    for &no_alternate_is_valid in [true, false].iter() {
        let m1 = Box::new(summarizers::MinRtt50ImprovementSummarizer {
            minrtt50_diff_min_improv: 1,
            max_minrtt50_diff_ci_halfwidth: 5.0,
            no_alternate_is_valid,
        });
        let m2 = Box::new(summarizers::MinRtt50ImprovementSummarizer {
            minrtt50_diff_min_improv: 5,
            max_minrtt50_diff_ci_halfwidth: 5.0,
            no_alternate_is_valid,
        });
        let m3 = Box::new(summarizers::MinRtt50ImprovementSummarizer {
            minrtt50_diff_min_improv: 10,
            max_minrtt50_diff_ci_halfwidth: 5.0,
            no_alternate_is_valid,
        });
        let ml = Box::new(summarizers::MinRtt50LowerBoundImprovementSummarizer {
            max_minrtt50_diff_ci_halfwidth: 5.0,
            no_alternate_is_valid,
        });
        let h1 = Box::new(summarizers::HdRatioImprovementSummarizer {
            hdratio_min_improv: 0.01,
            max_hdratio_diff_ci_halfwidth: 0.05,
            no_alternate_is_valid,
        });
        let h2 = Box::new(summarizers::HdRatioImprovementSummarizer {
            hdratio_min_improv: 0.03,
            max_hdratio_diff_ci_halfwidth: 0.05,
            no_alternate_is_valid,
        });
        let h3 = Box::new(summarizers::HdRatioImprovementSummarizer {
            hdratio_min_improv: 0.05,
            max_hdratio_diff_ci_halfwidth: 0.05,
            no_alternate_is_valid,
        });
        let hl = Box::new(summarizers::HdRatioLowerBoundImprovementSummarizer {
            max_hdratio_diff_ci_halfwidth: 0.05,
            no_alternate_is_valid,
        });
        let mut batch: Vec<Box<dyn TimeBinSummarizer>> = vec![m1, m2, m3, ml, h1, h2, h3, hl];
        summarizers.append(&mut batch);
    }
    summarizers
}

fn build_temporal_configs() -> Vec<perfstats::TemporalConfig> {
    let c1 = perfstats::TemporalConfig {
        bin_duration_secs: 900,
        min_days: 2,
        min_frac_valid_bins: 0.8,
        continuous_min_frac_shifted_bins: 0.8,
        diurnal_min_frac_bad_bins: 0.1,
        diurnal_bad_bin_min_prob_shift: 0.9,
        uneventful_max_frac_shifted_bins: 0.05,
    };
    let c2 = perfstats::TemporalConfig {
        bin_duration_secs: 900,
        min_days: 4,
        min_frac_valid_bins: 0.8,
        continuous_min_frac_shifted_bins: 0.8,
        diurnal_min_frac_bad_bins: 0.1,
        diurnal_bad_bin_min_prob_shift: 0.9,
        uneventful_max_frac_shifted_bins: 0.05,
    };
    vec![c1, c2]
}

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();
    let opts = Opt::from_args();

    let db = db::DB::from_file(&opts.input)?;
    info!("loaded DB with {} rows", db.rows);
    info!("db has {} paths, {} total traffic", db.pathid2traffic.len(), db.total_traffic);

    let tempconfigs: Vec<perfstats::TemporalConfig> = build_temporal_configs();
    let summarizers: Vec<Box<dyn perfstats::TimeBinSummarizer>> = build_summarizers();

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
