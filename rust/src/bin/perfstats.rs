use std::error::Error;
use std::path::PathBuf;

use log::info;
use structopt::StructOpt;

use fbperf::performance::db;
use fbperf::performance::perfstats;
use fbperf::performance::perfstats::TimeBinSummarizer;

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
}

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();
    let opts = Opt::from_args();

    let db = db::DB::from_file(&opts.input)?;
    info!("loaded DB with {} rows", db.rows);
    info!("db has {} paths, {} total traffic", db.pathid2traffic.len(), db.total_traffic);

    let tempcfg = perfstats::TemporalConfig {
        bin_duration_secs: 900,
        min_days: 1,
        min_frac_valid_bins: 0.8,
        continuous_min_frac_shifted_bins: 0.8,
        diurnal_min_frac_bad_bins: 0.3,
        diurnal_bad_bin_min_prob_shift: 0.8,
        uneventful_max_frac_shifted_bins: 0.2,
    };

    let minrtt50_summarizer = perfstats::MinRtt50ImprovementSummarizer {
        minrtt50_diff_min_improv: 5,
        max_minrtt50_diff_ci_halfwidth: 10,
    };
    let dbsum = perfstats::DBSummary::build(&db, &minrtt50_summarizer, &tempcfg);
    info!(
        "{} has {} paths, {} valid traffic",
        minrtt50_summarizer.prefix(),
        dbsum.pathid2summary.len(),
        dbsum.valid_bytes
    );
    dbsum.dump_temporal_tables(&mut std::io::stdout())?;

    let minrtt50_summarizer = perfstats::MinRtt50ImprovementSummarizer {
        minrtt50_diff_min_improv: 5,
        max_minrtt50_diff_ci_halfwidth: 10000,
    };
    let dbsum = perfstats::DBSummary::build(&db, &minrtt50_summarizer, &tempcfg);
    info!(
        "{} has {} paths, {} valid traffic",
        minrtt50_summarizer.prefix(),
        dbsum.pathid2summary.len(),
        dbsum.valid_bytes
    );
    dbsum.dump_temporal_tables(&mut std::io::stdout())?;

    Ok(())
}
