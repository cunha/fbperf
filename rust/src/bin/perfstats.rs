use std::borrow::Borrow;
use std::collections::HashSet;
use std::error::Error;
use std::fs;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

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
    input_files: Vec<PathBuf>,
    #[structopt(long, parse(from_os_str))]
    /// The output directory where to store files
    outdir: PathBuf,
    #[structopt(long, parse(from_os_str), default_value = "")]
    pathid_dump_list_file: PathBuf,
    #[structopt(long, default_value = "900")]
    bin_duration_secs: u32,
    #[structopt(long, default_value = "4")]
    threads: usize,
}

fn build_summarizers(db: &db::DB) -> Vec<Arc<dyn TimeBinSummarizer>> {
    let max_minrtt50_ci_halfwidth: f32 = 20.0;
    let max_hdratio50_ci_halfwidth: f32 = 0.20;
    let mut summarizers: Vec<Arc<dyn TimeBinSummarizer>> = Vec::new();

    for &max_minrtt50_diff_ci_halfwidth in [10.0f32].iter() {
        for &min_minrtt50_diff in [5.0, 10.0, 20.0].iter() {
            let ml = Arc::new(summarizers::opportunity::MinRtt50ImprovementSummarizer {
                minrtt50_min_improv: min_minrtt50_diff,
                max_minrtt50_diff_ci_halfwidth,
                max_hdratio50_diff_ci_halfwidth: 0.1,
                compare_lower_bound: true,
            });
            summarizers.push(ml);
        }
        for &min_minrtt50_diff in [5.0, 10.0, 20.0, 50.0].iter() {
            let ml =
                Arc::new(summarizers::degradation::MinRtt50LowerBoundDegradationSummarizer::new(
                    0.1,
                    min_minrtt50_diff,
                    max_minrtt50_diff_ci_halfwidth,
                    max_minrtt50_ci_halfwidth,
                    db,
                ));
            summarizers.push(ml);
        }
    }
    for &max_hdratio50_diff_ci_halfwidth in [0.1f32].iter() {
        for &min_hdratio_diff in [0.05, 0.1, 0.2].iter() {
            let hl = Arc::new(summarizers::opportunity::HdRatio50ImprovementSummarizer {
                hdratio50_min_improv: min_hdratio_diff,
                max_hdratio50_diff_ci_halfwidth,
                compare_lower_bound: true,
            });
            summarizers.push(hl);
        }
        for &min_hdratio_diff in [0.05, 0.1, 0.2, 0.5, 0.75].iter() {
            let hl =
                Arc::new(summarizers::degradation::HdRatio50LowerBoundDegradationSummarizer::new(
                    0.9,
                    min_hdratio_diff,
                    max_hdratio50_diff_ci_halfwidth,
                    max_hdratio50_ci_halfwidth,
                    db,
                ));
            summarizers.push(hl);
        }
    }

    let relationship_pairs = [
        (
            (1u32 << db::PeerType::PeeringPrivate as u8)
                | (1 << db::PeerType::PeeringPublic as u8)
                | (1 << db::PeerType::PeeringPaid as u8),
            1u32 << db::PeerType::Transit as u8,
        ),
        (
            1u32 << db::PeerType::PeeringPublic as u8,
            (1u32 << db::PeerType::PeeringPrivate as u8 | 1u32 << db::PeerType::PeeringPaid as u8),
        ),
        (
            (1u32 << db::PeerType::PeeringPrivate as u8 | 1u32 << db::PeerType::PeeringPaid as u8),
            1u32 << db::PeerType::PeeringPublic as u8,
        ),
        (1u32 << db::PeerType::Transit as u8, 1u32 << db::PeerType::Transit as u8),
    ];
    for &(primary_bitmask, alternate_bitmask) in &relationship_pairs {
        let ml = Arc::new(summarizers::relationships::MinRtt50RelationshipSummarizer {
            primary_bitmask,
            alternate_bitmask,
            minrtt50_min_improv: 5.0,
            max_minrtt50_diff_ci_halfwidth: 10.0,
            compare_lower_bound: true,
        });
        summarizers.push(ml);
        let ml = Arc::new(summarizers::relationships::HdRatio50RelationshipSummarizer {
            primary_bitmask,
            alternate_bitmask,
            hdratio50_min_improv: 0.05,
            max_hdratio50_diff_ci_halfwidth: 0.2,
            compare_lower_bound: true,
        });
        summarizers.push(ml);
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
        continuous_min_frac_shifted_bins: 0.75,
        diurnal_min_bad_bins: 4,
        diurnal_bad_bin_min_prob_shift: 0.5,
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
        diurnal_bad_bin_min_prob_shift: 0.5,
        uneventful_max_frac_shifted_bins: 0.0,
    });
    configs.push(perfstats::TemporalConfig {
        bin_duration_secs: 900,
        min_days: 2,
        min_frac_existing_bins: 0.8,
        min_frac_bins_with_alternate: 0.8,
        min_frac_valid_bins: 0.8,
        continuous_min_frac_shifted_bins: 0.90,
        diurnal_min_bad_bins: 8,
        diurnal_bad_bin_min_prob_shift: 0.8,
        uneventful_max_frac_shifted_bins: 0.05,
    });
    configs
}

fn load_all_databases(opts: &Opt) -> db::DB {
    let gdb_arc_mtx = Arc::new(Mutex::new(db::DB::default()));

    let pool = rayon::ThreadPoolBuilder::new().num_threads(opts.threads).build().unwrap();
    let wg = WaitGroup::new();

    let bin_duration_secs = opts.bin_duration_secs;
    for input in opts.input_files.iter() {
        let gdb_arc_mtx = Arc::clone(&gdb_arc_mtx);
        let wg = wg.clone();
        let input: PathBuf = input.clone();
        pool.spawn(move || {
            match db::DB::from_file(&input, bin_duration_secs) {
                Ok(partial_db) => {
                    let mut global_db = gdb_arc_mtx.lock().unwrap();
                    global_db.merge(partial_db);
                }
                Err(e) => {
                    error!("IO error processing {:?}", input);
                    error!("{:?}", e);
                }
            }
            drop(gdb_arc_mtx);
            drop(wg);
        });
    }

    wg.wait();
    match Arc::try_unwrap(gdb_arc_mtx) {
        Ok(mutex) => mutex.into_inner().unwrap(),
        Err(_) => {
            unreachable!();
        }
    }
}

fn load_pathid_timeseries(input: &PathBuf) -> Result<HashSet<db::PathId>, Box<dyn Error>> {
    let mut pathids: HashSet<db::PathId> = HashSet::new();
    if input.to_str().unwrap() == "" {
        return Ok(pathids);
    }
    let f = fs::File::open(input)?;
    let filerdr = BufReader::new(f);
    for line in filerdr.lines() {
        let line = line?;
        if let Some(pathid) = db::PathId::from_text(&line) {
            pathids.insert(pathid);
        } else {
            error!("Could not parse PathId from line [{}]", &line);
        }
    }
    info!("will dump {} PathIds for timeseries plotting", pathids.len());
    Ok(pathids)
}

fn dump_pathid_timeseries(
    db: &db::DB,
    dbsum: &perfstats::DBSummary,
    path: &PathBuf,
    pathids: &HashSet<db::PathId>,
) -> Result<(), Box<dyn Error>> {
    let mut filepath = path.clone();
    filepath.push("pathid-timeseries-dump.txt");
    let file =
        fs::OpenOptions::new().read(true).write(true).truncate(true).create(true).open(filepath)?;
    let mut bw = BufWriter::new(file);
    for pid in pathids {
        let pinfo = &db.pathid2info[pid];
        let psum = match dbsum.pathid2summary.get(pid) {
            Some(psum) => psum,
            None => continue,
        };
        for (time, bin) in pinfo.time2bin.iter() {
            let (is_shifted, diff_ci) = match psum.time2binstats.get(time) {
                Some(binstats) => (binstats.is_shifted as u8, binstats.diff_ci),
                None => (0, 0.0),
            };
            write!(bw, "{} {} {} {}", pid.text(), bin.bytes_acked_sum, is_shifted, diff_ci,)?;
            for rtopt in bin.num2route.iter() {
                match rtopt {
                    Some(rtinfo) => write!(
                        bw,
                        "{} {} {}",
                        rtinfo.minrtt_ms_p50, rtinfo.hdratio_p50, rtinfo.px_nexthops
                    )?,
                    None => write!(bw, " NULL NULL NULL")?,
                };
            }
        }
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();
    let opts = Opt::from_args();

    let pathids: HashSet<db::PathId> = load_pathid_timeseries(&opts.pathid_dump_list_file).unwrap();

    let db_arc = Arc::new(load_all_databases(&opts));
    info!("loaded global DB");
    info!("{}", db_arc.stats());

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
        let pathids = pathids.clone();
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
                dump_pathid_timeseries(&db, &dbsum, &dir, &pathids).unwrap_or_else(|e| {
                    error!("{}: could not dump prefix timeseries", summarizer.prefix());
                    error!("{:?}", e);
                })
            }
            drop(db);
            drop(summarizer);
            drop(wg);
        });
    }

    wg.wait();
    Ok(())
}
