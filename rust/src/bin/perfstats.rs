// use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::path::PathBuf;

use std::fs::File;
use std::io::BufReader;

use flate2::bufread::GzDecoder;
use structopt::StructOpt;

use fbperf::perfdb::DB;

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





struct PathPerformanceStats {

}

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();
    let opts = Opt::from_args();

    let f = File::open(opts.input)?;
    let filerdr = BufReader::new(f);
    let gzrdr = GzDecoder::new(filerdr);
    let mut csvrdr = csv::ReaderBuilder::new().delimiter(b'\t').from_reader(gzrdr);

    let db = DB::from_csv_reader(&mut csvrdr);
    dbg!(db.rows, db.parsing_errors);

    Ok(())
}
