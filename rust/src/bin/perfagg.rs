use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::path::PathBuf;

use ipnet::IpNet;
use structopt::StructOpt;

use fbperf::aggregation::{aggregate_prefixes, noncovered_prefixes};
use fbperf::inout::{dump_output, load_input, PrefixData};

#[derive(Debug, StructOpt)]
#[structopt(
    name = "perfagg",
    about = "Aggregate prefixes by performance.",
    rename_all = "kebab-case"
)]
struct Opt {
    #[structopt(parse(from_os_str))]
    /// The input CSV file
    input: PathBuf,
    #[structopt(parse(from_os_str))]
    /// The output CSV file
    output: PathBuf,
    #[structopt(long, default_value = "5")]
    /// Maximum median latency difference between aggregated sibling prefixes
    max_lat50_diff: i32,
    #[structopt(long, default_value = "0.1")]
    /// Maximum average HD-ratio difference between aggregated sibling prefixes
    max_hdratio_diff: f32,
}

fn compare_prefixes(
    pfx1: &IpNet,
    pfx2: &IpNet,
    prefix2data: &HashMap<IpNet, PrefixData>,
    opts: &Opt,
) -> bool {
    let opt1: Option<&PrefixData> = prefix2data.get(pfx1);
    let opt2: Option<&PrefixData> = prefix2data.get(pfx2);
    match (opt1, opt2) {
        (None, None) => panic!("Should't happen."),
        (Some(data1), None) => data1.is_deaggregated(),
        (None, Some(data2)) => data2.is_deaggregated(),
        (Some(data1), Some(data2)) => {
            if data1.origin_asn != data2.origin_asn {
                false
            } else {
                data1.equivalent_performance(data2, opts.max_lat50_diff, opts.max_hdratio_diff)
            }
        }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let opts = Opt::from_args();

    let asn2prefix2data: HashMap<u32, HashMap<IpNet, PrefixData>> = load_input(&opts.input);
    let mut asn2aggregated: HashMap<u32, HashSet<IpNet>> = HashMap::new();

    for (asn, prefix2data) in asn2prefix2data.iter() {
        let noncovered: HashSet<IpNet> = noncovered_prefixes(prefix2data.keys());
        asn2aggregated.insert(
            *asn,
            aggregate_prefixes(&noncovered, &|net1: &IpNet, net2: &IpNet| {
                compare_prefixes(net1, net2, &prefix2data, &opts)
            }),
        );
    }

    dump_output(&asn2prefix2data, &asn2aggregated, &opts.output);

    Ok(())
}
