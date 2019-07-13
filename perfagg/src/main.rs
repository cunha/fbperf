extern crate csv;
extern crate ipnet;
extern crate serde;
extern crate structopt;

use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::path::PathBuf;

use ipnet::IpNet;
use structopt::StructOpt;

mod aggregation;
mod inout;
mod timeseries;
use aggregation::aggregate_prefixes;
use inout::{dump_output, load_input, RouteInfo};
use timeseries::TimeSeries;

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
    max_hdratio_diff: f64,
}

struct PrefixData {
    prefix: IpNet,
    bgp_prefix: IpNet,
    origin_asn: u32,
    timeseries: TimeSeries<RouteInfo>,
    total_traffic: u64,
}

impl PrefixData {
    fn new(init: &RouteInfo) -> PrefixData {
        PrefixData {
            prefix: init.prefix,
            bgp_prefix: init.bgp_prefix,
            origin_asn: init.origin_asn,
            timeseries: TimeSeries::new(),
            total_traffic: 0,
        }
    }
    fn is_deaggregated(&self) -> bool {
        self.bgp_prefix.contains(&self.prefix)
    }
    fn equivalent_performance(&self, other: &PrefixData, opts: &Opt) -> bool {
        for (time, route1) in self.timeseries.iter() {
            let route2 = match other.timeseries.get(*time) {
                None => continue,
                Some(route2) => route2,
            };
            let lat50_diff = (route1.lat50 - route2.lat50).abs();
            if lat50_diff > opts.max_lat50_diff {
                return false;
            }
            let hdratio_diff = (route1.hdratio - route2.hdratio).abs();
            if hdratio_diff > opts.max_hdratio_diff {
                return false;
            }
        }
        true
    }
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
        (None, None) => false,
        (Some(data1), None) => data1.is_deaggregated(),
        (None, Some(data2)) => data2.is_deaggregated(),
        (Some(data1), Some(data2)) => {
            if data1.origin_asn != data2.origin_asn {
                false
            } else {
                data1.equivalent_performance(data2, opts)
            }
        }
    }

}

fn max_routable_prefix_length(prefix: &IpNet) -> u8 {
    match prefix {
        IpNet::V4(_) => 24,
        IpNet::V6(_) => 48,
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let opts = Opt::from_args();

    let prefix2data: HashMap<IpNet, PrefixData> = load_input(&opts.input);

    let max_len_prefixes: HashSet<IpNet> = prefix2data
        .keys()
        .filter(|p| p.prefix_len() == max_routable_prefix_length(p))
        .cloned()
        .collect();

    let aggregated = aggregate_prefixes(&max_len_prefixes, &|net1: &IpNet, net2: &IpNet| {
        compare_prefixes(net1, net2, &prefix2data, &opts)
    });

    dump_output(&prefix2data, &aggregated, &opts.output);

    Ok(())
}
