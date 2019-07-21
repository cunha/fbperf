use std::collections::HashMap;
use std::path::Path;

use crate::performance::pathperf::PathPerformance;

trait Aggregator {
    fn insert(&mut self, ps: &PathPerformance);
    fn dump(&self, prefix: &Path);
}

struct AggregatorSet {
    name2agg: HashMap<String, Box<Aggregator>>,
}

impl AggregatorSet {
    fn new() -> Self {
        let mut aggset = AggregatorSet{
            name2agg: HashMap::new(),
        };
        let aggbox: Box<AllAggregator> = Box::new(Default::default());
        aggset.name2agg.insert("all".to_string(), aggbox);
        aggset
    }
    fn insert(&mut self, ps: &PathPerformance) {
        for agg in self.name2agg.values_mut() {
            agg.insert(ps);
        }
    }
    fn dump(&self, prefix: &Path) {
        for (name, agg) in self.name2agg {
            agg.dump(&prefix.join(name));
        }
    }
}

#[derive(Default)]
struct AllAggregator {
    stats: CdfClassStats,
}
impl Aggregator for AllAggregator {
    fn insert(&mut self, ps: &PathPerformance) {
        self.stats.insert(ps);
    }
    fn dump(&self, prefix: &Path) {

        // Open output files
        // Dump CDFs
        // Dump counters
        return;
    }
}

#[derive(Default)]
struct CdfClassStats {
    performance_diffs: Vec<(f32, u32)>,
    num_bins: u32,
    total_traffic: u64,
}
impl CdfClassStats {
    fn insert(&self, ps: &PathPerformance) {
        for (time, binperf) in ps.time2binperf.iter() {
            self.performance_diffs.push((binperf.diff_ci, binperf.total_traffic))
        }
        self.num_bins += 1;
        self.total_traffic += ps.total_traffic;
    }
}

#[derive(Default)]
struct CountersClassStats {
    num_bins: u64,
    total_traffic: u64,
}
impl CountersClassStats {
    fn insert(&self, ps: &PathPerformance) {
        self.num_bins += 1;
        self.total_traffic += ps.total_traffic;
    }
}