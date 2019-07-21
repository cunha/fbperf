use std::collections::HashMap;

pub(crate) struct PathPerformance {
    pub total_traffic: u64,
    pub time2binperf: HashMap<u64, TimeBinPerformance>,
}

pub(crate) struct TimeBinPerformance {
    pub total_traffic: u32,
    pub diff_ci: f32,
}