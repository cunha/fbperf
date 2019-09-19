
        fn mock_week_hdratio(
            bin_duration_secs: u64,
            pri_hdratio_even: f32,
            alt_hdratio_even: f32,
            hdratio_var_even: f32,
            pri_hdratio_odd: f32,
            alt_hdratio_odd: f32,
            hdratio_var_odd: f32,
        ) -> BTreeMap<u64, TimeBin> {
            let mut time2bin: BTreeMap<u64, TimeBin> = BTreeMap::new();
            for time in (0..7 * 86400).step_by(bin_duration_secs as usize) {
                if time % (2 * bin_duration_secs) == 0 {
                    let timebin = TimeBin::mock_hdratio(
                        time,
                        pri_hdratio_even,
                        alt_hdratio_even,
                        hdratio_var_even,
                    );
                    time2bin.insert(time, timebin);
                } else {
                    let timebin = TimeBin::mock_hdratio(
                        time,
                        pri_hdratio_odd,
                        alt_hdratio_odd,
                        hdratio_var_odd,
                    );
                    time2bin.insert(time, timebin);
                }
            }
            time2bin
        }

        fn mock_hdratio(
            time: u64,
            pri_hdratio: f32,
            alt_hdratio: f32,
            hdratio_var: f32,
        ) -> TimeBin {
            let mut timebin = TimeBin {
                time_bucket: time,
                bytes_acked_sum: TimeBin::MOCK_TOTAL_BYTES,
                num2route: [None, None, None, None, None, None, None],
            };
            let primary = RouteInfo::mock_hdratio(1, pri_hdratio, hdratio_var);
            let alternate = RouteInfo::mock_hdratio(2, alt_hdratio, hdratio_var);
            timebin.num2route[0] = Some(Box::new(primary));
            timebin.num2route[1] = Some(Box::new(alternate));
            timebin
        }


        pub(crate) fn mock_hdratio(apm_route_num: u8, hdratio: f32, hdratio_var: f32) -> RouteInfo {
            RouteInfo {
                apm_route_num,
                bgp_as_path_len: 3,
                bgp_as_path_prepends: 1,
                peer_type: PeerType::Transit,
                minrtt_num_samples: RouteInfo::MOCK_NUM_SAMPLES,
                minrtt_ms_p50: 20,
                minrtt_ms_p50_ci_halfwidth: 1,
                hdratio_num_samples: RouteInfo::MOCK_NUM_SAMPLES,
                hdratio,
                hdratio_var,
                hdratio_p50: 1.0,
                hdratio_p50_ci_halfwidth: 0.01,
                hdratio_boot: 0.9,
                r0_hdratio_boot_diff_ci_lb: 0.85,
                r0_hdratio_boot_diff_ci_ub: 0.95,
                px_nexthops: 1,
            }
        }