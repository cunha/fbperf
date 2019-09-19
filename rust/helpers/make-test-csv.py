#!/usr/bin/env python3

import sys

import headers


def generate_route(
    apm_route_num,
    minrtt_ms_p50,
    minrtt_ms_p50_ci_lb,
    minrtt_ms_p50_ci_ub,
    hdratio_p50,
    hdratio_p50_ci_lb,
    hdratio_p50_ci_ub,
    hdratio_boot,
):
    num_samples = 100
    as_path_strings = '["1916"]'
    as_path_len = 2 + apm_route_num
    as_path_len_wo_prep = 2
    as_path_prepending = "true"
    px_nexthops = '["130.130.130.1"]'
    return (
        num_samples,
        num_samples,
        apm_route_num,
        "peering",
        "private",
        as_path_len,
        as_path_strings,
        as_path_len_wo_prep,
        as_path_prepending,
        px_nexthops,
        0.0,  # minrtt_ms_p25
        0.0,
        0.0,
        minrtt_ms_p50,
        minrtt_ms_p50_ci_lb,
        minrtt_ms_p50_ci_ub,
        0.0,  # minrtt_ms_p50_var
        hdratio_p50,
        hdratio_p50_ci_lb,
        hdratio_p50_ci_ub,
        0.0,  # hdratio_p50_var
        0.0,  # hdratio_avg
        hdratio_boot,
        0.0,  # hdratio_boot_ci_lb
        0.0,  # hdratio_boot_ci_ub
        0.0,  # hdratio_normal_var
    )


NULL_ROUTE = tuple(["NULL"] * 26)

BIN_DURATION = 900
NUM_ROUTES = 8

VIP_METRO = "gru"
BGP_PREFIX = "150.164.0.0/16"
BGP_PREFIX_LEN = 16
CLIENT_IS_IPV6 = "false"
CLIENT_CONTINENT = "SA"
CLIENT_COUNTRY = "BR"
CONN_SPEED_MAJORITY = "broadband"
CONN_TYPE_FROM_LIGER = "wifi"
CONN_TYPE_FROM_LIGER_SCORE = 1.0
BYTES_ACKED = 10000

ROW_DATA = (
    VIP_METRO,
    BGP_PREFIX,
    BGP_PREFIX_LEN,
    CLIENT_IS_IPV6,
    CLIENT_CONTINENT,
    CLIENT_COUNTRY,
    CONN_SPEED_MAJORITY,
    CONN_TYPE_FROM_LIGER,
    CONN_TYPE_FROM_LIGER_SCORE,
    BYTES_ACKED,
)
APM_ROUTE_DATA = ("false", "false", "false", "false", "false", "false")
NUM_PIVOTS = 2


def generate_week(r0even, r1even, boot_diff_even, r0odd, r1odd, boot_diff_odd):
    rows = list()
    for i, time in enumerate(range(0, 7 * 86400, BIN_DURATION)):
        fields = [time]
        fields.extend(ROW_DATA)
        fields.extend(APM_ROUTE_DATA)
        fields.append(NUM_PIVOTS)
        if (i % 2) == 0:
            r0 = generate_route(*r0even)
            r1 = generate_route(*r1even)
            boot_diff_lb, boot_diff_ub = boot_diff_even
        else:
            r0 = generate_route(*r0odd)
            r1 = generate_route(*r1odd)
            boot_diff_lb, boot_diff_ub = boot_diff_odd
        fields.extend(r0)
        fields.extend(r1)
        # add dummy routes for r2--r7
        for _ in range(2, NUM_ROUTES):
            fields.extend(NULL_ROUTE)
        # add dummy differences for medians:
        for _ in range(1, NUM_ROUTES):
            fields.extend([0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
        # add bootstrapped hdratio mean differences for r1:
        fields.append(boot_diff_lb)
        fields.append(boot_diff_ub)
        # add dummy bootstrapped hdratio mean differences:
        for _ in range(2, NUM_ROUTES):
            fields.extend([0.0, 0.0])
        rows.append(fields)
    return rows

R0 = ( 1, 30.0, 28.0, 32.0, 0.7, 0.68, 0.72, 0.7 )
R0_BOOT_DIFF = (-0.01, 0.01)

BETTER_R1 = ( 1, 20.0, 18.0, 22.0, 0.8, 0.78, 0.82, 0.8 )
BETTER_R1_BOOT_DIFF = (0.08, 0.12)

WORSE_R1 = ( 1, 40.0, 38.0, 42.0, 0.6, 0.58, 0.62, 0.6 )
WORSE_R1_BOOT_DIFF = (-0.12, -0.08)

SPECS = {
    "no-opp--no-deg.csv": (
        R0, R0, R0_BOOT_DIFF, R0, R0, R0_BOOT_DIFF
    ),
    "no-opp-worse--no-deg.csv": (
        R0, WORSE_R1, WORSE_R1_BOOT_DIFF, R0, R0, R0_BOOT_DIFF
    ),
    "half-opp--no-deg.csv": (
        R0, BETTER_R1, BETTER_R1_BOOT_DIFF, R0, R0, R0_BOOT_DIFF
    ),
    "full-opp--no-deg.csv": (
        R0, BETTER_R1, BETTER_R1_BOOT_DIFF, R0, BETTER_R1, BETTER_R1_BOOT_DIFF
    ),
    "half-opp--half-deg.csv": (
        R0, BETTER_R1, BETTER_R1_BOOT_DIFF, WORSE_R1, WORSE_R1, R0_BOOT_DIFF
    ),
    "no-opp--half-deg.csv": (
        R0, R0, R0_BOOT_DIFF, WORSE_R1, WORSE_R1, R0_BOOT_DIFF
    )
}


def main():
    for fname, spec in SPECS.items():
        with open(fname, "w") as fd:
            fd.write('\t'.join(headers.HEADERS) + "\n")
            rows = generate_week(*spec)
            for row in rows:
                fd.write("\t".join(str(x) for x in row) + "\n")


if __name__ == "__main__":
    sys.exit(main())
