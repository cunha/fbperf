#!/usr/bin/env python3

from dataclasses import dataclass
import datetime
from itertools import cycle
import logging
import math
import os
import sys
import time
from typing import Tuple

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib

# matplotlib.rcParams["text.usetex"] = True

TSTAMP_COL = 0
BYTES_ACKED_COL = 11
R0_FIRST_COL = 19
R0_NUM_SAMPLES_COL = 19
ROUTE_NUM_COLS = 20
METRIC_COL_OFFSET = {"minrtt": 10, "hdratio": 14}
YLABEL = {"minrtt": "Median RTT [ms]", "hdratio": "Median HDratio"}


@dataclass
class GraphSpec:
    pri_metric_col: int
    sec_metric_col: int
    ylabel: str
    xlim: Tuple[float, float]
    ylim: Tuple[float, float]
    day_period: int
    hour_period: int



@dataclass
class TimeData:
    pri_metric: float
    sec_metric: float
    num_bytes: int
    num_samples: int

    @staticmethod
    def build(fields, spec):
        pri_metric = math.nan
        if fields[spec.pri_metric_col] != "NULL":
            pri_metric = float(fields[spec.pri_metric_col])
        sec_metric = math.nan
        if fields[spec.sec_metric_col] != "NULL":
            sec_metric = float(fields[spec.sec_metric_col])
        return TimeData(
            pri_metric,
            sec_metric,
            int(fields[BYTES_ACKED_COL]),
            int(fields[R0_NUM_SAMPLES_COL]),
        )

    @staticmethod
    def average(ds0, ds1, ds2, ds3):
        pri_avg = (
            ds0.pri_metric + ds1.pri_metric + ds2.pri_metric + ds3.pri_metric
        ) / 4
        sec_avg = (
            ds0.sec_metric + ds1.sec_metric + ds2.sec_metric + ds3.sec_metric
        ) / 4
        b_avg = (ds0.num_bytes + ds1.num_bytes + ds2.num_bytes + ds3.num_bytes) / 4
        s_avg = (
            ds0.num_samples + ds1.num_samples + ds2.num_samples + ds3.num_samples
        ) / 4
        return TimeData(pri_avg, sec_avg, b_avg, s_avg)


def read(fpath, spec):
    ts2data = dict()
    with open(fpath) as fd:
        for line in fd:
            fields = line.split("\t")
            tstamp = int(fields[TSTAMP_COL])
            data = TimeData.build(fields, spec)
            ts2data[tstamp] = data
    ts, ds = zip(*list(sorted(ts2data.items())))
    # averaging over 3 bins to reduce noise:
    ts2data = dict()
    for i in range(1, len(ts) - 2, 4):
        ts2data[ts[i]] = TimeData.average(ds[i - 1], ds[i], ds[i + 1], ds[i + 2])
    return ts2data


def plot(ts2data, spec, outfp):
    outdir = os.path.split(outfp)[0]
    os.makedirs(outdir, exist_ok=True)

    lines = ["-", "--", "-.", ":"]
    linecycler = cycle(lines)
    plt.style.use("seaborn-colorblind")

    # fmt = mdates.DateFormatter("%Y-%m-%d\n%H:%M")
    # fmt = mdates.DateFormatter("%Y-%m-%d")
    major_fmt = mdates.DateFormatter("%b %d")
    minor_fmt = mdates.DateFormatter("%H:%M")

    fig, ax1 = plt.subplots(figsize=(12, 4))
    ax1.tick_params(axis="y", which="major", labelsize=12)
    # ax1.tick_params(axis="x", which="major", labelsize=16)
    # ax1.tick_params(axis="x", which="minor", labelsize=14)
    # ax1.xaxis_date()
    # ax1.xaxis.set_major_formatter(major_fmt)
    # ax1.xaxis.set_major_locator(mdates.DayLocator(interval=spec.day_period))

    ax1.set_xlim(
        datetime.datetime.fromtimestamp(spec.xlim[0]),
        datetime.datetime.fromtimestamp(spec.xlim[1]),
    )

    ax1.set_ylim(spec.ylim[0], spec.ylim[1])
    ax1.set_ylabel(spec.ylabel, fontsize=16)

    ts, ds = zip(*list(sorted(ts2data.items())))
    ts = list(datetime.datetime.fromtimestamp(t) for t in ts)

    pri = list(d.pri_metric for d in ds)
    ax1.plot(ts, pri, next(linecycler), label="Preferred", lw=2)

    if spec.pri_metric_col != spec.sec_metric_col:
        sec = list(d.sec_metric for d in ds)
        ax1.plot(ts, sec, next(linecycler), color="red", label="Alternate", lw=3)

    samples = list(d.num_samples for d in ds)
    maxsamples = max(samples)
    samples = list(s / maxsamples for s in samples)

    bytesvec = list(d.num_bytes for d in ds)
    maxbytes = max(bytesvec)
    bytesvec = list(b / maxbytes for b in bytesvec)

    print(maxsamples, maxbytes)

    ax2 = ax1.twinx()
    ax2.tick_params(axis="x", which="major", labelsize=16)
    ax2.xaxis_date()
    ax2.xaxis.set_major_formatter(major_fmt)
    ax2.xaxis.set_major_locator(mdates.DayLocator(interval=spec.day_period))
    if spec.hour_period is not None:
        ax1.xaxis.set_minor_formatter(minor_fmt)
        ax1.xaxis.set_minor_locator(mdates.HourLocator(interval=spec.hour_period))
    ax2.set_xlim(
        datetime.datetime.fromtimestamp(spec.xlim[0]),
        datetime.datetime.fromtimestamp(spec.xlim[1]),
    )
    ax2.set_ylim(0, 5)
    ax2.fill_between(ts, samples, color="gray")
    ax2.set_yticks([])

    fig.autofmt_xdate()
    fig.tight_layout()
    # plt.legend(loc="best", fontsize=16)
    # plt.grid()
    plt.savefig(outfp, bbox_inches="tight", transparent=False)
    plt.close(fig)


def get_metric_col(route_idx, metric):
    metric_offset = METRIC_COL_OFFSET[metric]
    return R0_FIRST_COL + ROUTE_NUM_COLS * route_idx + metric_offset


CONFIGS = [
    # {
    #     "input_fpath": "outdir/kul,202.44.224.0-19",
    #     "output_fpath": "outdir/kul,202.44.224.0-19.png",
    #     "pri_route_idx": 1,
    #     "sec_route_idx": 2,
    #     "metric": "minrtt",
    #     "xlim": (
    #         time.mktime(time.strptime("2019-09-11 18:00", "%Y-%m-%d %H:%M")),
    #         time.mktime(time.strptime("2019-09-15 18:00", "%Y-%m-%d %H:%M")),
    #     ),
    #     "ylim": (20, 60),
    #     "day_period": 1,
    #     "hour_period": 6,
    # },
    # {
    #     "input_fpath": "outdir/lax,2001:e60:a000::-36",
    #     "output_fpath": "outdir/lax,2001:e60:a000::-36.png",
    #     "pri_route_idx": 1,
    #     "sec_route_idx": 3,
    #     "metric": "minrtt",
    #     "xlim": (
    #         time.mktime(time.strptime("2019-09-10 18:00", "%Y-%m-%d %H:%M")),
    #         time.mktime(time.strptime("2019-09-15 18:00", "%Y-%m-%d %H:%M")),
    #     ),
    #     "ylim": (140, 190),
    #     "day_period": 1,
    #     "hour_period": 6,
    # },
    # {
    #     "input_fpath": "outdir/qro,177.237.160.0-19",
    #     "output_fpath": "outdir/qro,177.237.160.0-19.png",
    #     "pri_route_idx": 1,
    #     "sec_route_idx": 2,
    #     "metric": "minrtt",
    #     "xlim": (
    #         time.mktime(time.strptime("2019-09-07 04:00", "%Y-%m-%d %H:%M")),
    #         time.mktime(time.strptime("2019-09-16 18:00", "%Y-%m-%d %H:%M")),
    #     ),
    #     "ylim": (45, 70),
    #     "day_period": 1,
    #     "hour_period": 6,
    # },
    # {
    #     "input_fpath": "outdir/dfw,136.50.0.0-20",
    #     "output_fpath": "outdir/dfw,136.50.0.0-20.png",
    #     "pri_route_idx": 1,
    #     "sec_route_idx": 2,
    #     "metric": "minrtt",
    #     "xlim": (
    #         time.mktime(time.strptime("2019-09-07 04:00", "%Y-%m-%d %H:%M")),
    #         time.mktime(time.strptime("2019-09-16 18:00", "%Y-%m-%d %H:%M")),
    #     ),
    #     "ylim": (0, 120),
    #     "day_period": 1,
    #     "hour_period": 6,
    # },
    {
        "input_fpath": "outdir/dfw,168.194.0.0-24",
        "output_fpath": "outdir/dfw,168.194.0.0-24.png",
        "pri_metric_col": get_metric_col(route_idx=1, metric="minrtt"),
        "sec_metric_col": get_metric_col(route_idx=1, metric="minrtt"),
        "ylabel": YLABEL["minrtt"],
        "xlim": (
            time.mktime(time.strptime("2019-09-13 09:00", "%Y-%m-%d %H:%M")),
            time.mktime(time.strptime("2019-09-16 09:00", "%Y-%m-%d %H:%M")),
        ),
        "ylim": (35, 66),
        "day_period": 1,
        "hour_period": None,
    },
]


def main():
    logging.getLogger("matplotlib").setLevel(logging.ERROR)

    for config in CONFIGS:
        inputfp = config["input_fpath"]
        outfp = config["output_fpath"]
        del config["input_fpath"]
        del config["output_fpath"]
        spec = GraphSpec(**config)
        ts2data = read(inputfp, spec)
        plot(ts2data, spec, outfp)


if __name__ == "__main__":
    sys.exit(main())
