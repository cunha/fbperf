import bisect
from collections import defaultdict
import csv
import gzip
import ipaddress
import glob
import logging
import math
import os
import pickle
import re

from mpl_toolkits.axes_grid1 import host_subplot
import mpl_toolkits.axisartist as AA

import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np


CA_STATE_OPEN = 0
CA_STATE_DISORDER = 1
CA_STATE_CWR = 2
CA_STATE_RECOVERY = 3
CA_STATE_LOSS = 4


SERVER_IPS = [ipaddress.IPv4Address('10.255.0.2'),
              ipaddress.IPv4Address('10.255.0.3')]
MONITORED_PREFIX = ipaddress.IPv4Network('10.128.0.0/10')


class PathUtils:
    @staticmethod
    def patch_rundir(rundir, name):
        base = 'emulation/runs'
        if base not in rundir:
            return None
        patched = rundir.replace(base, '%s/%s' % (base, name))
        return patched if os.path.exists(patched) else None


class EventUtils:
    @staticmethod
    def connection_termination(row):
        if row['event_type_name'] != 'INET_SOCK_SET_STATE':
            return False
        if row['new_skt_state_name'] != 'TCP_CLOSE':
            return False
        # if row['old_skt_state_name'] == 'TCP_SYN_RECV':
        #     return False
        return True

    @staticmethod
    def ca_event_cwr_or_loss(row):
        if row['event_type_name'] != 'TCP_CA_EVENT':
            return False
        return (row['ca_event_name'] == 'CA_EVENT_COMPLETE_CWR' or
                row['ca_event_name'] == 'CA_EVENT_LOSS')

    @staticmethod
    def ca_state_degradation(row):
        if row['event_type_name'] != 'TCP_SET_CA_STATE':
            return False
        if int(row['new_ca_state']) < int(row['old_ca_state']):
            return False
        return (row['new_ca_state_name'] == 'TCP_CA_Recovery' or
                row['new_ca_state_name'] == 'TCP_CA_Loss')


class ApmClientUtils:
    PREFIX = ipaddress.IPv4Network('10.128.0.0/10')

    @staticmethod
    def addr2group(v4addr, ngroups):
        group = int(v4addr.packed[2]) % ngroups
        return ngroups if group == 0 else group

    @staticmethod
    def addr2path(v4addr):
        path = int(v4addr.packed[1]) - 128
        path = path//16
        return path

    @staticmethod
    def parse_apmdir(apmdir, ngroups):
        pid_cid_str = apmdir.split('-')[-1]
        pid, cid = pid_cid_str.split('.')
        pid = int(pid)
        cid = int(cid)
        group = 0
        if ngroups != 0:
            group = ngroups if (cid % ngroups) == 0 else (cid % ngroups)
        return pid, cid, group


def iface2group(iface, ngroups):
    m = re.match(r'vep\d+c(\d+)g', iface)
    if not m:
        m = re.match(r'ven(\d+)g', iface)
    if not m:
        raise ValueError('cannot parse [%s]' % iface)
    grp = int(m.group(1)) % ngroups
    return grp if grp != 0 else ngroups


def iface2path(iface):
    m = re.match(r'vegg(\d+)gs', iface)
    if not m:
        raise ValueError('cannot parse [%s]' % iface)
    return int(m.group(1))


def get_percentiles(data):
    sdata = sorted(data)
    percentiles = list()
    inc = len(data)/100
    i = inc/2
    for i in range(100):
        idx = int(i*len(data)/100 + len(data)/200)
        percentiles.append(sdata[idx])
    return percentiles


def get_quartiles(data):
    sdata = sorted(data)
    length = len(sdata)
    i25 = int(length/4 - 0.5)
    w25 = (length/4 - 0.5) - i25
    q25 = (1-w25)*sdata[i25] + w25*sdata[i25+1]
    i50 = int(length/2 - 0.5)
    w50 = (length/2 - 0.5) - i50
    q50 = (1-w50)*sdata[i50] + w50*sdata[i50+1]
    i75 = int(3*length/4 - 0.5)
    w75 = (3*length/4 - 0.5) - i75
    q75 = (1-w75)*sdata[i75] + w75*sdata[i75+1]
    return (q25, q50, q75)


def log_likelihood_single(connection_lossrates, avg_lossrate):
    if avg_lossrate == 0:
        assert sum(connection_lossrates) == 0
        return 0.0
    percentiles = get_percentiles(connection_lossrates)
    logprod = 0
    for p in percentiles:
        logprod += math.log(((1-avg_lossrate)**p) * avg_lossrate)
    return logprod



def get_metric_cdf(rundir):
    fn = os.path.join(rundir, 'metric-43pkts.pickle')
    if os.path.exists(fn):
        with open(fn, 'rb') as fd:
            data = pickle.load(fd)
    else:
        rs = RunStats.get(rundir, 0)
        data = rs.grp_segs_1stloss[0] + list(100000 for _ in
                rs.grp_stats_pkts[0][43]['segs_end_noloss'])
        data.sort()
        with open(fn, 'wb') as fd:
            pickle.dump(data, fd)
    return data



def get_avg_utilization(rundir):
    utils = load_gs_utilizations(rundir)
    return sum(utils)/len(utils)


class F1Stats:
    VERSION = 1
    MAXPKTS = 128
    PREFIX = ipaddress.IPv4Network('10.128.0.0/10')

    def __init__(self, connections, woloss, fstloss, metric):
        assert len(connections) == F1Stats.MAXPKTS
        assert len(woloss) == F1Stats.MAXPKTS
        assert len(fstloss) == F1Stats.MAXPKTS
        assert len(metric) == F1Stats.MAXPKTS
        self.version = F1Stats.VERSION
        self.connections = list(connections)
        self.woloss = list(woloss)
        self.fstloss = list(fstloss)
        self.metric = list(metric)

    @staticmethod
    def get(rundir):
        picklefn = os.path.join(rundir, 'f1stats.v%d.pickle' % F1Stats.VERSION)
        if os.path.exists(picklefn):
            with open(picklefn, 'rb') as fd:
                f1stats = pickle.load(fd)
                assert f1stats.VERSION == F1Stats.VERSION
        else:
            logging.info('building %s', picklefn)
            f1stats = F1Stats.__build(rundir)
            with open(picklefn, 'wb') as fd:
                pickle.dump(f1stats, fd)
        return f1stats

    @staticmethod
    def __build(rundir):
        connections = list(0 for _ in range(F1Stats.MAXPKTS))
        fstloss = list(0 for _ in range(F1Stats.MAXPKTS))
        woloss = list(0 for _ in range(F1Stats.MAXPKTS))

        fd = gzip.open(os.path.join(rundir, 'events.dump.gz'), 'rt')
        for row in csv.DictReader(fd):
            dst = ipaddress.IPv4Address(row["dst"].split(':')[0])
            if dst not in F1Stats.PREFIX:
                continue
            if connection_termination(row):
                segs = int(row['segs_out'])
                if segs <= 0:
                    continue
                retrans = int(row['total_retrans'])
                pktsout = min(F1Stats.MAXPKTS-1, segs - retrans)
                connections[pktsout] += 1
                if retrans == 0:
                    woloss[pktsout] += 1
            elif ca_state_degradation(row):
                if row['total_retrans'] is None:
                    continue
                if int(row['total_retrans']) == 0:
                    segsnow = int(row['segs_out']) - int(row['packets_out'])
                    segsnow = min(F1Stats.MAXPKTS-1, segsnow)
                    fstloss[segsnow] += 1
        fd.close()

        cum_connections = list(0 for _ in range(F1Stats.MAXPKTS))
        cum_fstloss = list(0 for _ in range(F1Stats.MAXPKTS))
        cum_woloss = list(0 for _ in range(F1Stats.MAXPKTS))
        metric = list(0 for _ in range(F1Stats.MAXPKTS))
        cum_connections[F1Stats.MAXPKTS-1] = connections[F1Stats.MAXPKTS-1]
        cum_fstloss[F1Stats.MAXPKTS-1] = fstloss[F1Stats.MAXPKTS-1]
        cum_woloss[F1Stats.MAXPKTS-1] = woloss[F1Stats.MAXPKTS-1]
        metric[F1Stats.MAXPKTS-1] = float(fstloss[F1Stats.MAXPKTS-1]
                + woloss[F1Stats.MAXPKTS-1])/connections[F1Stats.MAXPKTS-1]
        for i in range(F1Stats.MAXPKTS-2, -1, -1):
            cum_connections[i] = cum_connections[i+1] + connections[i]
            cum_fstloss[i] = cum_fstloss[i+1] + fstloss[i]
            cum_woloss[i] = cum_woloss[i+1] + woloss[i]
            metric[i] = float(cum_fstloss[i] + cum_woloss[i])/cum_connections[i]

        return F1Stats(connections, woloss, fstloss, metric)


class RunStats:
    VERSION = 3
    PREFIX = ipaddress.IPv4Network('10.128.0.0/10')

    def __init__(self, ngroups, # grp_cwnd_ca,
                                # grp_cwnd_1stloss,
                                # grp_cwnd_end_large_noloss,
                                grp_segs_1stloss,
                                grp_segs_end,
                                grp_retrans_end,
                                grp_lossrate_end,
                                grp_minrtt_end,
                                grp_srtt_end,
                                grp_stats_pkts):
        self.version = RunStats.VERSION
        self.ngroups = int(ngroups)
        # self.grp_cwnd_ca = dict(grp_cwnd_ca)
        # self.grp_cwnd_1stloss = dict(grp_cwnd_1stloss)
        # self.grp_cwnd_end_large_noloss = dict(grp_cwnd_end_large_noloss)
        self.grp_segs_1stloss = dict(grp_segs_1stloss)
        self.grp_segs_end = dict(grp_segs_end)
        self.grp_retrans_end = dict(grp_retrans_end)
        self.grp_lossrate_end = dict(grp_lossrate_end)
        self.grp_minrtt_end = dict(grp_minrtt_end)
        self.grp_srtt_end = dict(grp_srtt_end)
        self.grp_stats_pkts = dict(grp_stats_pkts)

    @staticmethod
    def get(rundir, ngroups):
        picklefn = os.path.join(rundir, 'runstats.%dgrp.v%d.pickle' % (
                                        ngroups, RunStats.VERSION))
        if os.path.exists(picklefn):
            with open(picklefn, 'rb') as fd:
                runstats = pickle.load(fd)
                assert runstats.VERSION == RunStats.VERSION
                assert len(runstats.grp_segs_end) == ngroups or ngroups == 0
        else:
            logging.info('building %s', picklefn)
            runstats = RunStats.__build(rundir, ngroups)
            with open(picklefn, 'wb') as fd:
                pickle.dump(runstats, fd)
        return runstats

    @staticmethod
    def __build(rundir, ngroups):
        # grp_cwnd_ca = defaultdict(list)
        # grp_cwnd_1stloss = defaultdict(list)
        # grp_cwnd_end_large_noloss = defaultdict(list)
        grp_segs_1stloss = defaultdict(list)
        grp_segs_end = defaultdict(list)
        grp_retrans_end = defaultdict(list)
        grp_lossrate_end = defaultdict(list)
        grp_minrtt_end = defaultdict(list)
        grp_srtt_end = defaultdict(list)
        # grp_tputfix_end = defaultdict(list)

        grp_stats_pkts = defaultdict(dict)

        groups = [0] if ngroups == 0 else list(range(1, ngroups+1))
        for grp in groups:
            # grp_cwnd_ca[0]
            # grp_cwnd_1stloss[0]
            # grp_cwnd_end_large_noloss[0]
            grp_segs_1stloss[grp]
            grp_segs_end[grp]
            grp_retrans_end[grp]
            grp_lossrate_end[grp]
            grp_minrtt_end[grp]
            grp_srtt_end[grp]
            # grp_tputfix_end[grp]
            for pkts in [32, 43, 86]:
                grp_stats_pkts[grp][pkts] = {'cnt': 0,
                        'fstloss_after': 0,
                        'segs_end_noloss': list()}

        fd = gzip.open(os.path.join(rundir, 'events.dump.gz'), 'rt')
        for row in csv.DictReader(fd):
            dst = ipaddress.IPv4Address(row["dst"].split(':')[0])
            if dst not in RunStats.PREFIX:
                continue

            grp = dst2group(dst, ngroups) if ngroups != 0 else 0

            if connection_termination(row):
                segs = int(row['segs_out'])
                if segs <= 0:
                    continue
                retrans = int(row['total_retrans'])

                grp_retrans_end[grp].append(retrans)
                grp_segs_end[grp].append(segs)
                grp_lossrate_end[grp].append(retrans/segs)
                grp_minrtt_end[grp].append(int(row['min_rtt_us'])/1000000)
                grp_srtt_end[grp].append(int(row['srtt_us'])/1000000)
                # grp_tputfix_end[grp].append(None)

                for pkts, stats in sorted(grp_stats_pkts[grp].items()):
                    if segs - retrans < pkts:
                        break
                    stats['cnt'] += 1
                    if retrans == 0:
                        stats['segs_end_noloss'].append(segs)

            elif ca_state_degradation(row):
                # cwnd = int(row['prior_cwnd'])
                # grp_cwnd_ca[grp].append(cwnd)
                if row['total_retrans'] is None:
                    continue
                if int(row['total_retrans']) == 0:
                    # grp_cwnd_1stloss[grp].append(cwnd)
                    segs_now = int(row['segs_out']) - int(row['packets_out'])
                    grp_segs_1stloss[grp].append(segs_now)
                    for pkts, stats in sorted(grp_stats_pkts[grp].items()):
                        if segs_now < pkts:
                            break
                        stats['fstloss_after'] += 1

        fd.close()

        return RunStats(ngroups, # grp_cwnd_ca,
                                 # grp_cwnd_1stloss,
                                 # grp_cwnd_end_large_noloss,
                                 grp_segs_1stloss,
                                 grp_segs_end,
                                 grp_retrans_end,
                                 grp_lossrate_end,
                                 grp_minrtt_end,
                                 grp_srtt_end,
                                 grp_stats_pkts)


def get_start_end_timestamps(rundir):
    fd = open(os.path.join(rundir, 'utilization-gs.dump'), 'r')
    lines = fd.readlines()
    fd.close()
    first = int(float(lines[1].split(',')[0])) + 60  # skip header line
    last = int(float(lines[-1].split(',')[0])) - 60
    assert last - first > 120
    return first, last


def get_drop_rate(rundir):
    start_pkts, start_drops, end_pkts, end_drops = 0, 0, 0, 0
    start, end = get_start_end_timestamps(rundir)
    fd = open(os.path.join(rundir, 'queue-lengths.csv'))
    for row in csv.DictReader(fd):
        tstamp = float(row['tstamp'])
        if tstamp < start or tstamp > end:
            continue
        if start_pkts == 0:
            start_pkts = int(row['packets'])
            start_drops = int(row['dropped'])
        end_pkts = int(row['packets'])
        end_drops = int(row['dropped'])
    packets = end_pkts - start_pkts
    drops = end_drops - start_drops
    assert packets > 0 and drops >= 0 and packets > drops
    return drops/packets


def load_gs_queue_sizes(rundir):
    start, end = get_start_end_timestamps(rundir)
    queueszs = list()
    fd = open(os.path.join(rundir, 'queue-lengths.csv'))
    for row in csv.DictReader(fd):
        tstamp = float(row['tstamp'])
        if tstamp < start or tstamp > end:
            continue
        backlog = float(row['backlog_packets'])
        queueszs.append(backlog)
    fd.close()
    return queueszs


def load_gs_utilizations(rundir):
    start, end = get_start_end_timestamps(rundir)
    utils = list()
    fd = open(os.path.join(rundir, 'utilization-gs.dump'))
    for row in csv.DictReader(fd):
        tstamp = float(row['tstamp'])
        if tstamp < start or tstamp > end:
            continue
        utils.append(float(row['txutil']))
    fd.close()
    return utils


def load_gc_utilizations(path, ngroups):
    start, end = get_start_end_timestamps(path)
    grp2utils = defaultdict(list)
    fd = open(os.path.join(path, 'utilization-gc.dump'))
    for row in csv.DictReader(fd):
        tstamp = float(row['tstamp'])
        if tstamp < start or tstamp > end:
            continue
        iface = row['iface']
        grp = iface2group(iface, ngroups)
        grp2utils[grp].append(float(row['txutil']))
    fd.close()
    return grp2utils


class FlowDB:
    def __init__(self, fname):
        self.fd = open(fname)
        self.sd2endCsize = dict()
        self.tstamp = 0

    def get_fsize(self, src, dst, tstamp):
        # print(src, dst, tstamp)
        self.advance(tstamp)
        # print(self.tstamp)
        if self.tstamp == 1e20:
            if (src, dst) not in self.sd2endCsize:
                return 0
            end, size = self.sd2endCsize[(src, dst)]
            return 0 if end < tstamp else size
        else:
            end, size = self.sd2endCsize[(src, dst)]
            assert end >= tstamp, '%d %d' % (end, size)
            return size

    def advance(self, tstamp):
        if self.tstamp > tstamp:
            return
        while self.tstamp//10000 <= tstamp//10000:
            line = self.fd.readline()
            if not line:
                self.tstamp = 1e20
                return
            src, dst, start, end, fsize = line.strip().split(',')
            start = int(start)
            assert start - self.tstamp > -100000
            self.tstamp = start
            self.sd2endCsize[(src, dst)] = (int(end), int(fsize))


def plot_cdfs(key2data, title, xlabel, ylabel, outfn, xlim=None, ylim=None, cutpoint=None, label2style=None):
    if label2style is None:
        label2style = defaultdict(lambda: '-')
    fig, ax1 = plt.subplots()
    ax1.set_title(title, fontsize=12)
    ax1.set_xlabel(xlabel, fontsize=16)
    ax1.set_ylabel(ylabel, fontsize=16)
    if xlim is not None:
        ax1.set_xlim(xlim[0], xlim[1])
    ax1.set_ylim(0, 1)
    if ylim is not None:
        ax1.set_ylim(ylim[0], ylim[1])
    fig.tight_layout()
    for key, data in key2data.items():
        data.sort()
        cdf = getcdf(data)
        xs, ys = zip(*cdf)
        if cutpoint is not None:
            idx = bisect.bisect_left(xs, cutpoint)
            xs = xs[:idx]
            ys = ys[:idx]
        ax1.plot(xs, ys, label2style[key], markersize=6, label=key)
    plt.legend(loc='best')
    plt.grid()
    plt.savefig(outfn, bbox_inches='tight')
    plt.close(fig)


def getcdf(data):
    def get_tuple(item):
        if isinstance(item, tuple):
            return item
        if isinstance(item, list):
            return item
        if isinstance(item, float):
            return item, 1
        if isinstance(item, int):
            return item, 1
        raise RuntimeError('unknown item type')
    HEIGHT_STEP = 0.0001
    result = list()

    if not data:
        return [[0.0, 0.0], [0.0, 1.0]]

    last, cnt = get_tuple(data[0])
    for item in data[1:]:
        val, weight = get_tuple(item)
        if val != last:
            assert not result or last > result[-1][0], 'input not sorted'
            result.append((last, cnt))
            cnt += weight
        else:
            cnt += weight
        last = val

    if not result:
        result.append([0.0, 0.0])
    result.append([last, cnt])

    cdf = list()

    h = HEIGHT_STEP
    i = 0
    while i < len(result):
        x, y = result[i][0], float(result[i][1])/cnt
        while y < h:
            i += 1
            x, y = result[i][0], float(result[i][1])/cnt
        cdf.append((x, y))
        while y >= h:
            h += HEIGHT_STEP
        i += 1
    cdf.append((result[-1][0], 1.0))
    return cdf


def plot_f1(f1scores, thresholds, outfn):
    fig, ax1 = plt.subplots()
    # ax2 = ax1.twinx()

    ax1.set_xlabel('Number of Packets until First Loss', fontsize=16)
    ax1.set_ylabel('F1 score', fontsize=16)
    # ax1.set_ylabel('F1 score', fontsize=16, color='b')
    # ax2.set_ylabel('Threshold', fontsize=16, color='r')

    ax1.set_xlim(0, 128)
    ax1.set_ylim(0.5, 1)
    # ax2.set_ylim(0.5, 1)

    fig.tight_layout()

    xvalues = list(range(len(f1scores)))
    assert len(f1scores) == len(thresholds)

    ax1.plot(xvalues, f1scores, '-')
    # ax1.plot(xvalues, f1scores, 'b-', label='F1 Score')
    # ax2.plot(xvalues, thresholds, 'r.', label='Metric threshold')

    # plt.legend(loc='best')
    plt.grid()
    plt.savefig(outfn, bbox_inches='tight')
    plt.close(fig)

def plot_f2(f1scores, f2scores, outfn):
    fig, ax1 = plt.subplots()
    # ax2 = ax1.twinx()

    ax1.set_xlabel('Number of Packets until First Loss', fontsize=16)
    ax1.set_ylabel('F1 score', fontsize=16)
    # ax1.set_ylabel('F1 score', fontsize=16, color='b')
    # ax2.set_ylabel('Threshold', fontsize=16, color='r')

    ax1.set_xlim(0, 128)
    ax1.set_ylim(0.5, 1)
    # ax2.set_ylim(0.5, 1)

    fig.tight_layout()

    xvalues = list(range(len(f1scores)))
    assert len(f1scores) == len(f2scores)

    ax1.plot(xvalues, f1scores, '-', label='U < 95% vs U >= 95%')
    ax1.plot(xvalues, f2scores, '-', label='U < 95% vs U >= 97%')
    # ax1.plot(xvalues, f1scores, 'b-', label='F1 Score')
    # ax2.plot(xvalues, thresholds, 'r.', label='Metric threshold')

    plt.legend(loc='best')
    plt.grid()
    plt.savefig(outfn, bbox_inches='tight')
    plt.close(fig)


def plot_lines(label2qtiles, label2points, xlabel, outfn, y1labels, xlim=None):
    markers = ['+', 'x', '.', '*', '1', '2', '3', '4']
    miter = iter(markers)
    host = host_subplot(111, axes_class=AA.Axes)
    plt.subplots_adjust(right=0.75)

    labels = sorted(list(label2qtiles.keys()))
    extra_axes_labels = sorted(set(labels) - set(y1labels))
    extra_axes_objects = list()

    host.set_xlabel(xlabel)
    if xlim is not None:
        host.set_xlim(xlim[0], xlim[1])

    host.set_ylabel('\n'.join(lbl[1] for lbl in y1labels))
    host.set_ylim(0, 1)

    offset = 0
    for eal in extra_axes_labels:
        axobj = host.twinx()
        extra_axes_objects.append(axobj)
        new_fixed_axis = axobj.get_grid_helper().new_fixed_axis
        axobj.axis["right"] = new_fixed_axis(loc="right",
                                             axes=axobj,
                                             offset=(offset, 0))
        axobj.axis["right"].toggle(all=True)
        offset += 60
        axobj.set_ylabel(eal[1])

    for lbl, points in label2points.items():
        xs, ys = zip(*points)
        if lbl in y1labels:
            lineobj, = host.plot(xs, ys, label=lbl[0], marker=next(miter))
        else:
            idx = extra_axes_labels.index(lbl)
            axobj = extra_axes_objects[idx]
            lineobj, = axobj.plot(xs, ys, label=lbl[0], marker=next(miter))
            axobj.axis["right"].label.set_color(lineobj.get_color())

    for lbl, points in label2qtiles.items():
        xs, qtiles = zip(*points)
        q25s, q50s, q75s = zip(*qtiles)
        if lbl in y1labels:
            lineobj, = host.plot(xs, q50s, label=lbl[0], marker=next(miter))
            host.fill_between(xs, q25s, q75s,
                    alpha=0.25, facecolor='#333333')
        else:
            idx = extra_axes_labels.index(lbl)
            axobj = extra_axes_objects[idx]
            lineobj, = axobj.plot(xs, q50s, label=lbl[0], marker=next(miter))
            axobj.axis["right"].label.set_color(lineobj.get_color())
            axobj.fill_between(xs, q25s, q75s,
                    alpha=0.25, facecolor='#333333')


    host.legend(loc='best')
    fig = host.get_figure()
    fig.set_size_inches((10, 5))
    fig.savefig(outfn, bbox_inches='tight')
    fig.clear()
    host.clear()


    # fig, ax1 = plt.subplots()
    # ax1.set_xlabel(xlabel, fontsize=16)
    # ax1.set_ylabel(ylabel, fontsize=16)
    # if xlim is not None:
    #     ax1.set_xlim(xlim[0], xlim[1])
    # if ylim is not None:
    #     ax1.set_ylim(ylim[0], ylim[1])
    # fig.tight_layout()
    # for label, points in sorted(label2points.items()):
    #     xs, ys = zip(*points)
    #     ax1.plot(xs, ys, label=label, marker='+')
    # plt.legend(loc='best')
    # plt.grid()
    # plt.savefig(outfn, bbox_inches='tight')
    # plt.close(fig)


def plot_timeseries(key2data, title, xlabel, ylabel, outfn):
    fig, ax1 = plt.subplots()
    ax1.set_title(title, fontsize=12)
    ax1.set_xlabel(xlabel, fontsize=16)
    ax1.set_ylabel(ylabel, fontsize=16)
    # ax1.set_xlim(0, 1)
    # ax1.set_ylim(0, 1)
    fig.tight_layout()
    _mintstamp = min(min(pair[0] for pair in data)
                    for data in key2data.values())
    for key, data in sorted(key2data.items()):
        xraw, ys = zip(*data)
        # xs = list((r - mintstamp)/1000 for r in xraw)
        xs = xraw
        ax1.step(xs, ys, label=key, where='post')
    plt.legend(loc='best')
    plt.grid()
    plt.savefig(outfn, bbox_inches='tight')
    plt.close(fig)


# def plot_scatter_hist(points, xlabel, ylabel, outfn, xlim=None, ylim=None):
#     return
#     x = list(p[0] for p in points)
#     y = list(p[1] for p in points)

#     fig = plt.figure(figsize=(12,8))
#     gs = gridspec.GridSpec(3, 3)
#     ax_main = plt.subplot(gs[1:3, :2])
#     ax_xDist = plt.subplot(gs[0, :2], sharex=ax_main)
#     ax_yDist = plt.subplot(gs[1:3, 2], sharey=ax_main)

#     ax_main.scatter(x,y, marker='.')
#     ax_main.set_xlabel(xlabel, fontsize=16)
#     ax_main.set_ylabel(ylabel, fontsize=16)
#     ax_main.set_xlim(xlim[0], xlim[1])
#     ax_main.set_ylim(ylim[0], ylim[1])

#     ax_xDist.hist(x, bins=25, density=True, align='mid')
#     ax_xDist.tick_params(axis='x', bottom=False)
#     # ax_xDist.set(ylabel='Density')

#     # ax_xCumDist = ax_xDist.twinx()
#     # ax_xCumDist.hist(x,bins=100,cumulative=True,histtype='step',normed=True,color='r',align='mid')
#     # ax_xCumDist.tick_params('y', colors='r')
#     # ax_xCumDist.set_ylabel('cumulative',color='r')

#     ax_yDist.hist(y, bins=25, orientation='horizontal', density=True, align='mid')
#     ax_yDist.tick_params(axis='y', which='both', right=False, left=False, bottom=False)
#     # ax_yDist.set(xlabel='count')
#     # ax_yCumDist = ax_yDist.twiny()
#     # ax_yCumDist.hist(y,bins=100,cumulative=True,histtype='step',normed=True,color='r',align='mid',orientation='horizontal')
#     # ax_yCumDist.tick_params('x', colors='r')
#     # ax_yCumDist.set_xlabel('cumulative',color='r')

#     plt.savefig(outfn, bbox_inches='tight')
#     plt.close(fig)


def plot_scatter(points, xlabel, ylabel, outfn, xlim=None, ylim=None):
    fig, ax1 = plt.subplots()
    ax1.set_xlabel(xlabel, fontsize=16)
    ax1.set_ylabel(ylabel, fontsize=16)
    if xlim is not None:
        ax1.set_xlim(xlim[0], xlim[1])
    if ylim is not None:
        ax1.set_ylim(ylim[0], ylim[1])
    fig.tight_layout()
    x = list(p[0] for p in points)
    y = list(p[1] for p in points)
    ax1.scatter(x, y, s=1)
    plt.grid()
    plt.savefig(outfn, bbox_inches='tight')
    plt.close(fig)
