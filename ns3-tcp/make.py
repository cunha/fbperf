#!/usr/bin/env python3

import argparse
from collections import Counter, defaultdict
import logging
import sys
import resource

# PyPy library: pip3 install parse
import parse


def create_parser():
    desc = """Generate report and graphs from simulation results"""

    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument(
        "--input",
        dest="input",
        metavar="FILE",
        type=argparse.FileType("r"),
        required=True,
        help="File containing a DB with Snapshots (can specify " "multiple times)",
    )

    return parser


def table_string(bwdiff2cnt):
    lines = list()
    for bwdiff, cnt in sorted(bwdiff2cnt.items()):
        if bwdiff == 0:
            continue
        frac = cnt["correct"] / cnt["total"]
        lines.append("{:d} {:f} {:d}".format(bwdiff, frac, cnt["total"]))
    return "\n".join(lines)


def check_tput(record, string):
    if record["bwdiff"] < 0:
        return 1 if record[string] == 0 else 0
    if record["bwdiff"] > 0:
        return 1 if record[string] == 1 else 0
    # Should not get here, but if we do, any result is OK when bwdiff is 0:
    return 1


def add_bwdiff(key2bwdiff2cnt, record, keyhash, cnt):
    key = tuple(sorted(keyhash.items()))
    key2bwdiff2cnt[key][record["bwdiff"]] += cnt


def add_cfg(key2cfg2cnt, keyhash, cfgtuple, cnt):
    key = tuple(sorted(keyhash.items()))
    key2cfg2cnt[key][cfgtuple] += cnt


def main():
    resource.setrlimit(resource.RLIMIT_AS, (1 << 30, 1 << 30))
    resource.setrlimit(resource.RLIMIT_FSIZE, (1 << 35, 1 << 35))
    logging.basicConfig(filename="log.txt", format="%(message)s", level=logging.NOTSET)

    parser = create_parser()
    opts = parser.parse_args()

    key2bwdiff2cnt = defaultdict(
        lambda: defaultdict(lambda: Counter(correct=0, total=0))
    )
    key2cfg2cnt = defaultdict(lambda: defaultdict(lambda: Counter(correct=0, total=0)))

    # bwDiffKbps minRttMs initCwndPkts xferPkts ackDelayMs isValid tput1Ok tput2Ok
    fmt = "{target:d} {bwdiff:d} {rttms:d} {initcwnd:d} {xferpkts:d} {ackdelay:d} {ssthresh:d} {isvalid:b} {tput1:b} {tput2:b} {tputVanilla:b} {tputGeneric:b} {tputError:g} {tputGenericValid:b} {genericVsVanillaError:g}"
    for line in opts.input:
        record = parse.parse(fmt, line)
        if record is None:
            print(line)
            sys.exit(1)
        if record["bwdiff"] == 0:
            continue
        if record["target"] != 2500:
            # cleaning up output
            continue

        cnt1 = Counter(correct=check_tput(record, "tput1"), total=1)
        cnt2 = Counter(correct=check_tput(record, "tput2"), total=1)
        cnt3 = Counter(correct=check_tput(record, "tputVanilla"), total=1)
        cnt4 = Counter(correct=check_tput(record, "tputGeneric"), total=1)

        cfgdict = {
            "rttms": record["rttms"],
            "initcwnd": record["initcwnd"],
            "xferpkts": record["xferpkts"],
        }
        cfgtuple = tuple(sorted(cfgdict.items()))

        key1dict = {
            "technique": "correct-for-time-in-slow-start",
            "targetKbps": record["target"],
            "ackdelay": record["ackdelay"],
        }
        add_bwdiff(key2bwdiff2cnt, record, key1dict, cnt1)
        add_cfg(key2cfg2cnt, key1dict, cfgtuple, cnt1)

        key2dict = {
            "technique": "remove-time-in-slow-start",
            "targetKbps": record["target"],
            "ackdelay": record["ackdelay"],
        }
        add_bwdiff(key2bwdiff2cnt, record, key2dict, cnt2)
        add_cfg(key2cfg2cnt, key2dict, cfgtuple, cnt2)

        key3dict = {
            "technique": "vanilla",
            "targetKbps": record["target"],
            "ackdelay": record["ackdelay"],
        }
        add_bwdiff(key2bwdiff2cnt, record, key3dict, cnt3)
        add_cfg(key2cfg2cnt, key3dict, cfgtuple, cnt3)

        key4dict = {
            "technique": "generic",
            "targetKbps": record["target"],
            "ackdelay": record["ackdelay"],
        }
        add_bwdiff(key2bwdiff2cnt, record, key4dict, cnt4)
        add_cfg(key2cfg2cnt, key4dict, cfgtuple, cnt4)

        if record["isvalid"] == 1:
            key1dict["isvalid"] = 1
            add_bwdiff(key2bwdiff2cnt, record, key1dict, cnt1)
            add_cfg(key2cfg2cnt, key1dict, cfgtuple, cnt1)
            key2dict["isvalid"] = 1
            add_bwdiff(key2bwdiff2cnt, record, key2dict, cnt2)
            add_cfg(key2cfg2cnt, key2dict, cfgtuple, cnt2)
            key3dict["isvalid"] = 1
            add_bwdiff(key2bwdiff2cnt, record, key3dict, cnt3)
            add_cfg(key2cfg2cnt, key3dict, cfgtuple, cnt3)
            key4dict["isvalid"] = 1
            add_bwdiff(key2bwdiff2cnt, record, key4dict, cnt4)
            add_cfg(key2cfg2cnt, key4dict, cfgtuple, cnt4)

    for key, bwdiff2cnt in key2bwdiff2cnt.items():
        sys.stdout.write("%% KEY %s\n" % repr(key))
        sys.stdout.write("%s\n" % table_string(bwdiff2cnt))
        sys.stdout.write("%s\n" % print_worst_configs(key2cfg2cnt[key], 10))


def print_worst_configs(cfg2cnt, num):
    lines = list()
    lst = sorted(list((cnt["correct"], cfg) for cfg, cnt in cfg2cnt.items()))
    for i in range(0, min(len(lst), num)):
        string = "{:s}={:d} {:s}={:d} {:s}={:d} correct={:d}".format(
            lst[i][1][0][0],
            lst[i][1][0][1],
            lst[i][1][1][0],
            lst[i][1][1][1],
            lst[i][1][2][0],
            lst[i][1][2][1],
            lst[i][0],
        )
        lines.append(string)
    return "\n".join(lines)


if __name__ == "__main__":
    sys.exit(main())
