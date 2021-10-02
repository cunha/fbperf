#!/usr/bin/env python3

import sys
import argparse
from dataclasses import dataclass
import resource
import logging

import ipaddress
from typing import Any


@dataclass
class PathId:
    metro: str
    prefix: Any[ipaddress.IPv4Address, ipaddress.IPv6Address]
    continent: str
    country: str


@dataclass
class GraphSpec:
    pathid: PathId
    route1: int
    route2: int


def create_parser():  # {{{
    desc = """Extract a set of (PoP, prefix) pairs from FB data"""
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument(
        "--path-list",
        dest="path_list_fn",
        action="store",
        metavar="FILE",
        type=str,
        required=True,
        help="File containing one path spec per line",
    )
    parser.add_argument(
        "--basedir",
        dest="basedir",
        metavar="DIR",
        type=str,
        help="Directory with one .tsv.gz file per metro",
    )
    parser.add_argument(
        "--outdir",
        dest="outdir",
        metavar="DIR",
        type=str,
        help="Output directory (will create one file per path)",
    )

    parser.add_argument(
        "--pfxa-list",
        dest="pfxafn",
        metavar="FILE",
        type=str,
        help="File with one PrefixAnnounce per line [%(default)s]",
        default="/mnt/data/cunha/sb.routing/dbs/prejudiced.pfxa.txt",
    )

    parser.add_argument(
        "--ip2asdb",
        dest="ip2asfn",
        metavar="FILE",
        type=str,
        help="File with IP2AS database [%(default)s]",
        default="/mnt/data/cunha/sb.routing/dbs/origin_as_mapping.txt",
    )

    parser.add_argument(
        "--out",
        dest="fn",
        metavar="FILE",
        type=str,
        required=True,
        help="Output filename",
    )

    return parser


# }}}


def main():  # {{{
    resource.setrlimit(resource.RLIMIT_AS, (1 << 30, 1 << 30))
    resource.setrlimit(resource.RLIMIT_FSIZE, (1 << 35, 1 << 35))
    logging.basicConfig(filename="log.txt", format="%(message)s", level=logging.NOTSET)

    parser = create_parser()
    opts = parser.parse_args()


# }}}


if __name__ == "__main__":
    sys.exit(main())
