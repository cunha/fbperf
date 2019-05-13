import ipaddress
import json
import math


def _str_to_bool(string):
    return string in ("true", "True", "OK", "ok", "Ok")


class RowParseError(RuntimeError):
    pass


class Row:
    # pylint: disable=E1101

    FIELDS = {
        int: ["bytes_acked_sum", "time_bucket"],
        _str_to_bool: ["client_is_ipv6"],
        ipaddress.ip_network: ["bgp_ip_prefix"],
        str: ["vip_metro"],
    }
    MAX_ROUTE_NUM = 7

    def __init__(self, csvrow):
        for fparser, flist in Row.FIELDS.items():
            for fname in flist:
                if csvrow[fname] == "NULL":
                    raise RowParseError
                setattr(self, fname, fparser(csvrow[fname]))
        self.num2rtinfo = Row.parse_routes(csvrow)

    def key(self):
        return (self.vip_metro, self.bgp_ip_prefix)

    def primary_route(self, vf):
        rtinfo = self.num2rtinfo.get(0, None)
        if rtinfo is None or not vf(rtinfo):
            return None
        return rtinfo

    def best_route(self, metric="minrtt_ms_p50", aggfunc=min, validfunc=lambda x: True):
        best_rti = None
        best_metric = None
        for rti in self.num2rtinfo.values():
            if not validfunc(rti):
                continue
            rti_metric = getattr(rti, metric)
            if best_metric is None or aggfunc(rti_metric, best_metric) != best_metric:
                best_rti = rti
                best_metric = rti_metric
        return best_rti

    def get_primary_bestalt_minrtt(self, samples):
        vf = lambda x: x.num_samples >= samples
        primary = self.primary_route(vf)
        vf = lambda x: x.apm_route_num > 1 and x.num_samples > samples
        bestalt = self.best_route("minrtt_ms_p50", min, vf)
        if bestalt is None:
            bestalt = primary
        return (primary, bestalt)

    def get_primary_bestalt_hdratio(self, samples):
        vf = lambda x: x.hdratio_num_samples >= samples and x.hdratio is not None
        primary = self.primary_route(vf)
        vf = lambda x: x.apm_route_num > 1 and x.hdratio_num_samples >= samples and x.hdratio is not None
        bestalt = self.best_route("hdratio", max, vf)
        if bestalt is None:
            bestalt = primary
        return (primary, bestalt)

    @staticmethod
    def parse_routes(row):
        num2rtinfo = dict()
        for i in range(Row.MAX_ROUTE_NUM + 1):
            if row["r%d_num_samples" % i] == "NULL":
                continue
            # if row["r%d_hdratio" % i] == "NULL":
            #     continue
            num2rtinfo[i] = RouteInfo(i, row)
        return num2rtinfo


class RouteInfo:
    # pylint: disable=E1101

    FIELDS = {
        int: [
            "num_samples",
            "apm_route_num",
            "bgp_as_path_len",
            "bgp_as_path_min_len_prepending_removed",
            "minrtt_ms_p10",
            "minrtt_ms_p50",
            "minrtt_ms_p50_ci_lb",
            "minrtt_ms_p50_ci_ub",
            "hdratio_num_samples",
        ],
        float: ["minrtt_ms_p50_var", "hdratio", "hdratio_var"],
        bool: ["bgp_as_path_prepending"],
        str: ["peer_type", "peer_subtype"],
        json.loads: ["px_nexthops"],
    }

    def __init__(self, route_number, row):
        self.csv_rt_num = int(route_number)
        findex = lambda fname: "r%d_%s" % (self.csv_rt_num, fname)
        for fparser, flist in RouteInfo.FIELDS.items():
            for fname in flist:
                if row[findex(fname)] == "NULL":
                    setattr(self, fname, None)
                else:
                    setattr(self, fname, fparser(row[findex(fname)]))


def rtt_median_diff_ci(pri: RouteInfo, alt: RouteInfo, z=2) -> (int, int, int):
    med1 = pri.minrtt_ms_p50
    med2 = alt.minrtt_ms_p50
    var1 = pri.minrtt_ms_p50_var
    var2 = alt.minrtt_ms_p50_var
    md = med1 - med2
    interval = z * math.sqrt(var1 + var2)
    return (md - interval, md, md + interval)


def hdr_mean_diff_ci(pri: RouteInfo, alt: RouteInfo, z=2) -> (int, int, int):
    avg1 = pri.hdratio
    avg2 = alt.hdratio
    var1 = pri.hdratio_var
    var2 = alt.hdratio_var
    n1 = pri.hdratio_num_samples
    n2 = alt.hdratio_num_samples
    diff = avg2 - avg1
    interval = z * math.sqrt(var1 / n1 + var2 / n2)
    return (diff - interval, diff, diff + interval)
