import ipaddress
import json


def _str_to_bool(string):
    return string in ("true", "True", "OK", "ok", "Ok")


class RowParseError(RuntimeError):
    pass


class Row:
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

    def primary_route(self):
        return self.num2rtinfo.get(0, None)

    def best_alternate_route(self, metric="minrtt_ms_p50", aggfunc=min):
        validfunc = lambda x: x.apm_route_num > 1
        return self.best_route(metric, aggfunc, validfunc)

    def best_route(
        self, metric="minrtt_ms_p50", aggfunc=min, validfunc=lambda x: True
    ):
        best_rti = None
        best_metric = None
        for rti in self.num2rtinfo.values():
            if not validfunc(rti):
                continue
            rti_metric = getattr(rti, metric)
            if (
                best_metric is None
                or aggfunc(rti_metric, best_metric) != best_metric
            ):
                best_rti = rti
                best_metric = rti_metric
        return best_rti

    def get_primary_bestalt(self):
        primary = self.primary_route()
        bestalt = self.best_alternate_route()
        if bestalt is None:
            bestalt = primary
        return (primary, bestalt)

    @staticmethod
    def parse_routes(row):
        num2rtinfo = dict()
        for i in range(Row.MAX_ROUTE_NUM + 1):
            if row["r%d_num_samples" % i] == "NULL":
                continue
            num2rtinfo[i] = RouteInfo(i, row)
        return num2rtinfo


class RouteInfo:
    FIELDS = {
        int: [
            "num_samples",
            "apm_route_num",
            "bgp_as_path_len",
            "bgp_as_path_min_len_prepending_removed",
            "minrtt_ms_p10",
            "minrtt_ms_p25",
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
                setattr(self, fname, fparser(row[findex(fname)]))
