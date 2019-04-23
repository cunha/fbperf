package pfxlencsv

import (
	"encoding/json"
	"log"
	"net"
	"strings"

	"cdnperf"
)

type Row struct {
	Samples           uint32 `csv:"num_samples"`
	SamplesHD         uint32 `csv:"num_samples_with_trans_capable_2500kbps_throughput"`
	BytesAcked        uint64 `csv:"bytes_acked_sum"`
	MinRttPercentiles string `csv:"minrtt_ms_percentiles_p10_p25_p50_p75_p90"`
	BgpPrefix         string `csv:"bgp_ip_prefix"`
	AggPrefix         string `csv:"ipv4_prefix_slash24_or_ipv6_prefix_slash48"`
}

type RowSummary struct {
	Version    uint8
	Samples    uint32
	SamplesHD  uint32
	BytesAcked uint64
	Pct2MinRtt map[uint8]uint16
	PrefixLen  uint8 // Zero indicates AS aggregation
	PrefixAgg  bool  // Indicates whether this row is for a deaggregated prefix
}

func (r *Row) Summarize() *RowSummary {
	var minRtts []uint16
	json.Unmarshal([]byte(r.MinRttPercentiles), &minRtts)
	if len(minRtts) != 5 {
		log.Panicln("Error parsing 5 pctiles %s", r.MinRttPercentiles)
	}

	rs := &RowSummary{
		Version:    4,
		Samples:    r.Samples,
		SamplesHD:  r.SamplesHD,
		BytesAcked: r.BytesAcked,
		Pct2MinRtt: map[uint8]uint16{
			10: minRtts[0],
			25: minRtts[1],
			50: minRtts[2],
			75: minRtts[3],
			90: minRtts[4],
		},
	}

	if r.BgpPrefix == "NULL" && r.AggPrefix == "NULL" {
		// AS-level aggregation
		rs.PrefixLen = 0
		rs.PrefixAgg = true
	} else if r.BgpPrefix != "NULL" && r.AggPrefix == "NULL" {
		// No aggregation
		_, prefix, err := net.ParseCIDR(r.BgpPrefix)
		cdnperf.CheckError(err)
		if strings.Contains(r.BgpPrefix, ":") {
			rs.Version = 6
		}
		size, _ := prefix.Mask.Size()
		rs.PrefixLen = uint8(size)
		rs.PrefixAgg = false
	} else if r.BgpPrefix == "NULL" && r.AggPrefix != "NULL" {
		// Aggregation
		_, prefix, err := net.ParseCIDR(r.AggPrefix)
		cdnperf.CheckError(err)
		if strings.Contains(r.AggPrefix, ":") {
			rs.Version = 6
		}
		size, _ := prefix.Mask.Size()
		rs.PrefixLen = uint8(size)
		rs.PrefixAgg = true
	} else {
		log.Panicln("Error parsing row %s", r)
	}

	return rs
}
