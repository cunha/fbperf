package minsamples

import (
	"encoding/json"
	"log"
)

type Row struct {
	Time                  uint32 `csv:"time_bucket"`
	TimeBucketSize        uint32 `csv:"time_bucket_size_secs"`
	ApmRoute              uint32 `csv:"apm_route_num"`
	Prefix                string `csv:"bgp_ip_prefix"`
	OrigSamples           uint32 `csv:"num_samples_in_orig_group"`
	OrigBytes             uint32 `csv:"bytes_acked_sum_in_orig_group"`
	LimitSamples          uint32 `csv:"limit_samples_in_subsample_group"`
	SubSamples            uint32 `csv:"num_samples_in_subsample_group"`
	SubSamplesBytesAcked  uint32 `csv:"bytes_acked_sum_in_subsample_group"`
	MinRttPercentiles     string `csv:"minrtt_ms_percentiles"`
	MinRttP10ConfInterval string `csv:"minrtt_ms_p10_with_confidence_intervals"`
	MinRttP50ConfInterval string `csv:"minrtt_ms_p50_with_confidence_intervals"`
}

type RowSummary struct {
	Samples        uint32
	MinRttP10      uint32
	MinRttP10Lower uint32
	MinRttP10Upper uint32
	MinRttP50      uint32
	MinRttP50Lower uint32
	MinRttP50Upper uint32
}

func (r *Row) Parse() *RowSummary {
	var minRttP10 []uint32
	json.Unmarshal([]byte(r.MinRttP10ConfInterval), &minRttP10)
	var minRttP50 []uint32
	json.Unmarshal([]byte(r.MinRttP50ConfInterval), &minRttP50)
	if len(minRttP10) != 3 || len(minRttP50) != 3 {
		log.Panicln("Error parsing row %s", r)
	}
	if r.LimitSamples == 0 && r.OrigSamples != r.SubSamples {
		log.Panicln("Inconsistent row %s", r)
	}
	return &RowSummary{
		Samples:        r.SubSamples,
		MinRttP10:      minRttP10[1],
		MinRttP10Lower: minRttP10[0],
		MinRttP10Upper: minRttP10[2],
		MinRttP50:      minRttP50[1],
		MinRttP50Lower: minRttP50[0],
		MinRttP50Upper: minRttP50[2],
	}
}
