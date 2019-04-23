package cicsv

import (
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"os"

	"cdnperf"
	"github.com/gocarina/gocsv"
)

type Row struct {
	Time                  uint32 `csv:"time_bucket"`
	TimeBucketSize        uint32 `csv:"time_bucket_size_secs"`
	ApmRoute              uint32 `csv:"apm_route_num"`
	Prefix                string `csv:"bgp_ip_prefix"`
	OrigSamples           uint32 `csv:"num_samples_in_orig_group"`
	OrigBytes             uint64 `csv:"bytes_acked_sum_in_orig_group"`
	LimitSamples          uint32 `csv:"limit_samples_in_subsample_group"`
	SubSamples            uint32 `csv:"num_samples_in_subsample_group"`
	SubSamplesBytesAcked  uint64 `csv:"bytes_acked_sum_in_subsample_group"`
	MinRttPercentiles     string `csv:"minrtt_ms_percentiles"`
	MinRttP10ConfInterval string `csv:"minrtt_ms_p10_with_confidence_intervals"`
	MinRttP50ConfInterval string `csv:"minrtt_ms_p50_with_confidence_intervals"`
}

type RowSummary struct {
	Samples         uint32
	BytesAcked      uint64
	MinRttP10       int16
	MinRttP10CiSize int16
	MinRttP50       int16
	MinRttP50CiSize int16
}

func ParseFile(fpath string, rowfunc func(r *Row)) {
	// This probably sets some gocsv-wide configuration variable,
	// and thus is not thread-safe.
	gocsv.SetCSVReader(func(in io.Reader) gocsv.CSVReader {
		r := csv.NewReader(in)
		r.Comma = '\t'
		return r
	})
	gzfd, err := os.Open(fpath)
	cdnperf.CheckError(err)
	defer gzfd.Close()
	csvfd, err := gzip.NewReader(gzfd)
	cdnperf.CheckError(err)
	err = gocsv.UnmarshalToCallback(csvfd, rowfunc)
	cdnperf.CheckError(err)
}

func (r *Row) Summarize() *RowSummary {
	var minRttP10 []int16
	json.Unmarshal([]byte(r.MinRttP10ConfInterval), &minRttP10)
	var minRttP50 []int16
	json.Unmarshal([]byte(r.MinRttP50ConfInterval), &minRttP50)
	if len(minRttP10) != 3 || len(minRttP50) != 3 {
		log.Panicln("Error parsing row %s", r)
	}
	if r.LimitSamples == 0 && r.OrigSamples != r.SubSamples {
		log.Panicln("Inconsistent row %s", r)
	}
	return &RowSummary{
		Samples:         r.SubSamples,
		BytesAcked:      r.OrigBytes,
		MinRttP10:       minRttP10[1],
		MinRttP10CiSize: minRttP10[2] - minRttP10[0],
		MinRttP50:       minRttP50[1],
		MinRttP50CiSize: minRttP50[2] - minRttP50[0],
	}
}
