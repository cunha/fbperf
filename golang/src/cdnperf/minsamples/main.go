package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"cdnperf"
	"cdnperf/csvhelp"
	"cdnperf/csvhelp/cicsv"
)

type Stats struct {
	Chan      chan *cicsv.Row
	samples   uint32
	outdir    string
	summaries []*cicsv.RowSummary
	waitGroup sync.WaitGroup
}

func NewStats(samples uint32, outdir string) *Stats {
	stats := &Stats{
		Chan:    make(chan *cicsv.Row, 4096),
		samples: samples,
		outdir:  outdir,
	}
	stats.waitGroup.Add(1)
	go stats.Run()
	return stats
}

func (s *Stats) Run() {
	defer s.waitGroup.Done()

	fname := fmt.Sprintf("ci_rows_%dsamples.txt", s.samples)
	fpath := filepath.Join(s.outdir, fname)
	fd, err := os.Create(fpath)
	cdnperf.CheckError(err)
	defer fd.Close()

	rowCount := 0
	for {
		row := <-s.Chan
		if row == nil {
			break
		}
		rowCount += 1
		rs := row.Summarize()
		s.summaries = append(s.summaries, rs)
		str := fmt.Sprintf("%d %d %d %d %d %d\n",
			rs.Samples, rs.BytesAcked,
			rs.MinRttP10, rs.MinRttP10CiSize,
			rs.MinRttP50, rs.MinRttP50CiSize)
		fd.WriteString(str)
	}
	log.Printf("Stats(%d) done, %d rows processed", s.samples, rowCount)
}

func (s *Stats) Join() {
	s.Chan <- nil
	s.waitGroup.Wait()
}

func (s *Stats) GetCDF(extractData func(*cicsv.RowSummary) float64) ([]float64, []float64) {
	sortedCiSizes := make([]float64, len(s.summaries))
	for i, rs := range s.summaries {
		sortedCiSizes[i] = extractData(rs)
	}
	sort.Slice(sortedCiSizes, func(i, j int) bool {
		return sortedCiSizes[i] < sortedCiSizes[j]
	})
	cdfx, cdfy := cdnperf.BuildCDF(sortedCiSizes, nil)
	return cdfx, cdfy
}

func (s *Stats) Dump(wg *sync.WaitGroup) {
	defer wg.Done()

	cdfx, cdfy := s.GetCDF(func(rs *cicsv.RowSummary) float64 {
		return float64(rs.MinRttP10CiSize)
	})
	fname := fmt.Sprintf("ci_size_%dsamples_rtt10.cdf", s.samples)
	fpath := filepath.Join(s.outdir, fname)
	cdnperf.DumpCDF(fpath, cdfx, cdfy)

	cdfx, cdfy = s.GetCDF(func(rs *cicsv.RowSummary) float64 {
		return float64(rs.MinRttP50CiSize)
	})
	fname = fmt.Sprintf("ci_size_%dsamples_rtt50.cdf", s.samples)
	fpath = filepath.Join(s.outdir, fname)
	cdnperf.DumpCDF(fpath, cdfx, cdfy)

	cdfx, cdfy = s.GetCDF(func(rs *cicsv.RowSummary) float64 {
		if rs.MinRttP10 == 0 {
			return 0.0
		}
		return float64(rs.MinRttP10CiSize) / float64(rs.MinRttP10)
	})
	fname = fmt.Sprintf("ci_relsize_%dsamples_rtt10.cdf", s.samples)
	fpath = filepath.Join(s.outdir, fname)
	cdnperf.DumpCDF(fpath, cdfx, cdfy)

	cdfx, cdfy = s.GetCDF(func(rs *cicsv.RowSummary) float64 {
		if rs.MinRttP50 == 0 {
			return 0.0
		}
		return float64(rs.MinRttP50CiSize) / float64(rs.MinRttP50)
	})
	fname = fmt.Sprintf("ci_relsize_%dsamples_rtt50.cdf", s.samples)
	fpath = filepath.Join(s.outdir, fname)
	cdnperf.DumpCDF(fpath, cdfx, cdfy)
}

func main() {
	input := os.Args[1]
	outdir := os.Args[2]

	samples2stats := make(map[uint32]*Stats)

	csvhelp.ParseFile(input, func(row *cicsv.Row) {
		stats, ok := samples2stats[row.LimitSamples]
		if !ok {
			log.Printf("Creating Stats(%d)\n", row.LimitSamples)
			stats = NewStats(row.LimitSamples, outdir)
			samples2stats[row.LimitSamples] = stats
		}
		stats.Chan <- row
	})

	var wg sync.WaitGroup
	for _, stats := range samples2stats {
		stats.Join()
		wg.Add(1)
		go stats.Dump(&wg)
	}
	wg.Wait()
}
