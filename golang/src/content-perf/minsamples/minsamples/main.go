package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"

	"content-perf/minsamples"
	"github.com/gocarina/gocsv"
)

type Stats struct {
	Chan      chan *minsamples.Row
	samples   uint32
	summaries []*minsamples.RowSummary
	waitGroup sync.WaitGroup
	fd        *os.File
}

func NewStats(samples uint32, outdir string) *Stats {
	stats := &Stats{
		Chan:    make(chan *minsamples.Row, 4096),
		samples: samples,
	}
	fname := fmt.Sprintf("ci_stats_%dsamples.txt", samples)
	fpath := filepath.Join(outdir, fname)
	fd, err := os.Create(fpath)
	minsamples.CheckError(err)
	stats.fd = fd
	stats.waitGroup.Add(1)
	go stats.Run()
	return stats
}

func (s *Stats) Run() {
	defer s.waitGroup.Done()
	defer s.fd.Close()
	rows := 0
	for {
		row := <-s.Chan
		if row == nil {
			break
		}
		rows += 1
		rs := row.Parse()
		minRttP10Diff := rs.MinRttP10Upper - rs.MinRttP10Lower
		minRttP50Diff := rs.MinRttP50Upper - rs.MinRttP50Lower
		str := fmt.Sprintf("%d %d %d %d\n",
			rs.MinRttP10, minRttP10Diff,
			rs.MinRttP50, minRttP50Diff)
		s.fd.WriteString(str)
	}
	log.Printf("Stats(%d) done after processing %d rows", s.samples, rows)
}

func (s *Stats) Join() {
	s.Chan <- nil
	s.waitGroup.Wait()
}

func main() {
	outdir := os.Args[2]
	csvfd, err := os.Open(os.Args[1])
	minsamples.CheckError(err)
	defer csvfd.Close()

	samples2stats := make(map[uint32]*Stats)

	gocsv.SetCSVReader(func(in io.Reader) gocsv.CSVReader {
		r := csv.NewReader(in)
		r.Comma = '\t'
		return r
	})
	err = gocsv.UnmarshalToCallback(csvfd, func(row *minsamples.Row) {
		stats, ok := samples2stats[row.LimitSamples]
		if !ok {
			log.Printf("creating Stats(%d)\n", row.LimitSamples)
			stats = NewStats(row.LimitSamples, outdir)
			samples2stats[row.LimitSamples] = stats
		}
		stats.Chan <- row
	})
	minsamples.CheckError(err)

	for _, stats := range samples2stats {
		stats.Join()
	}
}
