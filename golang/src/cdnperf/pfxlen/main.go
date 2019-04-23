package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"

	"cdnperf"
	"cdnperf/csvhelp"
	"cdnperf/csvhelp/pfxlencsv"
)

const (
	PrefixClassOrig4   = "BGP4"
	PrefixClassOrig6   = "BGP6"
	PrefixClassASN4    = "ASN4"
	PrefixClassASN6    = "ASN6"
	PrefixClassSlash24 = "S24"
	PrefixClassSlash48 = "S48"
)

type Stats struct {
	Chan       chan *pfxlencsv.RowSummary
	BytesAcked uint64
	Desc       string
	outdir     string
	summaries  []*pfxlencsv.RowSummary
	waitGroup  sync.WaitGroup
}

func NewStats(desc string, outdir string) *Stats {
	stats := &Stats{
		Chan:       make(chan *pfxlencsv.RowSummary, 4096),
		BytesAcked: 0,
		Desc:       desc,
		outdir:     outdir,
	}
	stats.waitGroup.Add(1)
	go stats.Run()
	return stats
}

func (s *Stats) Run() {
	defer s.waitGroup.Done()

	rowCount := 0
	for {
		rs := <-s.Chan
		if rs == nil {
			break
		}
		rowCount += 1
		s.BytesAcked += rs.BytesAcked
		s.summaries = append(s.summaries, rs)
	}
	log.Printf("Stats(%s) done, %d rows processed", s.Desc, rowCount)
}

func (s *Stats) Join() {
	s.Chan <- nil
	s.waitGroup.Wait()
}

func (s *Stats) SpreadQuantiles(lower, upper uint8) [3]uint16 {
	values := make([]float64, len(s.summaries))
	for i, rs := range s.summaries {
		values[i] = float64(rs.Pct2MinRtt[upper] - rs.Pct2MinRtt[lower])
	}
	sort.Slice(values, func(i, j int) bool {
		return values[i] < values[j]
	})
	Qs := cdnperf.GetQuartiles(values)
	return [3]uint16{uint16(Qs[0]), uint16(Qs[1]), uint16(Qs[2])}
}

func (s *Stats) GetCDF(extractData func(rs *pfxlencsv.RowSummary) float64) ([]float64, []float64) {
	sorted := make([]float64, len(s.summaries))
	for i, rs := range s.summaries {
		sorted[i] = extractData(rs)
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})
	cdfx, cdfy := cdnperf.BuildCDF(sorted, nil)
	return cdfx, cdfy
}

func (s *Stats) Dump(spreads []([2]uint8), wg *sync.WaitGroup) {
	defer wg.Done()
	for _, spread := range spreads {
		lo, up := spread[0], spread[1]
		cdfx, cdfy := s.GetCDF(func(rs *pfxlencsv.RowSummary) float64 {
			return float64(rs.Pct2MinRtt[up] - rs.Pct2MinRtt[lo])
		})
		fname := fmt.Sprintf("pfxlen%s_%dspread%d.cdf", s.Desc, lo, up)
		fpath := filepath.Join(s.outdir, fname)
		cdnperf.DumpCDF(fpath, cdfx, cdfy)
	}
}

func dumpPrefixToSpreadQuantiles(fpath string,
	pfxlen2spreadQs map[uint8][3]uint16) {
	fd, err := os.Create(fpath)
	cdnperf.CheckError(err)
	defer fd.Close()
	for pfxlen, q := range pfxlen2spreadQs {
		str := fmt.Sprintf("%d %d %d %d\n", pfxlen, q[0], q[1], q[2])
		fd.WriteString(str)
	}
}

func main() {
	input := os.Args[1]
	outdir := os.Args[2]
	minsamples, err := strconv.Atoi(os.Args[3])
	cdnperf.CheckError(err)
	SPREADS := [][2]uint8{[2]uint8{10, 90}, [2]uint8{25, 75}, [2]uint8{10, 75}}
	PFXLENGTHS := map[uint8]map[uint8]bool{
		4: map[uint8]bool{
			0:  true,
			16: true,
			18: true,
			20: true,
			22: true,
			24: true},
		6: map[uint8]bool{
			32: true,
			40: true,
			48: true,
			56: true,
			64: true},
	}

	pfxlen2stats := make(map[uint8]*Stats)
	class2stats := make(map[string]*Stats)
	globalTraffic := uint64(0)
	droppedTraffic := uint64(0)

	csvhelp.ParseFile(input, func(row *pfxlencsv.Row) {
		rs := row.Summarize()
		if !rs.PrefixAgg {
			globalTraffic += rs.BytesAcked
			if rs.Samples < uint32(minsamples) {
				droppedTraffic += rs.BytesAcked
			}
		}
		if rs.Samples < uint32(minsamples) {
			return
		}
		var class string
		if rs.PrefixAgg {
			if rs.Version == 4 {
				class = PrefixClassSlash24
				if rs.PrefixLen == 0 {
					class = PrefixClassASN4
				}
			} else {
				class = PrefixClassSlash48
				if rs.PrefixLen == 0 {
					class = PrefixClassASN6
				}
			}
		} else {
			class = PrefixClassOrig6
			if rs.Version == 4 {
				class = PrefixClassOrig4
			}
			_, ok := PFXLENGTHS[rs.Version][rs.PrefixLen]
			if ok {
				stats, ok := pfxlen2stats[rs.PrefixLen]
				if !ok {
					log.Printf("Creating Stats(%d)\n", rs.PrefixLen)
					stats = NewStats(strconv.Itoa(int(rs.PrefixLen)), outdir)
					pfxlen2stats[rs.PrefixLen] = stats
				}
				stats.Chan <- rs
			}
		}
		stats, ok := class2stats[class]
		if !ok {
			log.Printf("Creating Stats(%s)\n", class)
			stats = NewStats(class, outdir)
			class2stats[class] = stats
		}
		stats.Chan <- rs
	})

	log.Printf("globalTraffic = %d\n", globalTraffic)
	log.Printf("droppedTraffic = %d\n", droppedTraffic)

	fpath := filepath.Join(outdir, "traffic_ratios.txt")
	fd, err := os.Create(fpath)
	cdnperf.CheckError(err)
	defer fd.Close()

	var wg sync.WaitGroup
	for _, stats := range pfxlen2stats {
		stats.Join()
		ratio := float64(stats.BytesAcked) / float64(globalTraffic)
		str := fmt.Sprintf("%s %d %f\n", stats.Desc, stats.BytesAcked, ratio)
		fd.WriteString(str)
		wg.Add(1)
		go stats.Dump(SPREADS, &wg)
	}
	for _, stats := range class2stats {
		stats.Join()
		ratio := float64(stats.BytesAcked) / float64(globalTraffic)
		str := fmt.Sprintf("%s %d %f\n", stats.Desc, stats.BytesAcked, ratio)
		fd.WriteString(str)
		wg.Add(1)
		go stats.Dump(SPREADS, &wg)
	}
	wg.Wait()

	for _, s := range SPREADS {
		pfxlen2spreadQs := make(map[uint8][3]uint16)
		for pfxlen, stats := range pfxlen2stats {
			pfxlen2spreadQs[pfxlen] = stats.SpreadQuantiles(s[0], s[1])
		}
		fn := fmt.Sprintf("pfxlen_%dspread%d_qtiles.txt", s[0], s[1])
		fp := filepath.Join(outdir, fn)
		dumpPrefixToSpreadQuantiles(fp, pfxlen2spreadQs)
	}
}
