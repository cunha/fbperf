package cdnperf

import (
	"fmt"
	"log"
	"math"
	"os"
)

// func GetConfidenceIntervalPercentiles(pctile uint32, samples uint32) (uint32, uint32) {
// 	z := 1.959964
// 	pp := float64(pctile) / 100.0
// 	diff := 100 * z * math.Sqrt(pp*(1-pp)/float64(samples))
// 	lower := uint32(math.Floor(math.Max(float64(pctile)-diff, 0)))
// 	upper := uint32(math.Ceil(math.Min(float64(pctile)+diff, 100)))
// 	return lower, upper
// }

// func WriteCDF(filename string, data []*RowSummary, conv func(*RowSummary) float64) {
// x := make([]float64, len(data))
// for i, rs := range data {
// 	x[i] = conv(rs)
// }
// sort.Sort(x)
// fd.Writeln("0.0 0.0")
// cum := 0.0
// for i, div := range dividers {
// 	cum += counts[i] / total
// 	fd.Writeln("%f %f", div, cum)
// }
// }

func BuildCDF(data, weights []float64) ([]float64, []float64) {
	if len(data) == 0 {
		return []float64{0.0, 0.0}, []float64{0.0, 1.0}
	}
	if len(data) == 1 {
		return []float64{0.0, data[0]}, []float64{0.0, 1.0}
	}
	var xs []float64
	var ys []float64
	xs = append(xs, 0.0)
	ys = append(ys, 0.0)
	cx := data[0]
	cw := 0.0
	for i, x := range data {
		if x != cx {
			if x < cx {
				log.Panic("Input not sorted, BuildCDF failed")
			}
			xs = append(xs, cx)
			ys = append(ys, cw)
			cx = x
		}
		if weights == nil {
			cw += 1
		} else {
			cw += weights[i]
		}
	}

	var cdfx []float64
	var cdfy []float64
	cdfStep := 0.001
	height := cdfStep
	for i, x := range xs {
		y := ys[i] / cw
		if y < height {
			continue
		}
		cdfx = append(cdfx, x)
		cdfy = append(cdfy, y)
		height = math.Floor(y/cdfStep)*cdfStep + cdfStep
	}
	cdfx = append(cdfx, xs[len(xs)-1])
	cdfy = append(cdfy, 1.0)
	return cdfx, cdfy
}

func DumpCDF(fpath string, cdfx, cdfy []float64) {
	fd, err := os.Create(fpath)
	CheckError(err)
	defer fd.Close()
	for i, x := range cdfx {
		fd.WriteString(fmt.Sprintf("%f %f\n", x, cdfy[i]))
	}
}

// func GetQuartiles(data []interface{}, extract func(interface{}) float64) [3]float64 {

// }

func GetQuartiles(sorted []float64) [3]float64 {
	length := float64(len(sorted))
	i25 := math.Floor(length/4 - 0.5)
	w25 := (length/4 - 0.5) - i25
	p25 := (1-w25)*sorted[int(i25)] + w25*sorted[int(i25)+1]
	i50 := math.Floor(length/2 - 0.5)
	w50 := (length/2 - 0.5) - i50
	p50 := (1-w50)*sorted[int(i50)] + w50*sorted[int(i50)+1]
	i75 := math.Floor(3*length/4 - 0.5)
	w75 := (3*length/4 - 0.5) - i75
	p75 := (1-w75)*sorted[int(i75)] + w75*sorted[int(i75)+1]
	return [3]float64{p25, p50, p75}
}

func CheckError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
