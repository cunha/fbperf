package minsamples

import (
	"log"
	"math"
)

func GetConfidenceIntervalPercentiles(pctile uint32, samples uint32) (uint32, uint32) {
	z := 1.959964
	pp := float64(pctile) / 100.0
	diff := 100 * z * math.Sqrt(pp*(1-pp)/float64(samples))
	lower := uint32(math.Floor(math.Max(float64(pctile)-diff, 0)))
	upper := uint32(math.Ceil(math.Min(float64(pctile)+diff, 100)))
	return lower, upper
}

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

func CheckError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
