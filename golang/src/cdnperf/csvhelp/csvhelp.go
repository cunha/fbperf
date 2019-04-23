package csvhelp

import (
	"compress/gzip"
	"encoding/csv"
	"io"
	"os"

	"cdnperf"
	"github.com/gocarina/gocsv"
)

func ParseFile(fpath string, rowfunc interface{}) {
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
