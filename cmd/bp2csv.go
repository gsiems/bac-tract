// Extract one or more tables from an unzipped bacpac file and write to the corresponding comma-separated file(s)

package main

import (
	"bytes"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"runtime/pprof"
	//
	bp "github.com/gsiems/bac-tract/bactract"
)

type params struct {
	baseDir    string
	tableName  string
	tablesFile string
	rowLimit   uint64
}

func main() {

	var v params

	flag.StringVar(&v.baseDir, "b", "", "The directory containing the unzipped bacpac file.")
	flag.StringVar(&v.tableName, "t", "", "The table to extract data from. When not specified then extract all tables")
	flag.StringVar(&v.tablesFile, "f", "", "The file to read the list of tables to extract from, one table per line")
	flag.Uint64Var(&v.rowLimit, "c", 0, "The number of rows to extract. When 0 extract all rows.")
	var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
	//var memprofile = flag.String("memprofile", "", "write memory profile to this file")

	flag.Parse()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		err = pprof.StartCPUProfile(f)
		if err != nil {
			log.Fatal(err)
		}
		defer pprof.StopCPUProfile()
	}

	doDump(v)
}

func doDump(v params) {

	p, _ := bp.New(v.baseDir)

	model, err := p.GetModel()
	dieOnErrf("GetModel failed: %q", err)

	var tables []string
	if v.tableName != "" {
		tables = append(tables, v.tableName)
	} else if v.tablesFile != "" {

		content, err := ioutil.ReadFile(v.tablesFile)
		dieOnErrf("File read failed: %q", err)

		x := bytes.Split(content, []byte("\n"))
		for _, z := range x {
			tables = append(tables, string(z))
		}
	} else {
		tables, err = p.ExportedTables()
		dieOnErrf("ExportedTables failed: %q", err)
	}

	for _, table := range tables {
		t, ok := model.Tables[table]
		if ok {
			mkFile(t, v)
		}
	}
}

func mkFile(t bp.Table, v params) {

	r, err := t.DataReader()
	dieOnErrf("DataReader failed: %q", err)

	target := fmt.Sprintf("%s.%s.csv", t.Schema, t.TabName)
	f := openOutput(target)
	defer deferredClose(f)
	w := csv.NewWriter(f)

	writeHdr := true
	var i uint64
	for {

		if v.rowLimit > 0 {
			i++
			if i > v.rowLimit {
				break
			}
		}

		row, err := r.ReadNextRow()
		if err == io.EOF {
			break
		}
		dieOnErr(err)

		if writeHdr {
			var cols []string
			for _, ec := range row {
				cols = append(cols, ec.ColName)
			}
			err = w.Write(cols)
			dieOnErr(err)
			writeHdr = false
		}

		var cols []string
		for _, ec := range row {
			cols = append(cols, ec.Str)
		}
		err = w.Write(cols)
		dieOnErr(err)
	}
	w.Flush()
	dieOnErr(w.Error())
}

// openOutput opens the appropriate target for writing output, or dies trying
func openOutput(target string) (f *os.File) {

	var err error

	if target == "" || target == "-" {
		f = os.Stdout
	} else {
		f, err = os.OpenFile(target, os.O_CREATE|os.O_WRONLY, 0644)
		dieOnErrf("File open failed: %q", err)
	}
	return f
}

// deferredClose closes a file handle, or dies trying
func deferredClose(f *os.File) {
	err := f.Close()
	dieOnErrf("File close failed: %q", err)
}

func dieOnErrf(s string, err error) {
	if err != nil {
		log.Fatalf(s, err)
	}
}

func dieOnErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
