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
	cpuprofile string
	memprofile string
	debug      bool
}

func main() {

	var v params

	flag.StringVar(&v.baseDir, "b", "", "The directory containing the unzipped bacpac file.")
	flag.StringVar(&v.tableName, "t", "", "The table to extract data from. When not specified then extract all tables")
	flag.StringVar(&v.tablesFile, "f", "", "The file to read the list of tables to extract from, one table per line")
	flag.Uint64Var(&v.rowLimit, "c", 0, "The number of rows to extract. When 0 extract all rows.")
	flag.BoolVar(&v.debug, "debug", false, "Write debug information to STDOUT.")
	flag.StringVar(&v.cpuprofile, "cpuprofile", "", "The filename to write cpu profile information to")
	//flag.StringVar(&v.memprofile, "memprofile", "", The filename to write memory profile information to")

	flag.Parse()

	if v.cpuprofile != "" {
		f, err := os.Create(v.cpuprofile)
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

	p.SetDebug(v.debug)

	model, err := p.GetModel("")
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
			hasBinary := false
			for _, c := range t.Columns {
				if c.DataType == bp.Binary || c.DataType == bp.Varbinary || ec.DataType == bp.Geography {
					hasBinary = true
				}
			}

			if hasBinary {
				log.Printf("Warning: \"%s.%s\" has possible binary data.\n", t.Schema, t.TabName)
			}
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

		i++
		if v.rowLimit > 0 && i > v.rowLimit {
			break
		}

		row, err := r.ReadNextRow()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error: \"%s.%s\" (row %d): %s.\n", t.Schema, t.TabName, i, err)
			break
		}

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

			if ec.DataType == bp.Varbinary || ec.IsNull {
				cols = append(cols, "")
			} else {
				cols = append(cols, ec.Str)
			}
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
