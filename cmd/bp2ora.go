// Extract one or more tables from an unzipped bacpac file and write to
// the corresponding Oracle SQL*Loader file(s)

package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"runtime/pprof"
	"strings"

	//
	bp "github.com/gsiems/bac-tract/bactract"
)

type params struct {
	baseDir    string
	tableName  string
	tablesFile string
	rowLimit   uint64
	workers    int
}

type workItem struct {
	ID     int
	V      params
	Tab    bp.Table
	doneBy int
}

type Worker struct {
	workerID int
	todo     chan workItem
	done     chan workItem
}

func newWorker(workerID int, todo chan workItem, done chan workItem) Worker {
	return Worker{
		workerID: workerID,
		todo:     todo,
		done:     done,
	}
}

func (w Worker) start() {
	go func() {
		for {
			select {
			case item := <-w.todo:
				item.doneBy = w.workerID
				mkFile(item.Tab, item.V)
				w.done <- item
			}
		}
	}()
}

func main() {

	var v params

	flag.StringVar(&v.baseDir, "b", "", "The directory containing the unzipped bacpac file.")
	flag.StringVar(&v.tableName, "t", "", "The table to extract data from. When not specified then extract all tables")
	flag.StringVar(&v.tablesFile, "f", "", "The file to read the list of tables to extract from, one table per line")
	flag.Uint64Var(&v.rowLimit, "c", 0, "The number of rows to extract. When 0 extract all rows.")
	flag.IntVar(&v.workers, "w", 1, "The number of workers to use")
	var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
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

	tables := getTables(v)

	// create the channels
	todo := make(chan workItem, len(tables))
	done := make(chan workItem, 1)

	// start the workers
	for i := 0; i < v.workers; i++ {
		worker := newWorker(i, todo, done)
		worker.start()
	}

	// feed the work queue
	for i, table := range tables {
		item := workItem{ID: i, V: v, Tab: table}
		todo <- item
	}

	// wait for/catch the results
	for range tables {
		select {
		case _ = <-done:
		}
	}

}

func getTables(v params) (l []bp.Table) {

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
			l = append(l, t)
		}
	}

	return
}

func mkFile(t bp.Table, v params) {

	err := mkLoaderCtl(t)
	dieOnErr(err)

	err = mkLoaderDat(t, v)
	dieOnErr(err)
}

// mkLoaderDat generates the data file for SQL*Loader
func mkLoaderDat(t bp.Table, v params) (err error) {

	colSep := []byte(string(0x1c))
	recSep := []byte(" 0X1E")
	newLine := []byte("\n")

	r, err := t.DataReader()
	dieOnErrf("DataReader failed: %q", err)

	target := fmt.Sprintf("%s.%s.dat", t.Schema, t.TabName)
	f := openOutput(target)
	defer deferredClose(f)
	w := bufio.NewWriter(f)

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

		for j, ec := range row {
			if j > 0 {
				w.Write(colSep)
			}

			if ec.DataType != bp.Varbinary && !ec.IsNull {
				w.Write([]byte(ec.Str))
			}
		}
		w.Write(recSep)
		w.Write(newLine)

	}
	w.Flush()
	return
}

// mkLoaderCtl generates the essential Oracle SQL*Loader control file
func mkLoaderCtl(t bp.Table) (err error) {

	target := fmt.Sprintf("%s.%s.ctl", t.Schema, t.TabName)
	f := openOutput(target)
	defer deferredClose(f)
	w := bufio.NewWriter(f)

	var ctl []byte

	ctl = append(ctl, []byte("LOAD DATA\n")...)
	ctl = append(ctl, []byte("CHARACTERSET UTF8\n")...)
	ctl = append(ctl, []byte(fmt.Sprintf("INFILE %s.%s.dat \"str ' 0X1E\\n'\"\n", t.Schema, t.TabName))...)
	ctl = append(ctl, []byte(fmt.Sprintf("TRUNCATE INTO TABLE %s\n", t.TabName))...)
	ctl = append(ctl, []byte("FIELDS TERMINATED BY X'1C'\n")...)
	ctl = append(ctl, []byte("TRAILING NULLCOLS\n")...)
	ctl = append(ctl, []byte("(\n")...)

	for i, c := range t.Columns {

		colName := strings.ToUpper(c.ColName)

		if i > 0 {
			ctl = append(ctl, []byte(",\n")...)
		}
		ctl = append(ctl, []byte(fmt.Sprintf("    %q", colName))...)

		if c.DtStr == "datetime" || c.DtStr == "smalldatetime" {
			ctl = append(ctl, []byte(" DATE \"YYYY-MM-DD HH24:MI:SS\"")...)
		} else if c.DtStr == "datetime2" {
			ctl = append(ctl, []byte(" TIMESTAMP \"YYYY-MM-DD HH24:MI:SS.FF\"")...)
		}

		// if len too long then add char(len)
		if c.Length > 256 { // No, I really don't know where the threshold is...
			ctl = append(ctl, []byte(fmt.Sprintf(" char ( %d )", c.Length))...)
		}

		if c.IsNullable {
			ctl = append(ctl, []byte(fmt.Sprintf(" nullif %q=blanks", colName))...)
		}
	}

	ctl = append(ctl, []byte("\n)")...)

	w.Write(ctl)
	w.Flush()

	return
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
