// Extract column metadata for one or more tables from an unzipped bacpac file

package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"runtime/pprof"
	"sort"
	"strings"

	//
	bp "github.com/gsiems/bac-tract/bactract"
)

type params struct {
	baseDir    string
	tableName  string
	tablesFile string
}

func main() {

	var v params

	flag.StringVar(&v.baseDir, "b", "", "The directory containing the unzipped bacpac file.")
	flag.StringVar(&v.tableName, "t", "", "The table to extract column meta-data from. When not specified then extract column meta-data from all tables")
	flag.StringVar(&v.tablesFile, "f", "", "The file to read the list of tables to extract column meta-data from, one table per line")
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

	doDump(v)
}

func doDump(v params) {

	p, _ := bp.New(v.baseDir)

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
		for t, _ := range model.Tables {
			tables = append(tables, t)
		}
		sort.Strings(tables)
	}

	fmt.Println(strings.Join([]string{"table_schema",
		"table_name", "column_name", "ordinal_position", "is_nullable",
		"data_type", "character_maximum_length", "numeric_precision",
		"numeric_scale"}, "\t"))

	for _, table := range tables {
		t, ok := model.Tables[table]
		if ok {

			for i, c := range t.Columns {

				var attr []string
				attr = append(attr, t.Schema)
				attr = append(attr, t.TabName)
				attr = append(attr, c.ColName)
				attr = append(attr, fmt.Sprintf("%d", i))
				if c.IsNullable {
					attr = append(attr, "YES")
				} else {
					attr = append(attr, "NO")
				}
				attr = append(attr, c.DtStr)

				if isChar(c.DtStr) {
					attr = append(attr, fmt.Sprintf("%d", c.Length))
				} else if isBinary(c.DtStr) {
					attr = append(attr, fmt.Sprintf("%d", c.Length))
				} else {
					attr = append(attr, "")
				}

				attr = append(attr, fmt.Sprintf("%d", c.Precision))
				attr = append(attr, fmt.Sprintf("%d", c.Scale))

				fmt.Println(strings.Join(attr, "\t"))
			}
		}
	}
}

func isChar(dt string) (b bool) {
	switch dt {
	case "char", "varchar", "text", "nchar", "nvarchar", "ntext":
		return true
	}
	return false
}

func isBinary(dt string) (b bool) {
	switch dt {
	case "binary", "varbinary":
		return true
	}
	return false
}

func dieOnErrf(s string, err error) {
	if err != nil {
		log.Fatalf(s, err)
	}
}
