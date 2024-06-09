// Generate table creation DDL for one or more tables from an unzipped bacpac file

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

	"github.com/gsiems/sql-parse/sqlparse"
	parser "github.com/gsiems/sql-parse/sqlparse"
)

type params struct {
	baseDir    string
	tableName  string
	tablesFile string
	dialect    int
	cpuprofile string
	memprofile string
	debug      bool
}

func main() {

	var v params
	var dialect string

	flag.StringVar(&v.baseDir, "b", "", "The directory containing the unzipped bacpac file.")
	flag.StringVar(&dialect, "d", "Std", "The DDL dialect to output [Ora|Pg|Std].")
	flag.StringVar(&v.tableName, "t", "", "The table to generate the CREATE TABLE command for. When not specified then generate the DDL for all tables.")
	flag.StringVar(&v.tablesFile, "f", "", "The file to read the list of tables to extract from, one table per line")
	flag.StringVar(&v.cpuprofile, "cpuprofile", "", "The filename to write cpu profile information to")
	//flag.StringVar(&v.memprofile, "memprofile", "", The filename to write memory profile information to")

	flag.Parse()

	switch dialect {
	case "Pg":
		v.dialect = parser.PostgreSQL
	case "Ora":
		v.dialect = parser.Oracle
	default:
		v.dialect = parser.StandardSQL
	}

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

	for _, table := range tables {
		t, ok := model.Tables[table]

		if ok {
			fmt.Printf("CREATE TABLE %s.%s (\n    ", formatIdent(t.Schema, v.dialect), formatIdent(t.TabName, v.dialect))
			var colDefs []string

			for _, c := range t.Columns {

				colDef := formatIdent(c.ColName, v.dialect) + " " + convDatatype(c.DtStr, c.Length, c.Precision, c.Scale, v.dialect)
				if c.IsNullable {
					colDefs = append(colDefs, colDef)
				} else {
					colDefs = append(colDefs, colDef+" NOT NULL")
				}

			}
			fmt.Printf("%s", strings.Join(colDefs, ",\n    "))

			// Primary Key
			pkCols := joinCols(t.PK.Columns, v.dialect)
			if pkCols != "" {
				consname := fmt.Sprintf("pk_%s", t.TabName)
				pk_name := formatIdent(consname, v.dialect)
				fmt.Printf(",\n    CONSTRAINT %s PRIMARY KEY ( %s )", pk_name, pkCols)
			}

			// Unique Constraints
			for _, c := range t.Unique {
				uCols := joinCols(c.Columns, v.dialect)
				if uCols != "" {
					consname := formatIdent(c.ConsName, v.dialect)
					fmt.Printf(",\n    CONSTRAINT %s UNIQUE ( %s )", consname, uCols)
				}
			}

			fmt.Print(" ) ;\n\n")
		}
	}

	for _, table := range tables {
		t, ok := model.Tables[table]

		if ok {

			// Foreign Key Constraints
			for _, c := range t.FKs {

				fkCols := joinCols(c.Columns, v.dialect)
				refCols := joinCols(c.RefColumns, v.dialect)

				rt, ok2 := model.Tables[c.RefTable]
				if ok2 {
					fmt.Printf("ALTER TABLE %s.%s\n", formatIdent(t.Schema, v.dialect), formatIdent(t.TabName, v.dialect))
					fmt.Printf("    ADD CONSTRAINT %s FOREIGN KEY ( %s )\n", formatIdent(c.ConsName, v.dialect), fkCols)
					fmt.Printf("    REFERENCES %s.%s ( %s ) ;\n\n", formatIdent(rt.Schema, v.dialect), formatIdent(rt.TabName, v.dialect), refCols)
				}
			}
		}
	}
}

func joinCols(cols []string, dialect int) string {

	var cl []string
	for _, col := range cols {
		cl = append(cl, formatIdent(col, dialect))
	}

	return strings.Join(cl, ", ")
}

func formatIdent(s string, dialect int) string {

	if sqlparse.IsIdentifier(s, dialect) && !sqlparse.IsKeyword(s, dialect) {
		return strings.ToLower(s)
	}

	if dialect == parser.PostgreSQL {
		return fmt.Sprintf("%q", strings.ToLower(s))
	}
	return fmt.Sprintf("%q", strings.ToUpper(s))

}

func convDatatype(dt string, len, precision, scale, dialect int) string {

	switch dialect {
	case parser.PostgreSQL:
		return pgColType(dt, len, precision, scale)
	case parser.Oracle:
		return oraColType(dt, len, precision, scale)
	}

	return stdColType(dt, len, precision, scale)
}

func stdType(dt string, len int) string {

	var typeMap = map[string]string{
		"bigint":           "bigint",
		"binary":           "blob",
		"bit":              "boolean",
		"char":             "character",
		"date":             "date",
		"datetime2":        "timestamp",
		"datetime":         "timestamp",
		"datetimeoffset":   "timestamp with timezone",
		"decimal":          "decimal",
		"float":            "float",
		"int":              "int",
		"money":            "decimal",
		"nchar":            "national character",
		"ntext":            "nclob",
		"nvarchar":         "national character varying",
		"real":             "real",
		"smalldatetime":    "timestamp",
		"smallint":         "smallint",
		"smallmoney":       "decimal",
		"text":             "clob",
		"tinyint":          "smallint",
		"uniqueidentifier": "uuid",
		"varbinary":        "blob",
		"varchar":          "character varying",
	}

	stdtype, ok := typeMap[dt]
	if !ok {
		stdtype = strings.ToUpper(dt)
	}

	return stdtype
}

func stdColType(dt string, len, precision, scale int) string {

	switch dt {
	case "money":
		precision = 20
		scale = 4
	case "smallmoney":
		precision = 10
		scale = 4
	}

	datatype := stdType(dt, len)

	switch datatype {
	case "boolean", "blob", "clob", "smallint", "int", "bigint", "date", "timestamp", "timestamp with timezone", "uuid":
		return datatype
	}

	if precision != 0 && scale != 0 {
		return fmt.Sprintf("%s ( %d, %d )", datatype, precision, scale)
	}
	if precision != 0 {
		return fmt.Sprintf("%s ( %d )", datatype, precision)
	}
	if len != 0 {
		return fmt.Sprintf("%s ( %d )", datatype, len)
	}

	return datatype
}

func pgType(dt string, len int) string {

	var typeMap = map[string]string{
		"bigint":           "bigint",
		"binary":           "bytea",
		"bit":              "boolean",
		"char":             "char",
		"date":             "date",
		"datetime2":        "timestamp",
		"datetime":         "timestamp",
		"datetimeoffset":   "timestamp with timezone",
		"decimal":          "numeric",
		"float":            "double precision",
		"geography":        "varchar",
		"int":              "int",
		"money":            "numeric",
		"nchar":            "char",
		"ntext":            "varchar",
		"nvarchar":         "varchar",
		"real":             "real",
		"smalldatetime":    "timestamp",
		"smallint":         "smallint",
		"smallmoney":       "numeric",
		"text":             "text",
		"tinyint":          "smallint",
		"uniqueidentifier": "uuid",
		"varbinary":        "bytea",
		"varchar":          "varchar",
	}

	pgtype, ok := typeMap[dt]
	if !ok {
		pgtype = "varchar"
	}

	return pgtype
}

func pgColType(dt string, len, precision, scale int) string {

	switch dt {
	case "money":
		precision = 20
		scale = 4
	case "smallmoney":
		precision = 10
		scale = 4
	}

	datatype := pgType(dt, len)

	switch datatype {
	case "boolean", "bytea", "text", "smallint", "int", "bigint", "date", "timestamp", "timestamp with timezone", "uuid":
		return datatype
	}

	if precision != 0 && scale != 0 {
		return fmt.Sprintf("%s ( %d, %d )", datatype, precision, scale)
	}
	if precision != 0 {
		return fmt.Sprintf("%s ( %d )", datatype, precision)
	}
	if len != 0 {
		return fmt.Sprintf("%s ( %d )", datatype, len)
	}

	return datatype
}

func oraType(dt string, len int) string {

	var typeMap = map[string]string{
		"bigint":           "number",
		"binary":           "raw",
		"bit":              "number",
		"char":             "char",
		"datetime2":        "timestamp",
		"datetime":         "timestamp",
		"decimal":          "number",
		"float":            "float",
		"geography":        "varchar2",
		"int":              "number",
		"money":            "number",
		"nchar":            "nchar",
		"ntext":            "nclob",
		"nvarchar":         "nvarchar2",
		"real":             "float",
		"smalldatetime":    "date",
		"smallint":         "number",
		"smallmoney":       "number",
		"text":             "clob",
		"tinyint":          "number",
		"uniqueidentifier": "raw(16)",
		"varbinary":        "blob",
		"varchar":          "varchar2",
	}

	oratype, ok := typeMap[dt]
	if !ok {
		oratype = "clob"
	}

	switch oratype {
	case "char", "varchar":
		if len == 0 || len > 4000 {
			oratype = "clob"
		}
	case "nchar", "nvarchar":
		if len == 0 || len > 4000 {
			oratype = "nclob"
		}
	}

	return oratype
}

func oraColType(dt string, len, precision, scale int) string {

	switch dt {
	case "bigint":
		precision = 20
	case "bit":
		precision = 1
	case "int":
		precision = 10
	case "geography":
		len = 4000
	case "money":
		precision = 20
		scale = 4
	case "smallint":
		precision = 6
	case "smallmoney":
		precision = 10
		scale = 4
	case "tinyint":
		precision = 3
	}

	datatype := oraType(dt, len)

	switch datatype {
	case "blob", "clob", "nclob", "raw", "date":
		return datatype
	case "raw(16)":
		return "raw ( 16 )"
	}

	if precision != 0 && scale != 0 {
		return fmt.Sprintf("%s ( %d, %d )", datatype, precision, scale)
	}
	if precision != 0 {
		return fmt.Sprintf("%s ( %d )", datatype, precision)
	}
	if len != 0 {
		return fmt.Sprintf("%s ( %d )", datatype, len)
	}

	return datatype
}

func dieOnErrf(s string, err error) {
	if err != nil {
		log.Fatalf(s, err)
	}
}
