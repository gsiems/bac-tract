// Package bactract is an exploration/attempt at extracting data from
// MS SQL Server bacpac files (BACpac-exTRACT)
package bactract

import (
	//"errors"
	//"fmt"
	//"io"
	"io/ioutil"
	"os"
	//"strings"
)

const debugFlag = false // Whether or not to spew debugging information to STDOUT
const debugLen = 30     // Trim the length of byte arrays and strings when outputting debug information

// Note that this is an incomplete (I think) list of the possible
// datatypes, however, ya gotta work with what ya got
const (
	NullDatatype     = iota
	BigInt           = iota
	Binary           = iota
	Bit              = iota
	Char             = iota
	Date             = iota
	Datetime         = iota
	DatetimeOffset   = iota
	Datetime2        = iota
	Decimal          = iota
	Float            = iota
	Geography        = iota
	Int              = iota
	Money            = iota
	NChar            = iota
	NText            = iota
	Numeric          = iota
	NVarchar         = iota
	Real             = iota
	SmallDatetime    = iota
	SmallInt         = iota
	SmallMoney       = iota
	SQLVariant       = iota
	Time             = iota
	Text             = iota
	TinyInt          = iota
	UniqueIdentifier = iota
	Varbinary        = iota
	Varchar          = iota
)

// Bacpac is the base for an unzipped bacpac file
type Bacpac struct {
	baseDir string
}

// New returns a new Bacpac
func New(baseDir string) (b Bacpac, err error) {
	b.baseDir = baseDir

	return b, err
}

// ExportedTables returns the list of data containing tables found in the bacpac
func (b Bacpac) ExportedTables() (s []string, err error) {

	dir := catDir([]string{b.baseDir, "Data"})

	dirs, err := ioutil.ReadDir(dir)
	if err != nil {
		return s, err
	}

	for _, d := range dirs {

		p := catDir([]string{b.baseDir, "Data", d.Name()})

		fi, err := os.Stat(p)
		if err != nil {
			return s, err
		}
		switch mode := fi.Mode(); {
		case mode.IsDir():
			s = append(s, d.Name())
		}
	}

	return s, err
}
