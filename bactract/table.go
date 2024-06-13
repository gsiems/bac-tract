package bactract

// Read/parse the bacpac BCP data files.

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
)

type tReader struct {
	reader *buffFileReader
	//Rownum int
	table Table
}

// ExtractedColumn contains the data/metadata for a column extracted from a row of data.
type ExtractedColumn struct {
	ColName    string
	DataType   int
	DtStr      string
	Length     int
	Scale      int
	Precision  int
	IsNullable bool
	IsNull     bool
	Str        string
}

type storedSize struct {
	byteCount int
	isNull    bool
	sizeBytes []byte
}

type fn func(r *tReader, tc TableColumn) (ec ExtractedColumn, err error)

var dt = map[int]fn{
	BigInt:           readInteger,
	Binary:           readBinary,
	Bit:              readBit,
	Char:             readString,
	Date:             readDate,
	Datetime2:        readDatetime2,
	Datetime:         readDatetime,
	Decimal:          readDecimal,
	Float:            readFloat,
	Geography:        readGeography,
	Int:              readInteger,
	Money:            readMoney,
	NText:            readNText,
	Numeric:          readDecimal,
	NVarchar:         readNVarchar,
	Real:             readReal,
	SmallDatetime:    readSmallDatetime,
	SmallInt:         readInteger,
	SmallMoney:       readSmallMoney,
	Text:             readString,
	Time:             readTime,
	TinyInt:          readInteger,
	UniqueIdentifier: readUniqueIdentifier,
	Varbinary:        readVarbinary,
	Varchar:          readString,
	//NChar:            readNChar,
	//SQLVariant:       readSQLVariant,
}

// DataReader creates a multi-file-reader on the data files for the specified table
func (t *Table) DataReader() (reader tReader, err error) {

	files, err := ioutil.ReadDir(t.DataDir)
	if err == os.ErrNotExist {
		return reader, nil
	}
	if err != nil {
		return reader, err
	}

	var bcpFiles []string
	for _, f := range files {
		if strings.HasSuffix(f.Name(), "BCP") {
			filename := catDir([]string{t.DataDir, f.Name()})
			bcpFiles = append(bcpFiles, filename)
		}
	}

	reader.reader = BuffFileReader(0, bcpFiles)
	reader.table = *t

	return reader, err
}

// ReadNextRow reads the next table row from the BCP file and ...
func (r *tReader) ReadNextRow() (row []ExtractedColumn, err error) {

	for _, tc := range r.table.Columns {

		if debugFlag {
			debOut(fmt.Sprintf("%q %s %d, %d, %d, %v", tc.ColName, tc.DtStr, tc.Length, tc.Precision, tc.Scale, tc.IsNullable))
		}

		fcn, ok := dt[tc.DataType]
		if ok {
			ec, err := fcn(r, tc)
			if err != nil {
				if err == io.EOF {
					if debugFlag {
						debOut("\nEOF")
					}
				}

				// TODO?
				// cache the offending row and attempt to determine if
				// there is a preceeding, potentially offending column
				// (see readInteger) to determine if another attempt at
				// the can be taken.

				return row, err
			}

			ec.ColName = tc.ColName
			ec.DataType = tc.DataType

			ec.Length = tc.Length
			ec.Scale = tc.Scale
			ec.Precision = tc.Precision
			ec.IsNullable = tc.IsNullable
			ec.DtStr = tc.DtStr

			if debugFlag {
				if len(ec.Str) > debugLen && debugLen > 10 {
					s := fmt.Sprintf("%s ... %s", ec.Str[0:debugLen-6], ec.Str[len(ec.Str)-4:])
					debOut(fmt.Sprintf("Str: %s", s))
				} else {
					debOut(fmt.Sprintf("Str: %s", ec.Str))
				}
				debOut(fmt.Sprintf("IsNull: %v", ec.IsNull))
				debOut("")
			}

			row = append(row, ec)
		} else {
			err = fmt.Errorf("No parser defined for column %q (datatype %s)", tc.ColName, tc.DtStr)
			return row, err
		}
	}

	return row, nil
}

// readBytes reads the specified number of bytes from the reader
func (r *tReader) readBytes(label string, n int) (b []byte, err error) {

	if debugFlag {
		debOut(fmt.Sprintf("%s: Attempting to read %d bytes", label, n))
		// NB recover added to help when debugging parsing errors
		defer func() {
			if r := recover(); r != nil {
				err = errors.New(fmt.Sprintf("readBytes panic for n = %d: %s", n, r))
				return
			}
		}()
	}

	if n == 0 {
		return
	}

	b = make([]byte, n)
	_, err = r.reader.Read(b)

	if debugFlag {
		debHextOut("Bytes", b)
	}
	return b, err
}

// readStoredSize reads the specified number of bytes to determine the
// number of bytes used to store the value for the associated field.
// For example, a null int uses 0 bytes of storage while a non-null int
// uses 4 bytes.
func (r *tReader) readStoredSize(tc TableColumn, n, def int) (s storedSize, err error) {

	s.isNull = tc.IsNullable
	if !tc.IsNullable && def > 0 {
		// Just return the default for not-null columns
		s.byteCount = def
		return
	}

	// So either the column is nullable or there is no default size--
	// therfore read n bytes to determine how many data bytes to read
	s.sizeBytes, err = r.readBytes("readStoredSize", n)
	if err != nil {
		return
	}

	// All size bytes == 0xff indicates a null column
	isnull := true
	for i := 0; i < len(s.sizeBytes); i++ {
		if s.sizeBytes[i] != 0xff {
			isnull = false
			break
		}
	}
	if isnull {
		s.isNull = tc.IsNullable
		s.byteCount = 0
		return
	}

	// Discard trailing nulls when calculating the storage byte count.
	// For example, if a varchar uses 2 bytes to store the byteCount
	// but the varchar is only, say, 4 bytes long then the second
	// "storage size" byte is 0x00 and should not enter into the
	// byteCount calculation.
	b := stripTrailingNulls(s.sizeBytes)
	if len(b) == 0 {
		return
	}

	for i := 0; i < len(b); i++ {
		s.byteCount |= int(b[i]) << uint(8*i)
		s.isNull = false
	}

	return
}
