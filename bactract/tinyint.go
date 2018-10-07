package bactract

import (
	"fmt"
)

// readTinyInt reads the value for a 1 byte integer column
func readTinyInt(r *tReader, tc TableColumn) (ec ExtractedColumn, err error) {

	debOut("Func readTinyInt")

	// Determine how many bytes to read
	ss, err := r.readStoredSize(tc, 1, 1)
	if err != nil {
		return ec, err
	}

	// Check for nulls
	ec.IsNull = ss.isNull
	if ss.isNull {
		return ec, err
	}

	// Read and translate the integer
	b, err := r.readBytes("readText", ss.byteCount)
	if err != nil {
		return ec, err
	}

	z := int8(b[0])

	ec.Str = fmt.Sprint(z)

	return ec, err
}
