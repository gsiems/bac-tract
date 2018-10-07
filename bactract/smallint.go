package bactract

import (
	"fmt"
)

// readSmallInt reads the value for a 2 byte integer column
func readSmallInt(r *tReader, tc TableColumn) (ec ExtractedColumn, err error) {

	debOut("Func readSmallInt")

	// Determine how many bytes to read
	ss, err := r.readStoredSize(tc, 1, 2)
	if err != nil {
		return ec, err
	}

	// Check for nulls
	ec.IsNull = ss.isNull
	if ss.isNull {
		return ec, err
	}

	// Read and translate the integer
	b, err := r.readBytes("readSmallInt", ss.byteCount)
	if err != nil {
		return ec, err
	}

	var z int16
	for i, sb := range stripTrailingNulls(b) {
		z |= int16(sb) << uint(8*i)
	}

	ec.Str = fmt.Sprint(z)

	return ec, err
}
