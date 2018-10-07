package bactract

import (
	"fmt"
)

// readBigInt reads the value for an 8 byte integer column
func readBigInt(r *tReader, tc TableColumn) (ec ExtractedColumn, err error) {

	debOut("Func readBigInt")

	// Determine how many bytes to read
	ss, err := r.readStoredSize(tc, 1, 8)
	if err != nil {
		return ec, err
	}

	// Check for nulls
	ec.IsNull = ss.isNull
	if ss.isNull {
		return ec, err
	}

	// Read and translate the integer
	b, err := r.readBytes("readBigInt", ss.byteCount)
	if err != nil {
		return ec, err
	}

	var z int64
	for i, sb := range stripTrailingNulls(b) {
		z |= int64(sb) << uint(8*i)
	}

	ec.Str = fmt.Sprint(z)

	return ec, err
}
