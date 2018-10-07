package bactract

import (
	"fmt"
)

// readSmallMoney reads the value for a small money column
func readSmallMoney(r *tReader, tc TableColumn) (ec ExtractedColumn, err error) {

	debOut("Func readSmallMoney")

	// Determine how many bytes to read
	ss, err := r.readStoredSize(tc, 1, 4)
	if err != nil {
		return ec, err
	}

	// Check for nulls
	ec.IsNull = ss.isNull
	if ss.isNull {
		return ec, err
	}

	// Read and translate the integer
	b, err := r.readBytes("readSmallMoney", ss.byteCount)
	if err != nil {
		return ec, err
	}

	// TODO

	var z int32
	for i, sb := range stripTrailingNulls(b) {
		z |= int32(sb) << uint(8*i)
	}

	ec.Str = fmt.Sprint(z)

	return ec, err
}
