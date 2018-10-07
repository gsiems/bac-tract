package bactract

import (
	"fmt"
)

// readBit reads the value for a 1 byte integer column
func readBit(r *tReader, tc TableColumn) (ec ExtractedColumn, err error) {

	debOut("Func readBit")

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
	b, err := r.readBytes("readBit", ss.byteCount)
	if err != nil {
		return ec, err
	}

	ec.Str = fmt.Sprint(b[0])

	return ec, err
}
