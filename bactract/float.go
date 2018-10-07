package bactract

import (
	"fmt"
	"math"
	"strings"
)

// readFloat reads the value for a 4 or 8 byte float column
func readFloat(r *tReader, tc TableColumn) (ec ExtractedColumn, err error) {

	debOut("Func readFloat")

	// Determine how many bytes to read
	var defCount int
	if tc.Precision <= 24 {
		defCount = 4
	} else {
		defCount = 8
	}

	ss, err := r.readStoredSize(tc, 1, defCount)
	if err != nil {
		return ec, err
	}

	// Check for nulls
	ec.IsNull = ss.isNull
	if ss.isNull {
		return ec, err
	}

	// Read and translate the integer
	b, err := r.readBytes("readFloat", ss.byteCount)
	if err != nil {
		return ec, err
	}

	var z uint64
	for i := 0; i < ss.byteCount; i++ {
		z |= uint64(b[i]) << uint(8*i)
	}

	f := math.Float64frombits(z)

	s := fmt.Sprint(f)
	if strings.Contains(s, ".") {
		ec.Str = s
	} else if strings.Contains(s, "e") {
		ec.Str = s
	} else {
		ec.Str = strings.Join([]string{s, "0"}, ".")
	}

	return ec, err
}
