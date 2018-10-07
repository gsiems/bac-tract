package bactract

import (
	"fmt"
	"math"
	"strings"
)

// readReal reads the value for a 4 byte integer column
func readReal(r *tReader, tc TableColumn) (ec ExtractedColumn, err error) {

	debOut("Func readReal")

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
	b, err := r.readBytes("readReal", ss.byteCount)
	if err != nil {
		return ec, err
	}

	var z uint32
	for i := 0; i < ss.byteCount; i++ {
		z |= uint32(b[i]) << uint(8*i)
	}

	f := math.Float32frombits(z)

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
