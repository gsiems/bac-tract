package bactract

import (
	"fmt"
	"math"
	"strings"
)

// readReal reads the value for a 4 byte integer column
func readReal(r *tReader, tc TableColumn) (ec ExtractedColumn, err error) {

	fn := "readReal"
	defSz := 4
	debOut(fmt.Sprintf("Func %s", fn))

	// Determine how many bytes to read
	ss, err := r.readStoredSize(tc, 1, defSz)
	if err != nil {
		return
	}

	// Check for nulls
	ec.IsNull = ss.isNull
	if ss.isNull {
		return
	}

	// Assert: If not null then the stored size is the default
	if ss.byteCount != defSz {
		err = fmt.Errorf("%s byteCount too large for column %q (%d vs %d)", fn, tc.ColName, ss.byteCount, defSz)
		return
	}

	// Read and translate the integer
	b, err := r.readBytes(fn, ss.byteCount)
	if err != nil {
		return
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

	return
}
