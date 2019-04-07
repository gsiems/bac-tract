package bactract

import (
	"fmt"
)

// readBigInt reads the value for an 8 byte integer column
func readBigInt(r *tReader, tc TableColumn) (ec ExtractedColumn, err error) {

	fn := "readBigInt"
	defSz := 8
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

	var z int64
	for i, sb := range stripTrailingNulls(b) {
		z |= int64(sb) << uint(8*i)
	}

	ec.Str = fmt.Sprint(z)

	return
}
