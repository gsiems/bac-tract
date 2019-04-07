package bactract

import (
	"fmt"
)

// readTinyInt reads the value for a 1 byte integer column
func readTinyInt(r *tReader, tc TableColumn) (ec ExtractedColumn, err error) {

	fn := "readTinyInt"
	defSz := 1
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

	z := int8(b[0])

	ec.Str = fmt.Sprint(z)

	return
}
