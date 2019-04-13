package bactract

import (
	"fmt"
)

// readBit reads the value for a 1 byte integer column
func readBit(r *tReader, tc TableColumn) (ec ExtractedColumn, err error) {

	fn := "readBit"
	defSz := 1
	if debugFlag {
		debOut(fmt.Sprintf("Func %s", fn))
	}

	// Determine how many bytes to read
	ss, err := r.readStoredSize(tc, 1, defSz)
	if err != nil {
		return
	}

	// Check for nulls
	if ss.isNull {
		ec.IsNull = ss.isNull
		return
	}

	// Assert: If not null then the stored size is the default
	if ss.byteCount != defSz {
		err = fmt.Errorf("%s invalid byteCount (%d vs %d) for column %q", fn, defSz, ss.byteCount, tc.ColName)
		return
	}

	// Read and translate the integer
	b, err := r.readBytes(fn, ss.byteCount)
	if err != nil {
		return
	}

	ec.Str = fmt.Sprint(b[0])

	return
}
