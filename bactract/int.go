package bactract

import (
	"fmt"
)

// readInt reads the value for a 4 byte integer column
func readInt(r *tReader, tc TableColumn) (ec ExtractedColumn, err error) {

	fn := "readInt"
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
		err = fmt.Errorf("%s invalid byteCount (%d vs %d) for column %q", fn, defSz, ss.byteCount, tc.ColName)
		return
	}

	// Read and translate the integer
	b, err := r.readBytes(fn, ss.byteCount)
	if err != nil {
		return
	}

	var z int32
	for i, sb := range stripTrailingNulls(b) {
		z |= int32(sb) << uint(8*i)
	}

	ec.Str = fmt.Sprint(z)

	return
}
