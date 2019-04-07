package bactract

import (
	"fmt"
)

// readNVarchar reads the value for a varchar column
func readNVarchar(r *tReader, tc TableColumn) (ec ExtractedColumn, err error) {

	fn := "readNVarchar"
	debOut(fmt.Sprintf("Func %s", fn))

	// Determine how many bytes to read
	ss, err := r.readStoredSize(tc, 2, 0)
	if err != nil {
		return
	}

	// Check for nulls
	ec.IsNull = ss.isNull
	if ss.isNull {
		return
	}

	// Check the stored size vs. the column size
	if ss.byteCount > tc.Length*2 {
		err = fmt.Errorf("%s byteCount too large for column %q (%d vs %d)", fn, tc.ColName, ss.byteCount, tc.Length*2)
		return
	}

	// Assert: The stored size is an even number of bytes?
	if ss.byteCount%2 != 0 {
		err = fmt.Errorf("%s invalid byteCount (%d) for column %q", fn, ss.byteCount, tc.ColName)
		return
	}

	// Read the chars
	b, err := r.readBytes(fn, ss.byteCount)
	if err != nil {
		return
	}

	ec.Str = string(toRunes(b))
	return
}
