package bactract

import (
	"fmt"
)

// readString reads the value for a string {char, text, varchar} column
func readString(r *tReader, tc TableColumn) (ec ExtractedColumn, err error) {

	fn := "readString"
	debOut(fmt.Sprintf("Func %s", fn))

	// varchar : 2, 0
	// char : 2, length*2
	// text : 4, 0

	defSz := 0
	sz := 2

	if tc.DataType == Text {
		sz = 4
	} else if tc.DataType == Char {
		defSz = tc.Length * 2
	}

	// Determine how many bytes to read
	ss, err := r.readStoredSize(tc, sz, defSz)
	if err != nil {
		return
	}

	// Check for nulls
	ec.IsNull = ss.isNull
	if ss.isNull {
		return
	}

	// Check the stored size vs. the column size
	if tc.DataType == Char || tc.DataType == Varchar {
		if tc.Length > 0 && ss.byteCount > tc.Length*2 {
			err = fmt.Errorf("%s byteCount too large for column %q (%d vs %d)", fn, tc.ColName, ss.byteCount, tc.Length*2)
			return
		}
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
