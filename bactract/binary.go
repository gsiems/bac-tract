package bactract

import "fmt"

// readBinary reads the value for a varchar column
func readBinary(r *tReader, tc TableColumn) (ec ExtractedColumn, err error) {

	fn := "readBinary"
	debOut(fmt.Sprintf("Func %s", fn))

	// Determine how many bytes to read
	ss, err := r.readStoredSize(tc, 2, tc.Length)
	if err != nil {
		return
	}

	// Check for nulls
	ec.IsNull = ss.isNull
	if ss.isNull {
		return
	}

	// Check the stored size vs. the column size
	if ss.byteCount > tc.Length {
		err = fmt.Errorf("%s byteCount too large for column %q (%d vs %d)", fn, tc.ColName, ss.byteCount, tc.Length)
		return
	}

	var b []byte
	if ss.byteCount > 0 {
		// Read and translate the binary
		// TODO
		b, err = r.readBytes(fn, ss.byteCount)
		if err != nil {
			return
		}

		ec.Str = string(b)
	}
	return
}
