package bactract

import "fmt"

// readVarbinary reads the value for a varchar column
func readVarbinary(r *tReader, tc TableColumn) (ec ExtractedColumn, err error) {

	fn := "readVarbinary"
	debOut(fmt.Sprintf("Func %s", fn))

	// Determine how many bytes to read
	ss, err := r.readStoredSize(tc, 8, 0)
	if err != nil {
		return
	}

	// Check for nulls
	ec.IsNull = ss.isNull
	if ss.isNull {
		return
	}

	// Read and translate the varbinary
	// TODO
	b, err := r.readBytes(fn, ss.byteCount)
	if err != nil {
		return
	}

	ec.Str = string(b)
	return
}
