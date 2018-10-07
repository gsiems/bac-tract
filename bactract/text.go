package bactract

// readText reads the value for a text column
func readText(r *tReader, tc TableColumn) (ec ExtractedColumn, err error) {

	debOut("Func readText")

	// Determine how many bytes to read
	ss, err := r.readStoredSize(tc, 4, 0)
	if err != nil {
		return ec, err
	}

	// Check for nulls
	ec.IsNull = ss.isNull
	if ss.isNull {
		return ec, err
	}

	// Read the chars
	b, err := r.readBytes("readText", ss.byteCount)
	if err != nil {
		return ec, err
	}

	ec.Str = string(toRunes(b))
	return ec, err
}
