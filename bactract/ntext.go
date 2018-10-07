package bactract

// readNText reads the value for a varchar column
func readNText(r *tReader, tc TableColumn) (ec ExtractedColumn, err error) {

	debOut("Func readNText")

	// Determine how many bytes to read
	ss, err := r.readStoredSize(tc, 2, 0)
	if err != nil {
		return ec, err
	}

	// Check for nulls
	ec.IsNull = ss.isNull
	if ss.isNull {
		return ec, err
	}

	// Read the chars
	b, err := r.readBytes("readNText", ss.byteCount)
	if err != nil {
		return ec, err
	}

	ec.Str = string(toRunes(b))
	return ec, err
}
