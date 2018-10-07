package bactract

// readChar reads the value for a character column
func readChar(r *tReader, tc TableColumn) (ec ExtractedColumn, err error) {

	debOut("Func readChar")

	// Determine how many bytes to read
	ss, err := r.readStoredSize(tc, 2, 2*tc.Length)
	if err != nil {
		return ec, err
	}

	// Check for nulls
	ec.IsNull = ss.isNull
	if ss.isNull {
		return ec, err
	}

	// Read the chars
	b, err := r.readBytes("readChar", ss.byteCount)
	if err != nil {
		return ec, err
	}

	ec.Str = string(toRunes(b))
	return ec, err
}
