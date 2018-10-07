package bactract

// readVarbinary reads the value for a varchar column
func readVarbinary(r *tReader, tc TableColumn) (ec ExtractedColumn, err error) {

	debOut("Func readVarbinary")

	// Determine how many bytes to read
	ss, err := r.readStoredSize(tc, 8, 0)
	if err != nil {
		return ec, err
	}

	// Check for nulls
	ec.IsNull = ss.isNull
	if ss.isNull {
		return ec, err
	}

	// Read and translate the varbinary
	// TODO
	b, err := r.readBytes("readVarbinary", ss.byteCount)
	if err != nil {
		return ec, err
	}

	ec.Str = string(b)
	return ec, err
}
