package bactract

import "fmt"

// readGeography reads the value for a varchar column
func readGeography(r *tReader, tc TableColumn) (ec ExtractedColumn, err error) {

	fn := "readGeography"
	if debugFlag {
		debOut(fmt.Sprintf("Func %s", fn))
	}

	// Determine how many bytes to read
	ss, err := r.readStoredSize(tc, 8, 0)
	if err != nil {
		return
	}

	// Check for nulls
	if ss.isNull {
		ec.IsNull = ss.isNull
		return
	}

	// Read and translate the geography
	/* TODO: determine the actual format and translate to well known text (WKT)
	This could be:
	 - point:       Point ( Lat, Long, SRID )
	 - linestring:  Linestring ( Lat, Long, Lat, Long ), SRID
	 - polygon:     Polygon ( Lat, Long, Lat, Long, Lat, Long, ...), SRID

	The first 2 (possibly 4) bytes appear to be the SRID
	The 5th byte is the geometry type?

	*/

	b, err := r.readBytes(fn, ss.byteCount)
	if err != nil {
		return
	}

	ec.Str = string(b)
	return
}
