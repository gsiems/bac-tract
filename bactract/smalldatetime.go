package bactract

import (
	"fmt"
	"time"
)

// readSmallDatetime reads the value for a small-datetime column.
//
// Note 1. A smalldatetime appears to be stored as two int16 values,
// one for days since 1900-01-01 and the other for minutes since midnight
func readSmallDatetime(r *tReader, tc TableColumn) (ec ExtractedColumn, err error) {

	fn := "readSmallDatetime"
	defSz := 4
	debOut(fmt.Sprintf("Func %s", fn))

	// Determine how many bytes to read
	var ss storedSize
	ss, err = r.readStoredSize(tc, 1, defSz)
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

	if ss.byteCount > 0 {

		var s, y []byte

		y, err = r.readBytes(fmt.Sprintf("%s: dateBytes", fn), 2)
		if err != nil {
			return
		}

		s, err = r.readBytes(fmt.Sprintf("%s: timeBytes", fn), 2)
		if err != nil {
			return
		}

		var mins int
		for i, sb := range stripTrailingNulls(s) {
			mins |= int(sb) << uint(8*i)
		}

		var days int
		for i, sb := range stripTrailingNulls(y) {
			days |= int(sb) << uint(8*i)
		}

		start := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
		d := start.AddDate(0, 0, days)

		// Add the time portion
		m, _ := time.ParseDuration(fmt.Sprintf("%dm", mins))
		dt := d.Add(m)

		ec.Str = dt.Format("2006-01-02 15:04:05")

	}

	return
}
