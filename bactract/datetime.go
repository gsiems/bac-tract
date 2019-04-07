package bactract

import (
	"fmt"
	"time"
)

// readDatetime reads the value for a datetime column.
//
// Note 1. A datetime is stored as two integers (one for the date and
// one for the time) that appear to be offsets from 1900-01-01 00:00:00.
//
// Note 2. The value for the time potion of the datetime appears to be
// the seconds since midnight multiplied by 300 (5 minutes). I don't know
// why.
//
// Note 3. Go does not account for leap seconds when dong datetime
// calculations. Whether, or how much of an issue this is unknown--
// especially as I don't know if MS SQL-Sever accounts for leap seconds either.
func readDatetime(r *tReader, tc TableColumn) (ec ExtractedColumn, err error) {

	fn := "readDatetime"
	defSz := 8
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

	// Read the datetime
	if ss.byteCount > 0 {

		var b []byte
		b, err = r.readBytes(fn, ss.byteCount)
		if err != nil {
			return
		}

		var days int32
		for i, sb := range stripTrailingNulls(b[:4]) {
			days |= int32(sb) << uint(8*i)
		}

		var s int32
		for i, sb := range stripTrailingNulls(b[4:8]) {
			s |= int32(sb) << uint(8*i)
		}

		start := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
		d := start.AddDate(0, 0, int(days))
		m, _ := time.ParseDuration(fmt.Sprintf("%ds", s/300))
		dt := d.Add(m)

		ec.Str = dt.Format("2006-01-02 15:04:05")
	}

	return
}
