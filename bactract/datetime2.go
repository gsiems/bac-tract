package bactract

import (
	"fmt"
	"strings"
	"time"
)

// readDatetime2 reads the value for a datetime column.
func readDatetime2(r *tReader, tc TableColumn) (ec ExtractedColumn, err error) {

	fn := "readDatetime2"
	defSz := 8
	if debugFlag {
		debOut(fmt.Sprintf("Func %s", fn))
	}

	// Determine how many bytes to read
	var ss storedSize
	ss, err = r.readStoredSize(tc, 1, defSz)
	if err != nil {
		return
	}

	// Check for nulls
	if ss.isNull {
		ec.IsNull = ss.isNull
		return
	}

	// Read the datetime
	if ss.byteCount > 0 {

		dateSize := 3
		timeSize := ss.byteCount - dateSize

		var s, y []byte
		s, err = r.readBytes(fmt.Sprintf("%s: timeBytes", fn), timeSize)
		if err != nil {
			return
		}

		y, err = r.readBytes(fmt.Sprintf("%s: dateBytes", fn), dateSize)
		if err != nil {
			return
		}

		var ticks uint64
		for i, sb := range stripTrailingNulls(s) {
			ticks |= uint64(sb) << uint(8*i)
		}

		var days int
		for i, sb := range stripTrailingNulls(y) {
			days |= int(sb) << uint(8*i)
		}

		start := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
		d := start.AddDate(0, 0, days)

		// Add the time portion
		var duration string
		duration, err = calcTimeDuration(tc.Scale, ticks)
		if err != nil {
			return
		}

		m, _ := time.ParseDuration(duration)
		dt := d.Add(m)

		ec.Str = dt.Format(calcDatetimeFormat(tc.Scale, ticks))
	}

	return
}

// readDate reads the value for a date column.
//
// This is the same as the date portion of datetime2
func readDate(r *tReader, tc TableColumn) (ec ExtractedColumn, err error) {

	fn := "readDate"
	defSz := 3
	if debugFlag {
		debOut(fmt.Sprintf("Func %s", fn))
	}

	// Determine how many bytes to read
	var ss storedSize
	ss, err = r.readStoredSize(tc, 1, defSz)
	if err != nil {
		return
	}

	// Check for nulls
	if ss.isNull {
		ec.IsNull = ss.isNull
		return
	}

	// Assert: If not null then the stored size is the default
	if ss.byteCount != defSz {
		err = fmt.Errorf("%s byteCount too large for column %q (%d vs %d)", fn, tc.ColName, ss.byteCount, defSz)
		return
	}

	// Read the date
	if ss.byteCount > 0 {

		var b []byte
		b, err = r.readBytes(fn, ss.byteCount)
		if err != nil {
			return
		}

		var days int32
		for i, sb := range stripTrailingNulls(b) {
			days |= int32(sb) << uint(8*i)
		}

		start := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
		d := start.AddDate(0, 0, int(days))

		ec.Str = d.Format("2006-01-02")
	}

	return
}

// readTime reads the value for a time column.
//
// This is the same as the time portion of datetime2
func readTime(r *tReader, tc TableColumn) (ec ExtractedColumn, err error) {

	fn := "readTime"
	defSz := 5
	if debugFlag {
		debOut(fmt.Sprintf("Func %s", fn))
	}

	// Determine how many bytes to read
	var ss storedSize
	ss, err = r.readStoredSize(tc, 1, defSz)
	if err != nil {
		return
	}

	// Check for nulls
	if ss.isNull {
		ec.IsNull = ss.isNull
		return
	}

	// Read the time
	if ss.byteCount > 0 {

		var s []byte
		s, err = r.readBytes(fn, ss.byteCount)
		if err != nil {
			return
		}

		var ticks uint64
		for i, sb := range stripTrailingNulls(s) {
			ticks |= uint64(sb) << uint(8*i)
		}

		d := time.Date(1901, 1, 1, 0, 0, 0, 0, time.UTC)

		// Add the time
		var duration string
		duration, err = calcTimeDuration(tc.Scale, ticks)
		if err != nil {
			return
		}

		m, _ := time.ParseDuration(duration)
		t := d.Add(m)

		ec.Str = t.Format(calcTimeFormat(tc.Scale, ticks))
	}

	return
}

func calcDatetimeFormat(scale int, ticks uint64) (dtf string) {

	var ns []string
	ns = append(ns, "2006-01-02 15:04:05")
	if scale > 0 {
		ns = append(ns, ".")
		for i := 0; i < scale; i++ {
			ns = append(ns, "0")
		}
	}
	dtf = strings.Join(ns, "")

	return dtf
}

func calcTimeFormat(scale int, ticks uint64) (tf string) {

	var ns []string
	ns = append(ns, "15:04:05")
	if scale > 0 {
		ns = append(ns, ".")
		for i := 0; i < scale; i++ {
			ns = append(ns, "0")
		}
	}
	tf = strings.Join(ns, "")

	return tf
}

func calcTimeDuration(scale int, ticks uint64) (pds string, err error) {

	var u = map[int]string{
		0: "s",
		1: "ms",
		2: "ms",
		3: "ms",
		4: "us",
		5: "us",
		6: "us",
		7: "ns",
	}

	units, ok := u[scale]
	if !ok {
		err = fmt.Errorf("Could not determine units for datetime2 time duration. Unknown scale (%d)", scale)
		return pds, err
	}

	var pd uint64

	switch scale {
	case 0:
		pd = ticks / 1000000
	case 1:
		pd = ticks / 100000
	case 2:
		pd = ticks / 10000
	case 3:
		pd = ticks / 1000
	case 4:
		pd = ticks / 100
	case 5:
		pd = ticks / 10
	case 6:
		pd = ticks
	case 7:
		pd = ticks * 100
	}

	// 0 -> ticks * 1 s
	// 1 -> ticks * 100 ms
	// 2 -> ticks * 10 ms
	// 3 -> ticks * 1 ms
	// 4 -> ticks * 100 us
	// 5 -> ticks * 10 us
	// 6 -> ticks * 1 us
	// 7 -> ticks * 100 ns

	pds = fmt.Sprintf("%d%s", pd, units)
	return pds, err
}
