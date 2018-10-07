package bactract

import (
	"fmt"
	"strings"
	"time"
)

// readDatetime2 reads the value for a datetime column.
func readDatetime2(r *tReader, tc TableColumn) (ec ExtractedColumn, err error) {

	debOut("Func readDatetime2")

	// Determine how many bytes to read
	var ss storedSize
	ss, err = r.readStoredSize(tc, 1, 8)
	if err != nil {
		return ec, err
	}

	// Check for nulls
	ec.IsNull = ss.isNull
	if ss.isNull {
		return ec, err
	}

	// Read the datetime
	if ss.byteCount > 0 {

		dateSize := 3
		timeSize := ss.byteCount - dateSize

		var s, y []byte
		s, err = r.readBytes("readDatetime2: timeBytes", timeSize)
		if err != nil {
			return ec, err
		}

		y, err = r.readBytes("readDatetime2: dateBytes", dateSize)
		if err != nil {
			return ec, err
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
		duration, err = calcDuration(tc.Scale, ticks)
		if err != nil {
			return ec, err
		}

		m, _ := time.ParseDuration(duration)
		dt := d.Add(m)

		ec.Str = dt.Format(calcFormat(tc.Scale, ticks))
	}

	return ec, err
}

func calcDuration(scale int, ticks uint64) (pds string, err error) {

	var m = map[int]int{
		0: 1,
		1: 100,
		2: 10,
		3: 1,
		4: 100,
		5: 10,
		6: 1,
		7: 100,
	}

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

	mult, ok := m[scale]
	if !ok {
		err = fmt.Errorf("Could not determine multiplier for datetime2 time duration. Unknown scale (%d)", scale)
		return pds, err
	}

	units, ok := u[scale]
	if !ok {
		err = fmt.Errorf("Could not determine units for datetime2 time duration. Unknown scale (%d)", scale)
		return pds, err
	}

	pd := ticks * uint64(mult)

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

func calcFormat(scale int, ticks uint64) (dtf string) {

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
