package bactract

import (
	"fmt"
)

// readMoney reads the value for a small money column
func readMoney(r *tReader, tc TableColumn) (ec ExtractedColumn, err error) {

	// NB: Internally stored as a big integer
	// Range from -922,337,203,685,477.5808 (-922,337 trillion) to 922,337,203,685,477.5807 (922,337 trillion).

	fn := "readMoney"
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

	// Assert: If not null then the stored size is the default
	if ss.byteCount != defSz {
		err = fmt.Errorf("%s byteCount too large for column %q (%d vs %d)", fn, tc.ColName, ss.byteCount, defSz)
		return
	}

	// Read and translate the integer
	var b []byte
	b, err = r.readBytes(fn, ss.byteCount)
	if err != nil {
		return
	}

	var z int64
	for i, sb := range stripTrailingNulls(b) {
		z |= int64(sb) << uint(8*i)
	}

	// Adjust the decimal point
	sb := []byte(fmt.Sprint(z))
	j := len(sb) - 4
	if j > 0 {
		ec.Str = fmt.Sprintf("%s.%s", string(sb[0:j]), string(sb[j:]))
	} else if j == 0 {
		ec.Str = fmt.Sprintf("0.%s", string(sb))
	}

	return
}
