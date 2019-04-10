package bactract

import (
	"fmt"
)

// readInteger reads the value for an integer {int, biging, smallint, tinyint} column
func readInteger(r *tReader, tc TableColumn) (ec ExtractedColumn, err error) {

	fn := "readInteger"
	debOut(fmt.Sprintf("Func %s", fn))

	// tinyint : 1, 1
	// smallint : 1, 2
	// int : 1, 4
	// bigint : 1, 8

	defSz := 4

	if tc.DataType == Int {
		defSz = 4
	} else if tc.DataType == BigInt {
		defSz = 8
	} else if tc.DataType == SmallInt {
		defSz = 2
	} else if tc.DataType == TinyInt {
		defSz = 1
	}

	// Determine how many bytes to read
	ss, err := r.readStoredSize(tc, 1, defSz)
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
		err = fmt.Errorf("%s byteCount too large for column %q (%d vs %d)", fn, tc.ColName, ss.byteCount, defSz)
		return
	}

	// Read and translate the integer
	b, err := r.readBytes(fn, ss.byteCount)
	if err != nil {
		return
	}

	if tc.DataType == Int {
		var z int32
		for i, sb := range stripTrailingNulls(b) {
			z |= int32(sb) << uint(8*i)
		}
		ec.Str = fmt.Sprint(z)
		return
	}

	if tc.DataType == BigInt {
		var z int64
		for i, sb := range stripTrailingNulls(b) {
			z |= int64(sb) << uint(8*i)
		}
		ec.Str = fmt.Sprint(z)
		return
	}

	if tc.DataType == SmallInt {
		var z int16
		for i, sb := range stripTrailingNulls(b) {
			z |= int16(sb) << uint(8*i)
		}
		ec.Str = fmt.Sprint(z)
		return
	}

	if tc.DataType == TinyInt {
		ec.Str = fmt.Sprint(int8(b[0]))
	}

	return
}
