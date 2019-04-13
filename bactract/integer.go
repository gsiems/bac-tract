package bactract

import (
	"fmt"
)

// readInteger reads the value for an integer {int, biging, smallint, tinyint} column
func readInteger(r *tReader, tc TableColumn) (ec ExtractedColumn, err error) {

	fn := "readInteger"
	if debugFlag {
		debOut(fmt.Sprintf("Func %s", fn))
	}

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
	b, err := r.readBytes(fn, ss.byteCount)
	if err != nil {
		return
	}

	// WOULD BE HACK: if the column is defined as not null and the leading byte
	// is 0xff then we need might to unshift and read additional bytes
	// until the first byte is no longer 0xff. This is to attempt to deal
	// with those (so far few) tables that insert an extra '0xff 0xff 0xff
	// 0xff 0xff 0xff' into each? record. This is similar to the "prepended
	// null bytes in [var]char data" but, so far, harder to work around/fix.
	//
	// Something kinda like the following could almost work except that
	// it suffers false positives and results in breaking far more than
	// it fixes. To actually work ther needs to be more to the pattern
	// for identifying the inserted bytes.
	/*
		if tc.DataType == Int && !tc.IsNullable && b[0] == 0xff {
			isnull := true
			for i := 0; i < len(b); i++ {
				if b[i] != 0xff {
					isnull = false
					break
				}
			}

			if isnull {
				x, cerr := r.readBytes("read_0xff", 6)
				if cerr != nil {
					err = cerr
					return
				}
				if len(b) == 6 {
					b = x[2:]
				}
			}
		}
		//*/

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
