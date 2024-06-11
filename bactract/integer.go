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

	switch tc.DataType {
	case BigInt:
		defSz = 8
	case Int:
		defSz = 4
	case SmallInt:
		defSz = 2
	case TinyInt:
		defSz = 1
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

	// WOULD BE HACK: if the column is defined as not null and the leading byte
	// is 0xff then we need might to unshift and read additional bytes
	// until the first byte is no longer 0xff. This is to attempt to deal
	// with those (so far few) tables that insert an extra '0xff 0xff 0xff
	// 0xff 0xff 0xff' into each? record. This is similar to the "prepended
	// null bytes in [var]char data" but, so far, harder to work around/fix.
	//
	// Something kinda like the following could almost work except that
	// it suffers false positives and results in breaking far more than
	// it fixes. To actually work there needs to be more to the pattern
	// for identifying the inserted bytes.
	//
	// Actually, if the offending columns are hard-coded (only do the
	// following for specific columns) so as to avoid the false positives,
	// then the following appears to work quite well... We don't want to
	// have to do that though.
	//
	// While probably not fool-proof one option would be to process a
	// table like normal and, if/when an error occurs, to track back the
	// columns to the preceeding not-null integer column, then re-run the
	// table from the start (hard to backup the byte stream after all)
	// and call this func with the "assumed to be offending" column name.
	// To validate the goodness of fit for the "fix" might require
	// tracking the original row/column that failed to determine, in case
	// the tables still fails to fully parse, whether or not invoking the
	// workaround for the "assumed to be offending" column made things
	// better (failed later in the parse), worse (fails sooner), or
	// made no difference (the failure point/mode does not change).
	// Seems kinda hacky.

	if tc.IsAdulterated {
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
