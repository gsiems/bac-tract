package bactract

import (
	"fmt"
)

// readString reads the value for a string {char, text, varchar} column
func readString(r *tReader, tc TableColumn) (ec ExtractedColumn, err error) {

	fn := "readString"
	if debugFlag {
		debOut(fmt.Sprintf("Func %s", fn))
	}

	defSz := 0
	sz := 2

	switch tc.DataType {
	case Text:
		sz = 4
	case Char:
		defSz = tc.Length * 2
	case Varchar:
		// If the size is not specified then it appears to be up to maxsize?
		// Seems to require 8 bytes to store the size...
		if tc.Length == 0 {
			sz = 8
		}
	}

	// Determine how many bytes to read
	ss, err := r.readStoredSize(tc, sz, defSz)
	if err != nil {
		return
	}

	// Check for nulls
	if ss.isNull {
		ec.IsNull = ss.isNull
		return
	}

	// Check the stored size vs. the column size
	if tc.DataType == Char || tc.DataType == Varchar {
		if tc.Length > 0 && ss.byteCount > tc.Length*2 {
			err = fmt.Errorf("%s byteCount too large for column %q (%d vs %d)", fn, tc.ColName, ss.byteCount, tc.Length*2)
			return
		}
	}

	// Assert: The stored size is an even number of bytes?
	if ss.byteCount%2 != 0 {
		err = fmt.Errorf("%s invalid byteCount (%d) for column %q", fn, ss.byteCount, tc.ColName)
		return
	}

	// Read the chars
	b, err := r.readBytes(fn, ss.byteCount)
	if err != nil {
		return
	}

	// HACK: having seen at least one case of a not null char column
	// having size bytes anyhow. When this happens, the int of the first
	// two "data bytes" would match the column size since that is what
	// they really are. If the column length is under 9 then it's
	// probably safe to assume that this is a case of "not null char
	// with size bytes". 9 bytes because the first printable character
	// is the tab -- chr (9)
	if tc.DataType == Char && !tc.IsNullable && ss.byteCount < 18 {
		var z int16
		for i, sb := range stripTrailingNulls(b[0:2]) {
			z |= int16(sb) << uint(8*i)
		}
		if int(z) == len(b) {
			nextb, cerr := r.readBytes(fn, 2)
			if cerr != nil {
				err = cerr
				return
			}
			b = append(b[2:], nextb...)
		}
	}

	// HACK: if the column is not null and the leading byte is 0x00 then
	// we need to unshift and read additional bytes until the first byte
	// is no longer 0x00. This is to attempt to deal with those (so far
	// few) tables that insert an extra '0x00 0x00 0x00 0x00 0x00 0x00'
	// before the actual data of certain columns.
	if len(b) > 1 && b[0] == 0x00 {
		for {
			if b[0] != 0x00 {
				break
			}
			nextb, cerr := r.readBytes(fn, 1)
			if err != nil {
				err = cerr
				return
			}
			b = append(b[1:], nextb...)
		}
	}

	ec.Str = string(toRunes(b))
	return
}
