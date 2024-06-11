package bactract

//16-byte GUID.
//uniqueidentifier

import (
	"fmt"
	"strings"
)

// readUniqueIdentifier reads the value for a 16 byte GUID (uniqueidentifier) column
func readUniqueIdentifier(r *tReader, tc TableColumn) (ec ExtractedColumn, err error) {

	fn := "readGUID"
	if debugFlag {
		debOut(fmt.Sprintf("Func %s", fn))
	}

	// Determine how many bytes to read
	defSz := 16

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

	// Read and translate the GUID
	var b []byte
	b, err = r.readBytes(fn, ss.byteCount)
	if err != nil {
		return
	}

	var s []string
	var z1 int64
	for i, sb := range b[0:4] {
		z1 |= int64(sb) << uint(8*i)
	}
	s = append(s, fmt.Sprintf("%08X", z1))

	var z2 int64
	for i, sb := range b[4:6] {
		z2 |= int64(sb) << uint(8*i)
	}
	s = append(s, fmt.Sprintf("%04X", z2))

	var z3 int64
	for i, sb := range b[6:8] {
		z3 |= int64(sb) << uint(8*i)
	}
	s = append(s, fmt.Sprintf("%04X", z3))

	var z4 []string
	for _, sb := range b[8:10] {
		z4 = append(z4, fmt.Sprintf("%02X", sb))
	}
	s = append(s, strings.Join(z4, ""))

	var z5 []string
	for _, sb := range b[10:] {
		z5 = append(z5, fmt.Sprintf("%02X", sb))
	}
	s = append(s, strings.Join(z5, ""))

	//ec.Str = strings.Join(s, "-")
	ec.Str = strings.Join(s, "")

	/*
	   https://bornsql.ca/blog/how-sql-server-stores-data-types-guid/

	   guid value: CC05E271-BACF-4472-901C-957568484405
	   stored as: 0x71E205CCCFBA7244901C957568484405

	   | Segment                | Value        | Bytes | Stored as      |
	   | ---------------------- | ------------ | ----- | -------------- |
	   | time-low               | CC05E271     | 4     | 0x71E205CC     |
	   | time-mid               | BACF         | 2     | 0xCFBA         |
	   | time-high-and-version  | 4472         | 2     | 0x7244         |
	   | clock-seq-and-reserved | 90           | 1     | 0x90           |
	   | clock-seq-low          | 1C           | 1     | 0x1C           |
	   | node                   | 957568484405 | 6     | 0x957568484405 |

	*/

	return
}
