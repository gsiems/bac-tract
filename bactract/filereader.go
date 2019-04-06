package bactract

import (
	"io"
	"os"
)

const (
	defaultBufSz = 4096
)

type buffFileReader struct {
	filenames []string // list of filenames for the table data
	fix       int      // the index of files entry that is currently open
	file      *os.File // the filehandle to read from
	buff      []byte   // the read buffer
	bix       int      // buff offset to start reading from
	bct       int      // count of bytes read into buff
	err       error
}

func (mr *buffFileReader) Read(p []byte) (n int, err error) {

	if mr.buff == nil {
		mr.buff = make([]byte, defaultBufSz)
	}

	pLen := len(p)
	if pLen == 0 {
		return
	}

	var needed int

	for {

		// If the buffer is empty and there is no more to read then return
		if mr.atEOF() {
			err = io.EOF
			break
		}

		// If there is anything in the buffer then copy as much as is
		// needed/available
		avail := mr.bct - mr.bix
		needed = pLen - n
		if avail > 0 && needed > 0 {
			if avail > needed {
				// read as much as is needed
				copy(p[n:], mr.buff[mr.bix:mr.bix+needed])

				// zero out the read bytes
				mr.clearBytes(needed)

				n += needed
				mr.bix += needed
			} else {
				// read what there is
				copy(p[n:], mr.buff[mr.bix:])

				// zero out the read bytes
				mr.clearBytes(mr.bct - mr.bix)

				n += avail
				mr.bix = mr.bct
			}
		}

		// if the buffer is empty then refill it
		mr.checkFillBuffer()
		if mr.hasErr() {
			break
		}

		// if p is full then return
		if n == pLen {
			break
		}
	}

	if mr.hasErr() {
		err = mr.err
	}
	return
}

func (mr *buffFileReader) clearBytes(count int) {
	for i := 0; i < count; i++ {
		mr.buff[i+mr.bix] = 0
	}
}

func (mr *buffFileReader) hasErr() (t bool) {
	if mr.err != nil && mr.err != io.EOF {
		return true
	}
	return false
}

func (mr *buffFileReader) atEOF() (t bool) {
	if mr.bct == 0 && mr.err == io.EOF {
		return true
	}
	return false
}

// checkFillBuffer checks and if needed re-fills the buffer.
func (mr *buffFileReader) checkFillBuffer() {
	if mr.bix >= mr.bct {

		var n int
		mr.bix = 0
		mr.bct = 0
		var err error

		if mr.file == nil {
			mr.openNext()
			if mr.err != nil {
				return
			}
		}

		// If the number of bytes requested exceeds the number of bytes
		// available in the current file then open the next file and
		// continue to read until either the buffer fills, there is an
		// error, or the end of the final file is reached.
		for mr.bct < len(mr.buff) {
			if mr.atEOF() {
				break
			}
			n, err = mr.file.Read(mr.buff[mr.bct:])
			if err != nil {
				if err == io.EOF {
					mr.openNext()
				} else {
					mr.err = err
				}
			}

			mr.bct += n
			if mr.err != nil {
				break
			}
		}
	}
}

func (mr *buffFileReader) openNext() {

    // ensure that the current file (if any) is closed
	if mr.file != nil {
		err := mr.file.Close()
		if err != nil {
			mr.err = err
			return
		}
		mr.fix += 1
	}

	// have we run out of files?
	if mr.fix >= len(mr.filenames) {
		mr.err = io.EOF
		return
	}

	// attempt to open the next file in the list
	f, err := os.Open(mr.filenames[mr.fix])
	if err == nil {
		mr.file = f
		return
	}

	// deal with any errors
	if err != io.EOF {
		mr.err = err
		return
	}

	// which leaves having opened a file to EOF. Is that possible?
	mr.openNext()
	return
}

func BuffFileReader(sz int, filenames []string) *buffFileReader {

	var mr buffFileReader

	mr.filenames = filenames
	if sz <= 0 {
		mr.buff = make([]byte, defaultBufSz)
	} else {
		mr.buff = make([]byte, sz)
	}
	return &mr
}
