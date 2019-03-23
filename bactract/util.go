package bactract

// "Utility" functions

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// toInt converts a byte array (string) of digits to its corresponding
// integer value
func toInt(b []byte) (ret int, err error) {
	ret, err = strconv.Atoi(string(b))
	if err != nil {

		var digits = map[string]int{
			"0": 0,
			"1": 1,
			"2": 2,
			"3": 3,
			"4": 4,
			"5": 5,
			"6": 6,
			"7": 7,
			"8": 8,
			"9": 9,
		}

		ret = 0
		for i := range b {
			x, ok := digits[string(b[i])]
			if !ok {
				return 0, errors.New("toInt(): Not an integer")
			}
			ret = (10 * ret) + x
		}
	}
	return ret, nil
}

// toRunes translates a [character] byte slice to the corresponding rune slice
func toRunes(b []byte) (ret []rune) {

	var j int
	ret = make([]rune, len(b)/2)
	for i := 0; i < len(b); i = i + 2 {
		z := int32(b[i])
		if b[i+1] != 0x00 {
			z |= int32(b[i+1]) << uint(8)
		}
		ret[j] = z
		j++
	}
	return ret
}

// stripTrailingNulls removes the null bytes from the end of a byte slice
func stripTrailingNulls(b []byte) []byte {

	i := len(b)
	if i > 0 {
		for {
			if i == 1 || int(b[i-1]) != 0x00 {
				break
			}
			i--
		}
		b = b[:i]
	}

	return b
}

func catDir(t []string) (dir string) {
	dir = strings.Join(t, string(os.PathSeparator))
	return dir
}

func debOut(msg string) {
	if debugFlag {
		fmt.Println(msg)
	}
}

func debHextOut(label string, bytes []byte) {
	if debugFlag {
		fmt.Printf("%s: ", label)
		if len(bytes) > debugLen && debugLen > 10 {

			for _, b := range bytes[0 : debugLen-6] {
				if int(b) == 0 {
					fmt.Print("0x00 ")
				} else if int(b) < 0x10 {
					fmt.Printf("0x0%x ", b)
				} else {
					fmt.Printf("0x%x ", b)
				}
			}
			fmt.Print(" ... ")
			for _, b := range bytes[len(bytes)-4:] {
				if int(b) == 0 {
					fmt.Print("0x00 ")
				} else if int(b) < 0x10 {
					fmt.Printf("0x0%x ", b)
				} else {
					fmt.Printf("0x%x ", b)
				}
			}

		} else {
			for _, b := range bytes {
				if int(b) == 0 {
					fmt.Print("0x00 ")
				} else if int(b) < 0x10 {
					fmt.Printf("0x0%x ", b)
				} else {
					fmt.Printf("0x%x ", b)
				}
			}
		}
		fmt.Println()
	}
}
