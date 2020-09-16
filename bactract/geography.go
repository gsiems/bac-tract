package bactract

import (
	"encoding/hex"
	"fmt"
	"math"
)

// readGeography reads the value for a varchar column
func readGeography(r *tReader, tc TableColumn) (ec ExtractedColumn, err error) {

	fn := "readGeography"
	if debugFlag {
		debOut(fmt.Sprintf("Func %s", fn))
	}

	// Determine how many bytes to read
	ss, err := r.readStoredSize(tc, 8, 0)
	if err != nil {
		return
	}

	// Check for nulls
	if ss.isNull {
		ec.IsNull = ss.isNull
		return
	}

	// Read and translate the geography
	/* TODO: determine the actual format and translate to well known text (WKT) or something similar
	This could be:
	 - point:       Point ( Lat, Long, SRID )
	 - linestring:  Linestring ( Lat, Long, Lat, Long ), SRID
	 - polygon:     Polygon ( Lat, Long, Lat, Long, Lat, Long, ...), SRID

	The first 2 (possibly 4) bytes appear to be the SRID
	The 5th byte is the geometry type? Could this be the same as the WKT geometry integer code

	https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry

	Geometry types, and WKB integer codes
	| Type               | 2D   | Z    | M    | ZM   |
	| ------------------ | ---- | ---- | ---- | ---- |
	| Geometry           |    0 | 1000 | 2000 | 3000 |
	| Point              |    1 | 1001 | 2001 | 3001 |
	| LineString         |    2 | 1002 | 2002 | 3002 |
	| Polygon            |    3 | 1003 | 2003 | 3003 |
	| MultiPoint         |    4 | 1004 | 2004 | 3004 |
	| MultiLineString    |    5 | 1005 | 2005 | 3005 |
	| MultiPolygon       |    6 | 1006 | 2006 | 3006 |
	| GeometryCollection |    7 | 1007 | 2007 | 3007 |
	| CircularString     |    8 | 1008 | 2008 | 3008 |
	| CompoundCurve      |    9 | 1009 | 2009 | 3009 |
	| CurvePolygon       |   10 | 1010 | 2010 | 3010 |
	| MultiCurve         |   11 | 1011 | 2011 | 3011 |
	| MultiSurface       |   12 | 1012 | 2012 | 3012 |
	| Curve              |   13 | 1013 | 2013 | 3013 |
	| Surface            |   14 | 1014 | 2014 | 3014 |
	| PolyhedralSurface  |   15 | 1015 | 2015 | 3015 |
	| TIN                |   16 | 1016 | 2016 | 3016 |
	| Triangle           |   17 | 1017 | 2017 | 3017 |
	| Circle             |   18 | 1018 | 2018 | 3018 |
	| GeodesicString     |   19 | 1019 | 2019 | 3019 |
	| EllipticalCurve    |   20 | 1020 | 2020 | 3020 |
	| NurbsCurve         |   21 | 1021 | 2021 | 3021 |
	| Clothoid           |   22 | 1022 | 2022 | 3022 |
	| SpiralCurve        |   23 | 1023 | 2023 | 3023 |
	| CompoundSurface    |   24 | 1024 | 2024 | 3024 |
	| BrepSolid          |      | 1025 |      |      |
	| AffinePlacement    |  102 | 1102 |      |      |

	*/

	b, err := r.readBytes(fn, ss.byteCount)
	if err != nil {
		return
	}

	var srid int32
	if ss.byteCount > 4 {
		for i, sb := range stripTrailingNulls(b[0:4]) {
			srid |= int32(sb) << uint(8*i)
		}
	}

	// Currently only have data for points...
	switch {
	case ss.byteCount == 22:

		lat := tcord(b[6:14])
		long := tcord(b[14:])

		ec.Str = fmt.Sprintf("SRID=%d;POINT(%f %f)", srid, long, lat)

	case ss.ByteCount > 22:

		// case ( ss.ByteCount - 6 ) % 16 == 0: // list of points ?

		ec.Str = fmt.Sprintf("SRID=%d;(%s)", srid, hex.EncodeToString(b[6:]))

	}

	return
}

func tcord(b []byte) float64 {

	var z uint64
	for i := 0; i < 8; i++ {
		z |= uint64(b[i]) << uint(8*i)
	}

	f := math.Float64frombits(z)

	return f
}
