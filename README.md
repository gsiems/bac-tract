# bac-tract

Extract data from MS SQL-Server bacpac files (BACpac-exTRACT).

A bacpac file is a means of getting data out of Azure MS SQL-Server instances.

NB a bacpac file is simply a zip archive of other files and that the
files of interest are the model.xml file and the exported data files
under the Data directory.

The commands/tools available consist of:

* bp2col: Extracts column metadata for one or more tables from an unzipped bacpac file

* bp2csv: Extracts one or more tables from an unzipped bacpac file and writes the output to comma-separated file(s)

* bp2ddl: Generates table creation DDL for one or more tables from an unzipped bacpac file

* bp2ora: Extracts one or more tables from an unzipped bacpac file and writes the output to Oracle SQL*Loader control and data files

* bp2pg: Extracts one or more tables from an unzipped bacpac file and writes the output to pg_dump file(s)

NB that these tools all require that the bacpac file has already been unzipped.


Common flags used by the tools are:

```

    -b Base directory containing the unzipped bacpac file.

    -c The number of rows of data to extract per table (bp2csv, bp2ora,
        bp2pg). Defaults to extracting all rows of data.

    -d The SQL dialect to output (bp2ddl). Valid dialects are
        Ora (Oracle), Pg (Postresql), and Std (Standard).

    -e The column meta-data exceptions file to use (should there be a need).

    -f The file to read that contains the names of the tables to
        extract (the tables are listed one per line).

    -t The name of the table to extract.

    -w The number of parallel workers to use (bp2ora only) for
        extracting the data.

    -debug Write debugging information to STDOUT (bp2csv, bp2ora, bp2pg).

```

# Column meta-data exceptions

There are sometimes issues when extracting the data from the bacpac due
to the data being stored slightly differently from what the data model
indicates. When this happens the extraction will crash shortly after
encountering one of the anomolies. In that it is unknown how to predict
where the anomolies will be found there is support for overriding the
model meta-data to help the data extraction do the right thing.

Examples of the issues seen so far

 1. The first issue is of a not-null char column not parsing the same
 as all the other not-null char columns. When a char column is defined
 as not nullable then the typical behavior is to not insert the "size
 bytes" data as it is not needed. However, in one of the tables tested
 a a not-null char column also has size bytes data. The code attempts
 to, and (so far) mostly succeeds, in mitigating this behavior without
 needing to use any meta-data exceptions.

 2. The second issue involves the datafile sometimes having six null
 (0x00) bytes inserted between the size bytes and data bytes of varchar
 columns. Since no string data should start with null bytes this issue
 appears to have a straight-forward mmitigation. This behavior has been
 observed in two to three of the 200+ tables tested. Fortunately, this
 issue is managed by the data extraction code without need for using
 meta-data exceptions.

 3. The third issue appears to involve inserting a set of six 0xff
 bytes before not-null integer columns. This behavior has been observed
 in 3 of the 200+ tables tested. Programatically identifying and
 mitigating this issue has, so far, proved more difficult than the
 first two issues.

 4. The forth issue found is similar to the first in that a not-null
 column has size bytes-- in this case it is the bit datatype.

An example exceptions file is included under the cmd directory (see
colExceptionsExample.json). The file consists of a JSON array of one or
more exceptions, one exception per problematic column. While most of
the elements in the file should be self explanatory the isAdulterated
element is not-- this element is currently only used for indicating
those integer columns that exhibit the behavior in issue three above.

It should be noted that the tested bacpac files apparently do load
correctly into MS SQL-Server such that these issues aren't visible to
MS SQL-Server environments. Whether this is due to buggy behavior in
the bacpac exporting code that the bacpac importing code is able work
around, or whether this is intentional (anti-competitive?) behavior I
cannot say although MS history impies that it could be either or both.

Running the command with the debug flag set can be used to assist in
troubleshooting these anomolies if/when they occur where the
information is written to STDOUT and is very verbose. As each column of
data is read the column meta-data, the steps taken in parsing it, and a
synopsis of the read data is output. The following example shows the
results of parsing two columns from one row of data. Each column is
separated by a blank line. The first line of output contains the column
meta-data (columnName, dataType, Length, Precision, Scale, and
isNullable). The followup lines detail the function used to read the
data, reading of storage bytes (nullable columns only), reading of data
bytes, a synopsis of the data (hex-dump and plain text) extracted, and
whether the column was null or not.

```
"column_name" int 0, 0, 0, true
Func readInteger
readStoredSize: Attempting to read 1 bytes
Bytes: 0x04
readInteger: Attempting to read 4 bytes
Bytes: 0x8a 0x25 0x00 0x00
Str: 9610
IsNull: false

"column_name" varchar 1500, 0, 0, true
Func readString
readStoredSize: Attempting to read 2 bytes
Bytes: 0xc0 0x03
readString: Attempting to read 960 bytes
Bytes: 0x4c 0x00 0x69 0x00 0x63 0x00 0x65 0x00 0x6e 0x00 0x73 0x00 0x65 0x00 0x65 0x00 0x20 0x00 0x6d 0x00 0x6f 0x00 0x64 0x00  ... 0x65 0x00 0x2e 0x00
Str: Licensee modifications t ... nce.
IsNull: false

```

NB changing the code to allow enabling debug via command-line flag does
impose a ~4% penalty on performance even when not used (edit
bactrac/main.go to completely disable this and get that performance
back).

# Supported datatypes

Based on reverse engineering existing bacpac files, this is only able
to support those datatypes that exist in the bacpac files that have
been available to date.

Of the possible datatypes, those that exist in the bacpac files and
appear to have sufficient data to be able to properly extract/translate
the data consist of:

 * bigint
 * binary (parse only)
 * bit
 * char
 * datetime2
 * datetime
 * decimal
 * float
 * geography (parse, translate point types to WKT-ish form)
 * int
 * ntext
 * nvarchar
 * real
 * smalldatetime
 * smallint
 * smallmoney
 * text
 * tinyint
 * varbinary (parse only)
 * varchar

NB that the CollationLcid for the bacpac files examined is 1033 and
that it is unknown what impact other collations might have on the
parsing and interpreting of bacpack file data.

NB these tools require an already un-zipped bacpac file. Writing the
tools to work with the zipped bacpac file was considered out of scope and
not really necessary.
