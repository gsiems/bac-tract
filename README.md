# bac-tract

A feasibility study in parsing and extracting data from MS SQL-Server
bacpac files (BACpac-exTRACT).

Based on reverse engineering a single bacpac file, the results of this
study did not result in support for all possible datatypes. What this
study does show is that, for the datatypes that are supported, it is
both feasible and practical to extract table data directly from bacpac
files.

Of the possible datatypes, those that exist in the bacpac file and
appear to have sufficient data to be able to properly extract/translate
the data consist of:
 * bigint
 * bit
 * char
 * datetime2
 * datetime
 * decimal
 * float
 * int
 * ntext
 * nvarchar
 * real
 * smalldatetime
 * smallint
 * smallmoney (parse only?)
 * text
 * tinyint
 * varbinary (parse only?)
 * varchar

Note that the CollationLcid for the studied bacpac file is 1033 and that
it is unknown what impact other collations might have on the parsing and
interpreting of bacpack file data.
