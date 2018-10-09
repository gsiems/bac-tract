# bac-tract

A feasibility study in extracting data from MS SQL-Server bacpac files (BACpac-exTRACT)

Based on reverse engineering a single bacpac file, the results of this
study do not not support all possible datatypes.

Possible datatypes can be grouped in three main categories:
 * datatypes that are not present in the bacpac,
 * datatypes that are present in the bacpac but do not have sufficent data determine how to process them, and
 * datatypes that appear to have sufficient data to be able to properly extract the data.

Supported datatypes:
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
