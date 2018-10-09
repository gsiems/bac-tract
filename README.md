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

# Testing

## System

Testing on an older Dell Optiplex 780 desktop

 * OS:

        $ cat /etc/redhat-release

            CentOS release 6.10 (Final)

 * CPU:

        $ cat /proc/cpuinfo | grep 'model name' | sort -u

            Intel(R) Core(TM)2 Duo CPU     E8500  @ 3.16GHz

        $ cat /proc/cpuinfo | grep 'model name' | wc -l

            2

 * RAM:

        $ cat /proc/meminfo | grep MemTotal

            MemTotal:        3922536 kB

 * Disk:

        320 GB, 7200 rpm, formatted as ext3

## Bacpac

 * The bacpac file was/is actually a zip archive.

        $ file $bacpac

            $bacpac: Zip archive data, at least v2.0 to extract

 * Rather than deal directly with the zip file, the file was first unzipped.

        $ mkdir extracted
        $ pushd extracted
        $ unzip ../$bacpac
        $ popd

 * The majority of the bacpac is the exported data so ~1.2 GB of data available for extract.

         $ du -sh $bacpac extracted

            90M     $bacpac
            1.2G    extracted

 * There are 97 tables of interest totaling ~1.1 GB of data to extract.

        $ ls extracted/Data/ | grep -P '\.(d|r)' | wc -l > tables

        $ wc -l tables

            97 tables

## Performance

 * Using a simple file copy to establish a base line.

        $ time cp -a extracted tmp

            real    0m34.875s
            user    0m0.047s
            sys     0m2.608s

 * bp2csv

        $ time ../bp2csv -f ../tables -b ../extracted

            real    5m10.108s
            user    5m13.833s
            sys     0m3.986s

 * bp2pg

        $ time ../bp2pg -f ../tables -b ../extracted

            real    5m6.455s
            user    5m3.661s
            sys     0m5.394s

 * bp2ora

        $ time ../bp2ora -f ../tables -b ../extracted

            real    4m41.978s
            user    4m49.621s
            sys     0m4.542s

It should be noted that bp2ora doesn't spend time on escaping
characters while bp2csv and bp2pg do, which may explain the performance
difference when running bp2ora. It should also be noted that it took
slightly longer to run the SQL*Loader files than it did to create them.

Percent CPU while running was pretty consistent for all three cmds,
bouncing around just above 100%, while percent memory was consistently
at 1.3%.

Typical ```top``` output looks like:

        $ top

            top - 09:15:05 up 5 days, 17:27,  9 users,  load average: 0.31, 0.12, 0.16
            Tasks: 199 total,   1 running, 198 sleeping,   0 stopped,   0 zombie
            Cpu(s): 58.3%us,  1.7%sy,  0.0%ni, 40.0%id,  0.0%wa,  0.0%hi,  0.0%si,  0.0%st
            Mem:   3922536k total,  3772840k used,   149696k free,   170944k buffers
            Swap:  2046972k total,    18356k used,  2028616k free,  2171020k cached

              PID USER      PR  NI  VIRT  RES  SHR S %CPU %MEM    TIME+  COMMAND

            32689 gsiems    20   0 54860  49m 1740 S 100.7  1.3   1:15.42 bp2csv

            12787 gsiems    20   0 53804  48m 1460 R 103.7  1.3   4:25.99 bp2pg

             8722 gsiems    20   0 55916  50m 1760 S 111.7  1.3   0:16.41 bp2ora

## Accuracy

The data in the bacpac file started out in Oracle and was migrated to
MS SQL-Server. Therefore, using bp2ora to reload the data into a
separate Oracle schema and comparing the results should serve as a
reasonable test for determining how accurately the data was extracted
from the bacpac file.

# Conclusions

 * Extracting data from bacpac files is feasible-- for non-MS
    SQL-Server shops it can even be practical.

 * Memory usage is not a bottleneck/constraint.

 * Disk I/O is not currently the primary bottleneck/constraint.

 * CPU appears to be the current bottleneck/constraint.

 * Adding a work queue, sorted by descending table size, with multiple
    workers could speed up the data extraction significantly (if needed
    or desired). Given enough CPU cores, this could move the process to
    be fully I/O constrained.
