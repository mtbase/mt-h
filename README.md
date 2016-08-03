# MT-H DBGen
MT-H dbgen. A data generator for the MT-H benchmark. Extends the dbgen for
TPC-H from [Airlift](https://github.com/airlift/tpch).

### About MT-H
MT-H is an extension to TPC-H for multi-tenant workloads. Compared to TPC-H, it
assumes the Customers, Orders and Lineitems tables as tenant-specific in order
to evaluate a system that implements multi-tenancy-aware SQL (MTSL) as
explained in the [MTBase white
paper](https://github.com/mtbase/overview/blob/master/extended.pdf). Mt-H  and
MT-H dbgen are part of the [MTBase
project](https://github.com/mtbase/overview).

### Building and Running

Do `mvn clean package` in order to generate the executable under
`target/mt-dbgen.jar`.

Run the data generator with `java -jar ./target/mt-dbgen.jar`.

The output of `mt-dbgen` is controlled by a combination of command line
options. They are single-letter flags preceded by a dash and potentially
followed by an argument.

#### Command Line Options
```
 -f <arg>   -- set the output files format <oracle (with separator at end of each line)|postgres
               (no separator at line end)> (default:'oracle')
 -h         -- display this message
 -m <arg>   -- set distribution mode (uniform=default, zipf
 -o <arg>   -- set the output directory (default: 'output'
 -p <arg>   -- set the number of parts the files should be divided into (default: 1)
 -s <arg>   -- set scale factor (SF) (default: 1)
 -t <arg>   -- set number of tenants (default: 1)
 -h         -- display this message
 -s <arg>   -- set Scale Factor (SF) to <n> (default: 1)
 -t <arg>   -- set Number of Tenants to <n> (default: 1)
 -m <arg>   -- set distribution mode to <mode> (default: uniform, others:zipf)
 -T <arg>   -- generate tables
 -o <arg>   -- output path
```

#### Sample usage
 `java -jar mt-dbgen.jar -h` &rarr; displays the help message

 `java -jar mt-dbgen.jar -s 1 -t 10 -m uniform` &rarr; generates 1GB of data
 for 10 tenants and with a uniform distribution

### What does mt-dbgen generate?
mt-dbgen generates (at least) 11 separate ascii files in the `output` folder.
As in TPC-H dbgen, each file contains pipe-delimited data for one of the tables
defined in the MT-H database schema. If `-p <n>` is set, that means that for
each table (except for the constant-size tables Nation, Region, Tenants,
PhoneTransform, and CurrencyTransform), `n` separate files are created. This
procedure triggers MT-H dbgen to run multi-threaded and it also allows data
loading to happen in parallel (if the underlying DBMS allows that).
