# MT-H
TPC-H Multi-tenant dbgen. Based on the work from [here](https://github.com/airlift/tpch)

### About MT-H dbgen
Comparing with the original TPC-H dbgen verison, the tpch-mt version extended with serveral MT features and is designed for Multi-Tenancy DB benchmark.

### Usage
`mt-dbgen.java` is the entry point of tpch-mt application. The output is controlled by a combination of command line options
and environment variables. Command line options are assumed to be single letter flags preceded by a minus sign. They may be followed by an optional argument.

Do `mvn clean package` in order to generate the executable under `target/mt-dbgen.jar` then proceed with regular execution.

#### Command Line Options
```
 -h         -- display this message
 -s <arg>   -- set Scale Factor (SF) to <n> (default: 1)
 -t <arg>   -- set Number of Tenants to <n> (default: 1)
 -m <arg>   -- set distribution mode to <mode> (default: uniform, others:zipf)
 -T <arg>   -- generate tables
 -o <arg>   -- output path
 ```
 Sample usage: `java -jar mt-dbgen.jar -s 1 -t 10 -m uniform`.

### What will mt-dbgen create?
mt-dbgen will generate (at least 6) separate ascii files under the `output` folder. This is not as same as original TPCH dbgen. We eliminate the PART and PARTSUPP table. Each file will contain pipe-delimited load data for one of the tables defined in the TPC-H database schema. The default tables will contain the load data required for a scale factor 1 database. By default the file will be created in the current directory and be named `<table>.tbl`. As an example, customer.tbl will contain the load data for the customer table.
