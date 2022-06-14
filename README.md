Snowflake Query Profiler
========================

Snowflake query profiler, providing extensive information for an executed query.

**Additional Information**

* If it's among the longest queries or those consuming most data  
* How many time it has been called in the last month  
* All the query-related information from either ACCOUNT_USAGE or INFORMATION_SCHEMA  
* It runs the query if not found, but SQL text given  
* Correlated metrics  
* Hints on well-documented use case scenarios  
* Hyperlinks to the related Snowflake documentation for some specific metrics  
* Estimates on the volume of data your query parsed, returned or transferred  
* The EXPLAIN plan  

# Database Profile File

Create a **profiles_db.conf** copy of the **profiles_db_template.conf** file, and customize it with your own Snowflake connection parameters. Your top [default] profile is the active profile, considered by our tool. Below you may define other personal profiles, that you may override under [default] each time you want to change your active connection.

We connect to Snowflake with the Snowflake Connector for Python. We have code for (a) password-based connection, (b) connecting with a Key Pair, and (c) connecting with SSO. For password-based connection, save your password in a SNOWFLAKE_PASSWORD local environment variable. Never add the password or any other sensitive information to your code or to profile files. All names must be case sensitive, with no quotes. A database and schema are always required.

# CLI Executable File

Call from the command line in a Terminal window from VSC:

**<code>python query-profiler.py [options]</code>**  

You must specify the SQL query through one of the following options:

* --id queryId              - by the query ID from Snowflake  
* --sql 'queryText'         - the query is passed inline, between single quotes  
* --file queryFile          - the query is stored in a text file (usually myquery.sql)  

To compile into a CLI executable:

```
> pip install pyinstaller
> pyinstaller --onefile query-profiler.py
> dist/query-profiler [options]
```

# Example Usage

**<code>python query-profiler.py --file myquery.sql</code>**

With SQL query passed in a text file.

**<code>python query-profiler.py --id 01a0ed89-0600-ed44-0047-8283000220ca</code>**

With the query ID from Snowflake.

**<code>python query-profiler.py --sql 'select * from mytable'</code>**

With an inline SQL query.

# Example Output

<code>Getting query by SQL from myquery.sql file...  
The query was found by SQL in the ACCOUNT_USAGE schema.  
The query ID is 01a3a078-0501-5c84-004e-a283000ab22e.  

The query has been run successfully in 18,570 ms: it compiled in 1,069 ms + it executed in 17,362 ms.  
The query run between 2022-04-15 09:24:49.879000-07:00 and 2022-04-15 09:25:08.449000-07:00.  

It has been executed 1 times in the last month, for a total of 18.570000 seconds.  
It is among the top 10 longest queries executed in the last month.  
It is among the top 10 queries queries with most data scanned executed in the last month.  

The query was executed by the CSCUTARU user, using the ACCOUNTADMIN role.  
The query was executed within the SNOWFLAKE_SAMPLE_DATA.TPCH_SF100 database and schema context.  
The query used the X-Small COMPUTE_WH standard warehouse, with 1 nodes, 100% of resources available, with 0.000165 cloud ompute credits.  

The query produced 4 rows.  

The query has been queued for 139 ms, waiting for the warehouse to provision, due to the warehouse creation, resume, or resize.  

Nothing spilled to local storage, which is good.  
This often means that the warehouse node(s) had enough memory to process it all in RAM.  

Nothing spilled to remote storage, which is good.  
This often means that the warehouse node(s) had enough RAM and SSD disk space to process it all locally.  
See https://community.snowflake.com/s/article/Performance-impact-from-local-and-remote-disk-spilling.  

The query scanned a total of 15.4 GB (16490786816 bytes).  
Hint: Consider reducing the amount of data a query needs to read from the tables.  

Less than 5% (0.0) of your data has been found in the result cache.  
Hint: Look for consecutive queries that could use the query result cache.  
See https://community.snowflake.com/s/article/Understanding-Result-Caching.  

1,012 partitions out of a total of 1,022 have been scanned.  
Hint: Consider improving partition pruning, by eventually adding some cluster key, or a filter.  
See https://community.snowflake.com/s/article/How-to-recognize-unsatisfactory-pruning.  

EXPLAIN PLAN:  
GlobalStats:  
    partitionsTotal=1022  
    partitionsAssigned=1012  
    bytesAssigned=16490786816  
Operations:  
1:0     ->Result  LINEITEM.L_RETURNFLAG, LINEITEM.L_LINESTATUS, SUM(LINEITEM.L_QUANTITY), SUM(LINEITEM.L_EXTENDEDPRICE), SUM(LINEITEM.L_EXTENDEDPRICE * (1 - LINEITEM.L_DISCOUNT)), SUM((LINEITEM.L_EXTENDEDPRICE * (1 - LINEITEM.L_DISCOUNT)) * (1 + LINEITEM.L_TAX)), (SUM(LINEITEM.L_QUANTITY)) / (COUNT(LINEITEM.L_QUANTITY)), (SUM(LINEITEM.L_EXTENDEDPRICE)) / (COUNT(LINEITEM.L_EXTENDEDPRICE)), (SUM(LINEITEM.L_DISCOUNT)) / (COUNT(LINEITEM.L_DISCOUNT)), COUNT(*)  
1:1          ->Sort  LINEITEM.L_RETURNFLAG ASC NULLS LAST, LINEITEM.L_LINESTATUS ASC NULLS LAST  
1:2               ->Aggregate  aggExprs: [SUM(LINEITEM.L_QUANTITY), SUM(LINEITEM.L_EXTENDEDPRICE), SUM(LINEITEM.L_EXTENDEDPRICE * (1 - LINEITEM.L_DISCOUNT)), SUM((LINEITEM.L_EXTENDEDPRICE * (1 - LINEITEM.L_DISCOUNT)) * (1 + LINEITEM.L_TAX)), COUNT(LINEITEM.L_QUANTITY), COUNT(LINEITEM.L_EXTENDEDPRICE), SUM(LINEITEM.L_DISCOUNT), COUNT(LINEITEM.L_DISCOUNT), COUNT(*)], groupKeys: [LINEITEM.L_RETURNFLAG, LINEITEM.L_LINESTATUS]  
1:3                    ->Filter  LINEITEM.L_SHIPDATE <= '1998-09-02'  
1:4                         ->TableScan  SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.LINEITEM  L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE  {partitionsTotal=1022, partitionsAssigned=1012, bytesAssigned=16490786816}  
</code>
