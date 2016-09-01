# Dynamic SQL Queries on Adaptable Spark Streaming
This Spark program implements 3 important utilities: 
* Being independent of any *Classes* that need to change to solve another business case
* Dynamically controlling what *events* are passed across states
* Possibility to execute multiple dynamically defined SQL queries

### Adapting to Table Description

**DESCRIBE** statement metadata (Ex. from Hive), which provides column names and types ,will be used instead of using *Classes* to represent *events*. So no changes are needed to use the same application as another business case solution.

### Stateful Streaming with Dynamic Filtering

Previous read metadata gives us table's *Private Keys* that will form the *key* for the *key-value* paired *DStream*. 
```
 val eventStream : DStream[(String, EventData)] = (...) //where key is PKs value concatenation
```

Spark Streaming will mantain a value across *batches* for every *key* that holds accumulated *events* across those batches for that *key*. Since each state cannot just accumulate all *events* for a given *key* across *batches* some sort of filtering is required, which will choose what *events* from current batch together with *previous state* ones should be passed as *state* to the next batch.

Instead of having that filtering coded, it can be dynamically configured as *SQL WHERE clause* and changed over time with running application. Currently configured in a file (/src/main/resources/streaming-query.conf), as **filtering_where_clause** parameter, whose configurations are checked every batch.

```
filtering_where_clause = AGE > 10 AND RECEPTIONURGENCY > 4
query_0 = SELECT * FROM events
query_1 = SELECT NAME, AGE, RECEPTIONURGENCY FROM events
query_2 = SELECT NAME, REQUESTTIME FROM events WHERE AGE < 10
```

### Executing Multiple Dynamic SQL Queries

In previously mentioned configuration file you can also set what *SQL queries* should be executed on *streaming*, which holds all events passed as *state* for every key, on every *batch*.

You can also specify a set of queries that will only be executed once, and not every batch as previous ones in another file (/src/main/resources/streaming-one-time-query.conf).

```
one.time.query_3 = SELECT * FROM events WHERE RECEPTIONURGENCY > 8
```

Results of each query can then be exported somewhere. 
Currently saved as *parquet files* in (/src/main/resources/output) folder.

```
drwxrwxr-x. 2 xpandit xpandit 4096 Sep  1 10:47 '0_1472723240000 ms'    //querynumber_batch time 
```
