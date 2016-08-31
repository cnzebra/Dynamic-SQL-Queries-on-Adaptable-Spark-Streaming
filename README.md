# Dynamic SQL Queries on Adaptable Spark Streaming
This Spark program implements 3 important utilities: 
* Being independent of any *Classes* that need to change to solve another business case
* Dynamically controlling what *events* are passed across states
* Possibility to execute multiple dynamically defined SQL queries

### Adapting to Table Description

**DESCRIBE** statement metadata (Ex. from Hive), which provides column names and types ,will be used instead of using *Classes* to represent *events*. So no changes are needed to use the same application as another business case solution.

(To be continued...)


