
-- 3.0.0   TOPIC: Best Practices for Writing High-Performance Queries in
--         Snowflake
--         We submit SQL queries in order to interact with objects and their
--         data in Snowflake. Therefore, effective query formulation and
--         retrieval can have significant impact on query performance, which in
--         turn optimizes compute usage and credit consumption. This exercise
--         will review examples of common query constructs which will form the
--         backdrop for evaluating query performance. This is to help spot both
--         best practices for designing high performing SQL queries, as well as
--         typical issues in SQL query expressions that may cause performance
--         bottlenecks.

-- 3.1.0   Summary of Best Practices for High Performing SQL
--         Select only the columns you need and avoid using SELECT * (star)
--         Apply appropriate filters so the optimizer can effectively prune
--         partitions
--         GROUP BY a small number of distinct values
--         Use columns matching clustering order in the GROUP BY clause so the
--         query optimizer can push down partition pruning
--         ORDER BY smaller cardinality columns
--         When using sub-queries, apply ORDER BY only in the top-most (outer
--         level) SELECT statement
--         Use LIMIT to restrict the number of rows in a result set
--         Use columns matching clustering order in the JOIN predicate so the
--         query optimizer can push down partition pruning
--         JOIN on unique keys or primary keys whenever possible
--         Use TEMPORARY tables to materialize repetitive sub-queries and
--         intermediate results to control cost of repetitive queries

-- 3.2.0   Provide Filters in Queries to Assist SQL Pruner for Reducing Data
--         Access I/O
--         Review performance benefits and implications of filter design to
--         identify the best practices
--         Provide filter in WHERE clause to reduce the dataset as much as
--         possible.
--         Additionally, use columns matching the table’s clustering dimensions.
--         This will provide the best skipping/pruning of micro-partitions, as
--         such filtering enables your query to access only relevant subsets of
--         data which improves performance and reduces costs.

-- 3.2.1   Navigate to Worksheets and create a new worksheet and rename it to
--         Filter Design. Load the contents of the .sql file corresponding to
--         this lab into the worksheet via Load Worksheet or drag-n-drop.

-- 3.2.2   Run the following SQL to set context:

ALTER SESSION SET QUERY_TAG='(BADGER) Lab - TOPIC: Best Practices for Writing High Performance Queries in Snowflake';
USE ROLE TRAINING_ROLE;
USE WAREHOUSE BADGER_QUERY_WH;
USE DATABASE TRAINING_DB;
USE SCHEMA TPCH_SF1000;

ALTER SESSION SET USE_CACHED_RESULT = FALSE;
ALTER WAREHOUSE BADGER_QUERY_WH SET WAREHOUSE_SIZE = 'X-LARGE';


-- 3.2.3   Run the following query to filter on the columns that are clustered
--         columns:

SELECT
  c_custkey,
  c_name,
  sum(l_extendedprice * (1 - l_discount)) as revenue, c_acctbal,
  n_name,
  c_address,
  c_phone,
  c_comment
FROM customer
  inner join orders
    on c_custkey = o_custkey
  inner join lineitem
    on l_orderkey = o_orderkey
  inner join nation
    on c_nationkey = n_nationkey
WHERE
  o_orderdate >= to_date('1993-10-01')
    AND o_orderdate < dateadd(month, 3, to_date('1993-10-01'))
    AND l_returnflag = 'R'
GROUP BY
  c_custkey,
  c_name,
  c_acctbal,
  c_phone,
  n_name,
  c_address,
  c_comment
ORDER BY
  3 desc
LIMIT 20;


-- 3.2.4   Access the query profile after the execution completes.

-- 3.2.5   Click on the TableScan [4] node in the diagram as shown in the screen
--         capture below:
--         TableScan 4 Details

-- 3.2.6   Take note of the Pruning metrics:
--         Partitions scanned 121
--         Partitions total 3,135
--         Note: Don’t be concerned if the numbers you see in your query profile
--         differ slightly from the numbers shown in the screenshot. The
--         important point is that they are close, and display similar ratios.

-- 3.2.7   Observe the following items:
--         The SQL pruner skips a large portion of partitions
--         This corresponds to the filter:
--         Filter condition o_orderdate >= to_date('1993-10-01')   AND
--         o_orderdate < dateadd(month, 3, to_date('1993-10-01'))   AND
--         l_returnflag = 'R'
--         The table ORDERS is well clustered on the predicate column
--         O_ORDERDATE

-- 3.2.8   Run a query to check the clustering quality of the filter column
--         O_ORDERDATE by running the following command:

SELECT SYSTEM$CLUSTERING_INFORMATION( 'orders' , '(o_orderdate)' );

--         Review the result and notice that the table is fairly well clustered
--         around the O_ORDERDATE dimension
--         The average_depth is not high : 1.7499
--         The histogram shows that the both of the micro-partition groups are
--         at the top (where partition_depth is 1 and 2) and there are none at
--         the bottom (partition_depth between 3 and 16)
--         Review the following documentation:
--         Clustering Information Maintained for Micro-partitions

-- 3.2.9   Review the same query profile for the previous query.

-- 3.2.10  Click on the TableScan [6] node in the diagram as in the screenshot
--         below.
--         Take note of the Pruning metrics:
--         Partitions scanned 651
--         Partitions total 651

-- 3.2.11  Take note of the following observations:
--         The SQL optimizer did not prune any partition when reading the
--         CUSTOMER table.
--         There is no WHERE predicate for the CUSTOMER table and the JOIN
--         condition is (ORDERS.O_CUSTKEY = CUSTOMER.C_CUSTKEY)
--         The CUSTOMER table is not clustered by C_CUSTKEY.
--         TableScan 6 Details

-- 3.2.12  Run a query to check the clustering quality of the filter column
--         c_custkey with the following:

SELECT SYSTEM$CLUSTERING_INFORMATION( 'customer' , '(c_custkey)' );


-- 3.2.13  Examine the result and note that the table is poorly clustered around
--         the c_custkey dimension:
--         The average_depth is high : 618.5791
--         The histogram shows that most of the micro-partitions are grouped at
--         the lower-end of the histogram. All of the micro-partitions have a
--         partition_depth between 128 and 1024.
--         Revisit the documentation link cited above for more information
--         regarding clustering.
--         Identify the following best practices:
--         Provide filters in WHERE clauses to reduce the dataset as much as
--         possible.
--         Using columns matching the table’s clustering dimensions will provide
--         the best pruning of micro-partition files. Filters ensure that your
--         query accesses only relevant subsets of data. This improves
--         performance and reduces costs.

-- 3.3.0   Explore the Performance of GROUP BY and ORDER BY Operations
--         Review performance benefits and implications of GROUP BY column usage
--         scenarios to identify the best practices.
--         The following scenarios have lower requirements on compute resources,
--         thereby contributing to faster query performance.
--         Group by columns with the lowest cardinality (few distinct values)
--         you can
--         Group by columns with correlation to the table’s clustering columns
--         Order by columns with lowest cardinality (few distinct values) you
--         can

-- 3.3.1   GROUP BY with low cardinality columns

-- 3.3.2   Run the following query:

SELECT l_returnflag,
l_linestatus,
sum(l_quantity) as sum_qty,
sum(l_extendedprice) as sum_base_price,
sum(l_extendedprice * (1-l_discount)) as sum_disc_price,
sum(l_extendedprice * (1-l_discount) *
(1+l_tax)) as sum_charge,
avg(l_quantity) as avg_qty,
avg(l_extendedprice) as avg_price,
avg(l_discount) as avg_disc,
count(*) as count_order
FROM lineitem
WHERE l_shipdate <= dateadd(day, -90, to_date('1998-12-01'))
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;


-- 3.3.3   View the query profile and select the operator Aggregate [1].
--         Observe the following performance metrics for this operator:
--         The amount of data shuffled during the parallel aggregation operation
--         is limited to only 0.37 MB
--         There are no bottlenecks like spilling to local disk or to remote
--         storage
--         Aggregate Node 1

-- 3.3.4   ORDER BY Performance Example

-- 3.3.5   View the same query profile as in the example above.

-- 3.3.6   Click on the operator Sort [4].
--         Observe the following performance metrics for this operator:
--         The amount of data shuffled during the global sort operation is
--         limited to only 0.03 MB
--         There are no bottlenecks like spilling to local disk or to remote
--         storage
--         Sort Node 4
--         In the GROUP BY clause when using the column’s matching clustering
--         order, the query optimizer can push down partition pruning for a
--         performance benefit.

-- 3.3.7   Run the following query:

SELECT l_shipdate, count( * ) FROM lineitem
GROUP BY 1
ORDER BY 1;


-- 3.3.8   Access the query profile after the execution completes

-- 3.3.9   Click on the TableScan [2] node in the diagram as shown in the
--         screenshot below.
--         Take note of the Pruning metrics:
--         Partitions scanned 2,440
--         Partitions and total partitions 11,649
--         Observe the following benefits:
--         Table Scan Node

-- 3.3.10  Check the clustering quality of the filter column L_SHIPDATE using
--         the following command:

SELECT SYSTEM$CLUSTERING_INFORMATION( 'lineitem' , '(l_shipdate)' );

--         Review the results:
--         The table is well clustered around the L_SHIPDATE dimension
--         The average_depth is not high : 1.2099
--         The histogram shows that both micro-partition groups are at the top-
--         end (partition_depth of 1 and 2) and none are at the bottom-end
--         (partition_depth between 3 and 16)

-- 3.4.0   LIMIT Clauses
--         Applying a LIMIT clause to a query does not affect the amount of data
--         that is read. It simply limits the result set output.

-- 3.4.1   Use the LIMIT clause to limit a result set

-- 3.4.2   Execute the following query:

SELECT
S.SS_SOLD_DATE_SK,
R.SR_RETURNED_DATE_SK,
S.SS_STORE_SK,
S.SS_ITEM_SK,
S.SS_CUSTOMER_SK,
S.SS_TICKET_NUMBER,
S.SS_QUANTITY,
S.SS_SALES_PRICE,
S.SS_CUSTOMER_SK,
S.SS_STORE_SK,
S.SS_QUANTITY,
S.SS_SALES_PRICE,
R.SR_RETURN_AMT
FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.STORE_SALES  S
INNER JOIN SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.STORE_RETURNS  R on R.SR_ITEM_SK=S.SS_ITEM_SK
WHERE  S.SS_ITEM_SK =4164
LIMIT 100;


-- 3.4.3   Review the query profile and select the Limit[1] node in the diagram
--         as in the screenshot below.
--         Limit Node 1

-- 3.4.4   Observe the following traits:
--         The LIMIT operator is processed late in the query plan and has no
--         impact on controlling the table access and join operations
--         But the LIMIT clause does help to reduce significantly the query
--         result output which helps to speed up the overall query performance

-- 3.4.5   Identify the best practices:
--         Use LIMIT clauses to reduce the result set size.
--         Do not expect to use the LIMIT clause to control the number of rows
--         processed during query processing such as reading data, joining data,
--         or aggregating values.

-- 3.5.0   Join Optimizations in Snowflake
--         Joins are one of the most resource-intensive operations. The
--         Snowflake optimizer provides built-in, dynamic partition pruning to
--         help reduce data access during join processing.

-- 3.5.1   Identify the following best practice

-- 3.6.0   Dynamic Partition Pruning Example

-- 3.6.1   Run the following query:

USE SCHEMA snowflake_sample_data.tpcds_sf10tcl;

SELECT count(ss_customer_sk)
FROM store_sales JOIN date_dim d
ON ss_sold_date_sk = d_date_sk
WHERE d_year = 2000
GROUP BY ss_customer_sk;


-- 3.6.2   View the query profile and select the TableScan [4] node in the
--         diagram as in the screenshot below.
--         Table Scan Node 4

-- 3.6.3   Take note of the performance metrics for this operator:
--         Partitions scanned 16,505
--         Partitions total 84,577

-- 3.6.4   Observe the following conditions:

-- 3.6.5   Check the clustering quality of the predicate column SS_SOLD_DATE_SK
--         in the STORE_SALES table using the following command:

SELECT SYSTEM$CLUSTERING_INFORMATION( 'snowflake_sample_data.tpcds_sf10tcl.store_sales', '(ss_sold_date_sk)');


-- 3.6.6   Review the result:
--         The table is well clustered around the ss_sold_date_sk dimension
--         The clustering depth is low : 1.3368
--         The histogram shows that the vast majority of micro-partitions are at
--         the top range with a partition_depth of 00001

-- 3.6.7   Finally, let’s remove the query tag as the work for this lab is
--         complete.

ALTER SESSION UNSET QUERY_TAG;


