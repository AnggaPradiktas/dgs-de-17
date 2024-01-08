
-- 16.0.0  Query Optimization
--         In this lab you will learn and practice the following:
--         - Understanding query performance gains using filters on well-
--         clustered columns
--         - Using GROUP BY and ORDER BY clauses in the correct order
--         - Effects of a LIMIT clause in terms of performance
--         - Benefits of using JOIN on well-clustered columns

-- 16.1.0  Explore Query Performance

-- 16.1.1  Open a new worksheet or Create Worksheet from SQL File and set your
--         context:
--         Set your context and disable the query result cache.

USE ROLE training_role;
USE WAREHOUSE BADGER_wh;
USE DATABASE training_db;
USE SCHEMA tpch_sf1000;

ALTER SESSION SET USE_CACHED_RESULT = false;


-- 16.1.2  Run a query that filters on columns that are well-clustered:
--         A well-clustered column is a key factor in query performance because
--         table data that is not sorted or is only partially sorted may impact
--         query performance, particularly on very large tables.
--         Run the following query that filters on the the o_orderdate column.

SELECT c_custkey,
       c_name,
       SUM(l_extendedprice * (1 - l_discount)) as revenue, c_acctbal,
       n_name,
       c_address,
       c_phone,
       c_comment
FROM customer
  INNER JOIN orders
    ON c_custkey = o_custkey
  INNER JOIN lineitem
    ON l_orderkey = o_orderkey
  INNER JOIN nation
    ON c_nationkey = n_nationkey
WHERE o_orderdate >= to_date('1993-10-01')
    AND o_orderdate < dateadd(month, 3, to_date('1993-10-01'))
    AND l_returnflag = 'R'
GROUP BY c_custkey,
  c_name,
  c_acctbal,
  c_phone,
  n_name,
  c_address,
  c_comment
ORDER BY 3 desc
LIMIT 20;


-- 16.1.3  View the query profile and note performance metrics
--         Take note of:
--         - Partitions scanned - Partitions total
--         This query was filtered on the o_orderdate column. How effective was
--         micro-partition pruning for this query? In general, date columns tend
--         to be fairly well clustered.

-- 16.1.4  Check the clustering quality of the o_orderdate column
--         Use the system function SYSTEM$CLUSTERING_INFORMATION to view the
--         clustering information, including average clustering depth, on the
--         o_orderdate columns in the orders table.

SELECT SYSTEM$CLUSTERING_INFORMATION( 'orders' , '(o_orderdate)' );


-- 16.1.5  Click on the result row and examine statistics
--         Review the JSON object containing the name/value pairs and notice
--         that the table is fairly well clustered around the o_orderdate
--         dimension.
--         - The average_depth is relatively low, which indicates effective
--         clustering
--         - The histogram shows most micro-partitions clustered at the top end
--         (depths of 1 and 2), which also indicates effective clustering

/*
{
  "cluster_by_keys" : "LINEAR(o_orderdate)",
  "total_partition_count" : 2486,
  "total_constant_partition_count" : 182,
  "average_overlaps" : 1.8069,
  "average_depth" : 1.9292,
  "partition_depth_histogram" : {
    "00000" : 0,
    "00001" : 176,
    "00002" : 2310,
    "00003" : 0,
    "00004" : 0,
    "00005" : 0,
    "00006" : 0,
    "00007" : 0,
    "00008" : 0,
    "00009" : 0,
    "00010" : 0,
    "00011" : 0,
    "00012" : 0,
    "00013" : 0,
    "00014" : 0,
    "00015" : 0,
    "00016" : 0
  }
}
*/


-- 16.1.6  Return to the query profile for the query you ran

-- 16.1.7  Click on the TableScan [6] operator (at the bottom of the query
--         profile)
--         How many micro-partitions were pruned? Is this table scan filtered on
--         a column that is well-clustered?
--         The SQL pruner did not skip any micro-partitions when reading the
--         table CUSTOMER because there was no WHERE clause, and the JOIN
--         condition was on a column that was not well clustered.
--         Table Scan Node

-- 16.1.8  Check the clustering quality of the c_custkey column
--         Use the system function SYSTEM$CLUSTERING_INFORMATION to view the
--         clustering information, including average clustering depth, on the
--         c_custkey columns in the customer table.

-- ANSWER AVAILABLE IN ANSWER KEY


-- 16.1.9  Open the row and examine the result
--         - The clustering depth is high, which indicates a poorly-clustered
--         column
--         - The histogram shows most of the micro-partitions grouped toward the
--         bottom of the histogram, which is another indication of a poorly-
--         clustered column.

-- 16.2.0  Explore GROUP BY and ORDER BY Operations Performance
--         The GROUP BY groups rows with the same group-by-item expressions and
--         computes aggregate functions for the resulting group. The ORDER BY
--         specifies an ordering of the rows.
--         When combining the GROUP BY and ORDER BY clauses, to achieve better
--         performance the GROUP BY clause is placed after the WHERE clause and
--         the GROUP BY clause is placed before the ORDER BY clause.

-- 16.2.1  Set the warehouse size

ALTER warehouse BADGER_wh SET WAREHOUSE_SIZE=SMALL;


-- 16.2.2  Run a query which has a GROUP BY on a column with few distinct values

SELECT l_returnflag,
  l_linestatus,
  SUM(l_quantity) AS SUM_QTY,
  SUM(l_extendedprice) AS SUM_BASE_PRICE,
  SUM(l_extendedprice * (1-l_discount)) AS SUM_DISC_PRICE,
  SUM(l_extendedprice * (1-l_discount) *
    (1+l_tax)) AS SUM_CHARGE,
  AVG(l_quantity) AS AVG_QTY,
  AVG(l_extendedprice) AS AVG_PRICE,
  AVG(l_discount) AS AVG_DISC,
  COUNT(*) AS COUNT_ORDER
FROM lineitem
WHERE l_shipdate <= dateadd(day, -90, TO_DATE('1998-12-01'))
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;

--         Note that this produces only four groups.

-- 16.2.3  View the query profile and click on the operator Aggregate [1]
--         Note that the amount of data shuffled during the parallel aggregation
--         operation (Bytes sent over the network) is minimal.
--         Aggregate Node

-- 16.2.4  Click outside the operator to show the statistics panel
--         Note that there is no spilling to local or remote storage.

-- 16.2.5  Click on the operator Sort [4]
--         This is the operator for the ORDER BY. Note that the amount of data
--         shuffled during the global sort operation is also minimal.
--         Sort Node

-- 16.2.6  Run a query with a GROUP BY on a column with many distinct values

SELECT l_shipdate,
       COUNT( * )
FROM lineitem
GROUP BY 1
ORDER BY 1;

--         Note that this creates over 2,000 groups.

-- 16.3.0  Querying with LIMIT
--         Applying a LIMIT clause to a query does not affect the amount of data
--         that is read; it merely limits the results set output.

-- 16.3.1  Execute the following query with a LIMIT clause

SELECT
  s.ss_sold_date_sk,
  r.sr_returned_date_sk,
  s.ss_store_sk,
  s.ss_item_sk,
  s.ss_customer_sk,
  s.ss_ticket_number,
  s.ss_quantity,
  s.ss_sales_price,
  s.ss_customer_sk,
  s.ss_store_sk,
  s.ss_quantity,
  s.ss_sales_price,
  r.sr_return_amt
FROM snowflake_sample_data.tpcds_sf10tcl.store_sales  s
INNER JOIN snowflake_sample_data.tpcds_sf10tcl.store_returns  r
  ON r.sr_item_sk=s.ss_item_sk
WHERE  s.ss_item_sk =4164
LIMIT 100;


-- 16.3.2  Review the query profile:
--         Limit Node
--         Notice that the LIMIT operator is processed at the very end of the
--         query, and has no impact on table access or JOIN filtering. But the
--         LIMIT clause does help to reduce the query result output, which helps
--         to speed up the overall query performance.

-- 16.4.0  Join Optimizations in Snowflake
--         JOIN is one of the most resource-intensive operations. The Snowflake
--         optimizer provides built-in dynamic partition pruning to help reduce
--         data access during join processing. If you use a JOIN filter column
--         that is well-clustered, the query optimization can push down micro-
--         partition pruning.

-- 16.4.1  Set your context

USE SCHEMA snowflake_sample_data.tpcds_sf10tcl;


-- 16.4.2  Run the following query

SELECT count(ss_customer_sk)
FROM store_sales JOIN date_dim d
  ON ss_sold_date_sk = d_date_sk
WHERE d_year = 2000
GROUP BY ss_customer_sk;


-- 16.4.3  Open the query profile, and click on the operator TableScan [4]
--         Table Scan Node

-- 16.4.4  Take note of the performance metrics for this table scan
--         The micro-partition pruning is fairly effective; the SQL pruner
--         skipped a large number of micro-partitions. This corresponds to the
--         filter: D.D_DATE_SK = STORE_SALES.SS_SOLD_DATE_SK

-- 16.4.5  Check the clustering quality of the filter column
--         Use the system function SYSTEM$CLUSTERING_INFORMATION to view the
--         clustering information on the ss_sold_date_sk column in the
--         snowflake_sample_data.tpcds_sf10tcl.store_sales table.

-- ANSWER AVAILABLE IN ANSWER KEY


-- 16.4.6  Review the result
--         - The table is well clustered around the ss_sold_date_sk dimension
--         - The clustering depth is low
--         - The histogram shows most micro-partitions groups near the top
--         All of these contribute to better micro-partition pruning.

