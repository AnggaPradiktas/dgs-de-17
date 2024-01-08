
-- 4.0.0   TOPIC: Performance Analysis Toolkit and Tuning Metrics
--         This lab provides examples for identifying some of the most common
--         performance bottlenecks and issues you may encounter when running
--         queries in Snowflake. This set of exercises also shows the set of SQL
--         operations that are most commonly associated with these performance
--         bottlenecks and issues.

-- 4.1.0   Explore Spilling to Local and Remote Storage

-- 4.1.1   Set your context, and turn off the query result cache.

ALTER SESSION SET QUERY_TAG='(BADGER) Lab - TOPIC: Performance Analysis Toolkit and Tuning Metrics';
USE ROLE TRAINING_ROLE;
USE WAREHOUSE BADGER_QUERY_WH;
USE database BADGER_DB;
USE SCHEMA snowflake_sample_data.tpcds_sf10tcl;
ALTER SESSION SET USE_CACHED_RESULT = FALSE;



-- 4.1.2   Resize your warehouse to SMALL.

ALTER WAREHOUSE BADGER_QUERY_WH SUSPEND;
ALTER WAREHOUSE BADGER_QUERY_WH SET WAREHOUSE_SIZE = XSMALL;
ALTER WAREHOUSE BADGER_QUERY_WH RESUME;
ALTER WAREHOUSE BADGER_QUERY_WH SET
  WAREHOUSE_SIZE = SMALL
  WAIT_FOR_COMPLETION = TRUE;

--         Note: If the warehouse is not running, suspending it will cause an
--         inconsequential error. Ignore it.

-- 4.1.3   Run a query with a window function on the small warehouse.
--         The query lists detailed catalog sales data together with a running
--         sum of sales price within the order. On a small warehouse this will
--         take a few minutes to complete.

SELECT cs_bill_customer_sk
     , cs_order_number
     , i_product_name
     , cs_sales_price
     , SUM(cs_sales_price) OVER (PARTITION BY cs_order_number
        ORDER BY i_product_name
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sum
  FROM catalog_sales, date_dim, item
  WHERE cs_sold_date_sk = d_date_sk
    AND cs_item_sk = i_item_sk
    AND d_year IN (2000)
    AND d_moy IN (1,2,3,4,5,6)
  LIMIT 100;


-- 4.1.4   View the query profile and select the operator WindowFunction [1]
--         Note how long the query took, and how much of the query time was
--         spent on the window function. Also note that your query is spilling
--         to local storage, and possibly to remote storage.
--         WindowFunction 1 Spillage Small WH

-- 4.1.5   Resize your warehouse to MEDIUM.

ALTER WAREHOUSE BADGER_QUERY_WH SUSPEND;
ALTER WAREHOUSE BADGER_QUERY_WH RESUME;
ALTER WAREHOUSE BADGER_QUERY_WH SET
  WAREHOUSE_SIZE = MEDIUM
  WAIT_FOR_COMPLETION = TRUE;


-- 4.1.6   Re-run the query using a medium warehouse.

SELECT cs_bill_customer_sk
     , cs_order_number
     , i_product_name
     , cs_sales_price
     , SUM(cs_sales_price) OVER (PARTITION BY cs_order_number
        ORDER BY i_product_name
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sum
  FROM catalog_sales, date_dim, item
  WHERE cs_sold_date_sk = d_date_sk
    AND cs_item_sk = i_item_sk
    AND d_year IN (2000)
    AND d_moy IN (1,2,3,4,5,6)
  LIMIT 100;


-- 4.1.7   View the query profile.
--         What was the query time? Click on the WindowFunction[1] node in the
--         diagram and see if the query is still spilling to local or remote
--         storage.
--         WindowFunction 1 Spillage Medium WH

-- 4.1.8   Resize your virtual warehouse to LARGE.

ALTER WAREHOUSE BADGER_QUERY_WH SUSPEND;
ALTER WAREHOUSE BADGER_QUERY_WH RESUME;
ALTER WAREHOUSE BADGER_QUERY_WH SET
  WAREHOUSE_SIZE = LARGE
  WAIT_FOR_COMPLETION = TRUE;


-- 4.1.9   Re-run the query using a large warehouse.

SELECT cs_bill_customer_sk
     , cs_order_number
     , i_product_name
     , cs_sales_price
     , SUM(cs_sales_price) OVER (PARTITION BY cs_order_number
        ORDER BY i_product_name
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sum
  FROM catalog_sales, date_dim, item
  WHERE cs_sold_date_sk = d_date_sk
    AND cs_item_sk = i_item_sk
    AND d_year IN (2000)
    AND d_moy IN (1,2,3,4,5,6)
  LIMIT 100;


-- 4.1.10  View the query profile.
--         What was the execution time? Is WindowFunction[1] still spilling?
--         WindowFunction 1 Spillage Large WH
--         Snowflake’s architecture allows for linear scaling of compute
--         resources, enabling both better performance and lower cost at the
--         same time.
--         Example response times:

/*
Cluster Size   Response Time   Credit Cost
-------------- --------------- -------------
Small          3 mins          0.108
Medium         1 min 20s       0.088
Large          30s             0.066
*/


-- 4.2.0   Evaluate WHERE Clauses Based on Clustering Efficiency
--         In this scenario, we look into large table scan symptoms caused by
--         the fact that the query’s filter column is not the clustering
--         dimension of the base table.

-- 4.2.1   Set your context and warehouse size.

USE SCHEMA training_db.tpch_sf1000;

ALTER SESSION SET USE_CACHED_RESULT = FALSE;
ALTER WAREHOUSE BADGER_QUERY_WH SUSPEND;
ALTER WAREHOUSE BADGER_QUERY_WH SET WAREHOUSE_SIZE = MEDIUM;
ALTER WAREHOUSE BADGER_QUERY_WH RESUME;


-- 4.2.2   Run a query with a filter on a column that is not well-clustered.

SELECT COUNT(*)
FROM  lineitem
WHERE l_extendedprice < 30000;


-- 4.2.3   View the query profile and note the micro-partition pruning.
--         Query Profile Where Clause Poor Clustering
--         Note that all micro-partitions were scanned.

-- 4.2.4   Evaluate the clustering efficiency on the L_EXTENDEDPRICE column.

SELECT SYSTEM$CLUSTERING_INFORMATION( 'lineitem' , '(l_extendedprice)' );

--         Note that the table is poorly clustered on the L_EXTENDEDPRICE
--         dimension. The clustering depth shows almost 100% overlap in the
--         micro-partitions.

-- 4.2.5   Run a query that filters on a well-clustered column.

SELECT COUNT(*)
  FROM lineitem
  WHERE l_shipdate > to_date('1995-03-15');


-- 4.2.6   Open the query profile and view micro-partition pruning.
--         Query Profile Where Clause Well-Clustered

-- 4.2.7   Evaluate the clustering efficiency of the filter column.

SELECT SYSTEM$CLUSTERING_INFORMATION( 'lineitem' ,   '(l_shipdate)' );

--         Summary: The amount of data scanned by a query is directly related to
--         how well a query’s WHERE clause column correlates to the clustering
--         dimension of the table being accessed.

-- 4.3.0   Rogue Query Symptom from JOIN Explosion
--         In this scenario, we will explore run away query symptoms which are
--         common in real workloads. One common type of rogue query includes
--         join pitfalls like explosion of output and unintentional cross joins.

-- 4.3.1   Set your context.

USE SCHEMA  snowflake_sample_data.tpcds_sf100tcl;
ALTER WAREHOUSE BADGER_QUERY_WH SET WAREHOUSE_SIZE = MEDIUM;
USE WAREHOUSE BADGER_QUERY_WH;


-- 4.3.2   Run the following query:
--         This may take 3-4 minutes to complete.

SELECT S.SS_SOLD_DATE_SK
     , R.SR_RETURNED_DATE_SK
     , S.SS_STORE_SK
     , S.SS_ITEM_SK
     , S.SS_SALES_PRICE
     , S.SS_SALES_PRICE
     , R.SR_RETURN_AMT
  FROM STORE_SALES  S
    INNER JOIN STORE_RETURNS  R
       on R.SR_ITEM_SK=S.SS_ITEM_SK
  WHERE  S.SS_ITEM_SK =4164;


-- 4.3.3   View the profile for this query.
--         Note that the JOIN produces a large number of output records relative
--         to the sizes of the two input data sets.
--         Query Profile Join Explosion
--         To avoid performance issues with a JOIN that could explode the
--         outputs, you should JOIN on unique keys whenever possible. If not
--         possible, consider other options such as adding more filters to the
--         WHERE clause or scaling up the warehouse.

-- 4.4.0   Tune Timeout Parameters
--         In this exercise, we will explore tuning the timeout parameter
--         available to a virtual warehouse to manage long-running workloads.

-- 4.4.1   Review the existing values of the timeout parameters:

SHOW PARAMETERS IN WAREHOUSE BADGER_QUERY_WH;

--         Show Warehouse Parameters
--         You will see that the maximum timeout for a queued statement is 0
--         seconds (meaning it will never time out). The maximum timeout of a
--         statement is 2 days.

-- 4.4.2   Update the statement_timeout_in_seconds parameter to 30 seconds.

ALTER WAREHOUSE BADGER_QUERY_WH SET STATEMENT_TIMEOUT_IN_SECONDS = 30;


-- 4.4.3   Re-run the query from above.

SELECT S.SS_SOLD_DATE_SK
     , R.SR_RETURNED_DATE_SK
     , S.SS_STORE_SK
     , S.SS_ITEM_SK
     , S.SS_SALES_PRICE
     , S.SS_SALES_PRICE
     , R.SR_RETURN_AMT
  FROM STORE_SALES  S
    INNER JOIN STORE_RETURNS  R
       on R.SR_ITEM_SK=S.SS_ITEM_SK
  WHERE  S.SS_ITEM_SK =4164;

--         The statement will timeout before it completes, and generate an error
--         message.
--         Timeout Error Message

-- 4.4.4   Set the timeout back to its default using UNSET.

ALTER WAREHOUSE BADGER_QUERY_WH UNSET STATEMENT_TIMEOUT_IN_SECONDS;


-- 4.4.5   Confirm that the timeout parameter has been restored to 172,800
--         seconds (2 days)

SHOW PARAMETERS IN WAREHOUSE BADGER_QUERY_WH;


-- 4.5.0   Use Query Tags
--         In this exercise, we will explore setting the QUERY_TAG parameter to
--         track queries executed within a session. This additional metadata can
--         be queried later for tracking purposes. We can search the
--         INFORMATION_SCHEMA.QUERY_HISTORY view or by filtering on the Query
--         Tag in the History view of the Snowflake Web UI.

-- 4.5.1   See if an existing query tag is set.

SHOW PARAMETERS LIKE '%QUERY_TAG%' FOR SESSION;


-- 4.5.2   Update the QUERY_TAG session parameter:

ALTER SESSION SET QUERY_TAG = 'Lab 14 - BADGER_join_test';

SHOW PARAMETERS LIKE '%QUERY_TAG%' FOR SESSION;


-- 4.5.3   Run the join explosion query from the previous exercise with a LIMIT
--         clause.

SELECT s.ss_sold_date_sk
     , r.sr_returned_date_sk
     , s.ss_store_sk
     , r.sr_returned_date_sk
     , s.ss_item_sk
     , s.ss_sales_price
     , r.sr_return_amt
  FROM store_sales s
      INNER JOIN store_returns r
        ON  r.sr_item_sk=s.ss_item_sk
  WHERE s.ss_item_sk = 4164
  LIMIT 100;


-- 4.5.4   Use the History view in the Snowflake Web UI with the Query Tag
--         filter to find tagged queries.
--         It will look something like this:
--         Show Parameters Results

-- 4.5.5   Look for the query tag using the INFORMATION_SCHEMA.QUERY_HISTORY
--         table function.

SELECT QUERY_ID, QUERY_TAG
  FROM TABLE (INFORMATION_SCHEMA.QUERY_HISTORY())
  WHERE QUERY_TAG LIKE '%BADGER%';


-- 4.5.6   Finally, reset the QUERY_TAG session parameter to its default value
--         (empty string).

ALTER SESSION UNSET QUERY_TAG;

SHOW PARAMETERS LIKE '%QUERY_TAG%' FOR SESSION;
-- or
SHOW PARAMETERS LIKE '%QUERY_TAG%' IN SESSION;


