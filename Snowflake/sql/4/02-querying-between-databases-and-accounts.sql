
-- 2.0.0   Querying Between Databases and Accounts
--         This lab will take approximately 15 minutes to complete.

-- 2.1.0   Prepare for the Lab
--         The purpose of this lab is to show that query performance is not
--         degraded by including shares, clones, or multiple databases.

-- 2.1.1   Open a new worksheet and make sure your default context is set.

-- 2.1.2   Change your warehouse size to medium, and use the schema you created
--         in lab 1:

ALTER WAREHOUSE BADGER_wh SET WAREHOUSE_SIZE=Medium;
USE WAREHOUSE BADGER_wh;
USE SCHEMA BADGER_db.myschema;


-- 2.1.3   Create two temporary tables using CTAS.

CREATE TEMPORARY TABLE customer_ctas AS
   SELECT * FROM snowflake_sample_data.tpch_sf100.customer;
   
CREATE TEMPORARY TABLE orders_ctas AS
   SELECT * FROM snowflake_sample_data.tpch_SF100.orders;


-- 2.1.4   Create two temporary tables using cloning. Note that the cloning is
--         immediate, while the CTAS took time to complete.

CREATE TEMPORARY TABLE customer_clone  
   CLONE customer_ctas;
   
CREATE TEMPORARY TABLE orders_clone 
   CLONE orders_ctas;


-- 2.1.5   Turn off the Query Result Cache, to make sure all statements are
--         executed rather than being taken from cache. Then suspend your
--         warehouse to clear anything in the data cache.

ALTER SESSION SET USE_CACHED_RESULT=FALSE;

ALTER WAREHOUSE BADGER_wh SUSPEND;

--         NOTE that you may get an error saying that the warehouse cannot be
--         suspended - this means that the warehouse is already suspended.
--         Ignore this message if it occurs in this or subsequent labs.

-- 2.2.0   JOIN Local Tables

-- 2.2.1   Run the following query to join the two tables you created with CTAS:

SELECT c.c_custkey, c.c_name, o.o_orderstatus, o.o_orderpriority
  FROM customer_ctas c JOIN orders_ctas o ON c.c_custkey=o.o_custkey;


-- 2.2.2   Run the following query to join the two tables you created by
--         cloning:

SELECT c.c_custkey, c.c_name, o.o_orderstatus, o.o_orderpriority
  FROM customer_clone c JOIN orders_clone o ON c.c_custkey=o.o_custkey;


-- 2.2.3   Review the query profiles and compare the total execution time and
--         number of rows returned.
--         The join of the cloned tables may have run faster than the join with
--         the tables created with CTAS. Why might this be?
--         Look at the Percentage scanned from cache in the query profiles. A
--         cloned tables shares micro-partitions with the source table. Because
--         of this, the second JOIN was able to use data that was stored in the
--         data cache on the virtual warehouse.

-- 2.2.4   Suspend your warehouse to clear the data cache.

ALTER WAREHOUSE BADGER_wh SUSPEND;


-- 2.2.5   Re-run the JOIN of the cloned tables, and verify execution time and
--         number of rows returned.

SELECT c.c_custkey, c.c_name, o.o_orderstatus, o.o_orderpriority
  FROM customer_clone c JOIN orders_clone o ON c.c_custkey=o.o_custkey;

--         Since the data cache was empty, performance of the JOIN on the clones
--         should be very close to the performance on the tables that were
--         created with CTAS.

-- 2.3.0   Join Tables Using a Share

-- 2.3.1   Run a query that joins two tables from SNOWFLAKE_SAMPLE_DATA.

SELECT c.c_custkey, c.c_name, o.o_orderstatus, o.o_orderpriority
  FROM snowflake_sample_data.tpch_sf100.customer c JOIN snowflake_sample_data.tpch_sf100.orders o ON c.c_custkey=o.o_custkey;

--         The SNOWFLAKE_SAMPLE_DATA database is a shared database: the tab (or
--         view) data in a share is located in the data providers account. The
--         data consumer account does not store any of the data and does not pay
--         for the data storage. Also note this sample data database is shared
--         to all Snowflake customers and each customer can choose to setup a
--         database for users to have access to this sample data. Note that
--         queries on shared data do use the data consumers virtual
--         warehouses(s) and credits.

-- 2.3.2   Record how long the query took, and how many rows were returned.
--         How does the performance compare now? The query on the local
--         databases and the shared database should run in essentially the same
--         amount of time. Since these tables are fairly small, there may be
--         slight differences due to overhead.

-- 2.3.3   Suspend your virtual warehouse to clear the data cache.

ALTER WAREHOUSE BADGER_wh SUSPEND;


-- 2.3.4   JOIN using a local table and a shared table.

SELECT c.c_custkey, c.c_name, o.o_orderstatus, o.o_orderpriority
  FROM snowflake_sample_data.tpch_sf100.customer c JOIN BADGER_db.myschema.orders_ctas o ON c.c_custkey=o.o_custkey;


-- 2.3.5   Check the execution time and number of rows returned. The performance
--         should be comparable to the other JOINs you performed.

-- 2.3.6   Turn the Query Result Cache back on so it is available for future
--         labs.

ALTER SESSION SET USE_CACHED_RESULT=TRUE;


-- 2.3.7   Clean up the objects created for this lab.

DROP TABLE customer_ctas;
DROP TABLE customer_clone;
DROP TABLE orders_ctas;
DROP TABLE orders_clone;


