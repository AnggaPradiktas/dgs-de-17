
-- 15.0.0  Data Sharing
--         In this lab you will learn and practice the following:
--         - Setup Two Browsers for Data Sharing
--         - Basic Data Sharing
--         - Create database on data consumer account from tables shared on the
--         data provider server
--         - Remove objects
--         - Explore Secure User-defined Functions For Protecting Shared Data
--         Secure Data Sharing enables account-to-account sharing of data
--         through Snowflake database tables, secure views, and secure UDFs.
--         In this exercise you will learn how to setup two Snowflake accounts
--         for data sharing. The first account will be the data [provider-
--         account] . The second account will be the data [consumer-account].
--         You will then perform the steps to enable sharing selected objects in
--         a database in the [provider-account] with the [consumer-account]. No
--         actual data is copied or transferred between accounts. All data
--         sharing is accomplished through Snowflake’s unique services layer and
--         metadata store.

-- 15.1.0  Setup Browsers for Data Sharing

-- 15.1.1  Open two browser windows side-by-side

-- 15.1.2  In browser 1, enter the Snowflake [provider-account] URL, login,
--         navigate to worksheets and load the script into a new worksheet
--         You can use the Create Worksheet from SQL File. In this worksheet,
--         you will only execute commands that are surrounded by PROVIDER
--         comments:

-- PROVIDER --


-- 15.1.3  Rename the worksheet to something that contains the word provider

-- 15.1.4  In browser 2, enter the Snowflake [consumer-account] URL, login,
--         navigate to worksheets and load the script into a new worksheet
--         You can use the Create Worksheet from SQL File. In this worksheet,
--         you will only execute commands that are surrounded by CONSUMER
--         comments:

-- CONSUMER --


-- 15.1.5  Rename the worksheet to something that contains the word consumer
--         Your browsers setup should look something like this with probably a
--         different lab number.
--         <– Provider browser on the left hand side and the Consumer browser –>
--         on the right
--         Data Sharing Browsers Setup side-by-side

-- 15.2.0  Basic Data Sharing
--         In this exercise you will create a basic data share and share data.
--         Perform the following steps in the [provider-account] in browser 1.

-- 15.2.1  Set the context

-- PROVIDER --
USE ROLE training_role;
USE WAREHOUSE BADGER_wh;
CREATE DATABASE BADGER_share_db;
USE DATABASE BADGER_share_db;
-- PROVIDER --


-- 15.2.2  Create schema and tables

-- PROVIDER --
CREATE SCHEMA ds_tpch_sf1;
USE SCHEMA ds_tpch_sf1;

CREATE TABLE customer AS
  SELECT c_custkey,
    c_name,
    c_address,
    c_nationkey,
    c_phone,
    c_acctbal,
    c_mktsegment,
    c_comment
FROM snowflake_sample_data.tpch_sf1.customer;

CREATE TABLE orders AS
  SELECT o_orderkey,
    o_custkey,
    o_orderstatus,
    o_totalprice,
    o_orderdate,
    o_orderpriority,
    o_clerk,
    o_shippriority,
    o_comment
FROM snowflake_sample_data.tpch_sf1.orders;
-- PROVIDER --


-- 15.2.3  Create empty share
--         An empty share is a shell that you can later use to share actual
--         objects.

-- PROVIDER --
CREATE SHARE BADGER_share;
-- PROVIDER --


-- 15.2.4  Grant object privileges to the share

-- PROVIDER --
GRANT USAGE ON DATABASE BADGER_share_db
  TO SHARE BADGER_share;

GRANT USAGE ON SCHEMA BADGER_share_db.ds_tpch_sf1
  TO SHARE BADGER_share;

GRANT SELECT ON TABLE BADGER_share_db.ds_tpch_sf1.customer
  TO SHARE BADGER_share;

GRANT SELECT ON TABLE BADGER_share_db.ds_tpch_sf1.orders
  TO SHARE BADGER_SHARE;
-- PROVIDER --


-- 15.2.5  Add account to the share

-- PROVIDER --
-- In this next command replace [consumer-account] with the account for the consumer you are using
ALTER SHARE BADGER_share SET ACCOUNTS=[consumer-account];
-- PROVIDER --


-- 15.2.6  Validate the share configuration

-- PROVIDER --
SHOW GRANTS TO SHARE BADGER_share;
-- PROVIDER --


-- 15.3.0  Create database on data consumer account from tables shared on the
--         data provider server
--         Perform the following steps in the [consumer-account] in browser 2.

-- 15.3.1  View the in inbound shares
--         First click Home icon to take you out of the lab Worksheet and then
--         navigate to Data > Private Sharing and select Shared With Me at top
--         if not selected. You should see under Direct Shares the share you
--         just created on the provider side.

-- 15.3.2  Open back up your consumer side Worksheet by clicking Worksheets and
--         select your lab worksheet name

-- 15.3.3  Set context on Consumer side

-- CONSUMER --
USE ROLE training_role;

USE WAREHOUSE BADGER_wh;

-- CONSUMER --


-- 15.3.4  Use SQL to show available shares

-- CONSUMER --
SHOW SHARES LIKE 'BADGER_share';

DESCRIBE SHARE [provider-account].BADGER_share;
-- CONSUMER --


-- 15.3.5  Examine contents of share on data consumer

-- CONSUMER --
CREATE DATABASE BADGER_ds_consumer
FROM SHARE [provider-account].BADGER_share;

USE DATABASE BADGER_ds_consumer;

SHOW SCHEMAS;
USE SCHEMA ds_tpch_sf1;

SHOW TABLES;

SELECT * FROM customer
LIMIT 10;
-- CONSUMER --


-- 15.3.6  Use Public Role, and test ability to query share
--         Note: You will run into a few error messages as you run each
--         statement below. Keep going and the subsequent queries will make the
--         necessary changes.

-- CONSUMER --
GRANT USAGE ON WAREHOUSE BADGER_wh
  TO ROLE public;

USE ROLE PUBLIC;
USE DATABASE BADGER_ds_consumer;
--This command will fail because PUBLIC does not have access to the database

USE ROLE training_role;
GRANT USAGE ON DATABASE BADGER_ds_consumer TO ROLE public;
--You should have received an SQL compilation error message: you cannot add privileges to a share

GRANT IMPORTED PRIVILEGES ON DATABASE BADGER_ds_consumer TO ROLE public;

USE ROLE PUBLIC;
USE DATABASE BADGER_ds_consumer;

SHOW SCHEMAS;
USE SCHEMA ds_tpch_sf1;

SELECT * FROM customer
LIMIT 10;
-- CONSUMER --


-- 15.4.0  Create A Secure View And Add It To The Share
--         Perform all the steps in this section on [provider-account] in
--         browser 1.

-- 15.4.1  Create a schema

-- PROVIDER --
USE DATABASE BADGER_share_db;
CREATE SCHEMA private;
-- PROVIDER --


-- 15.4.2  Create a mapping table
--         A mapping table is only required if you wish to share the data in the
--         base table with multiple consumer accounts and share specific rows in
--         the table with specific accounts.

-- PROVIDER --
CREATE OR REPLACE TABLE private.sharing_access(
  c_custkey string,
  snowflake_account string);
-- PROVIDER --


-- 15.4.3  Populate mapping table

-- PROVIDER --
-- In this next command replace [consumer-account] with the account for the consumer you are using
INSERT INTO private.sharing_access (c_custkey, snowflake_account)
SELECT c_custkey,
       CASE WHEN c_custkey BETWEEN 1 AND 20 THEN '[consumer-account]'
            ELSE 'UNKNOWN'
            END AS SNOWFLAKE_ACCOUNT
FROM ds_tpch_sf1.customer
WHERE c_custkey BETWEEN 1 AND 50;
-- PROVIDER --


-- 15.4.4  Create a secure view
--         Remember a secure view hides the SQL used to create the view and runs
--         the optimizer after the secured data is filtered out meaning that the
--         query will not return an access error message. Unauthorized values
--         will appear as not in the table

-- PROVIDER --
CREATE OR REPLACE SECURE VIEW ds_tpch_sf1.cust_sensitive_data_vw AS
SELECT sd.c_custkey,
    sd.c_name,
    sd.c_address,
    sd.c_nationkey,
    sd.c_phone
FROM ds_tpch_sf1.customer sd
INNER JOIN private.sharing_access sa
  ON sd.c_custkey = sa.c_custkey
AND UPPER(sa.snowflake_account) = UPPER(CURRENT_ACCOUNT());
-- PROVIDER --


-- 15.4.5  Validate the table and secure view

-- PROVIDER --
SELECT
  COUNT(*)
FROM ds_tpch_sf1.cust_sensitive_data_vw;
-- should return 0 rows because the provider account is not mapped
-- PROVIDER --


-- 15.4.6  Validate the secure view by simulating data consumer

-- PROVIDER --
-- Again replace [consumer-account] with the account for the consumer you are using
ALTER SESSION SET SIMULATED_DATA_SHARING_CONSUMER='[consumer-account]';
SELECT
  COUNT(*)
FROM ds_tpch_sf1.cust_sensitive_data_vw;
-- This should return 20 rows because simulated consumer account is mapped!

ALTER SESSION UNSET SIMULATED_DATA_SHARING_CONSUMER;
-- PROVIDER --


-- 15.4.7  Add the secure view to the share

-- PROVIDER --
GRANT SELECT ON BADGER_share_db.ds_tpch_sf1.cust_sensitive_data_vw
  TO SHARE BADGER_SHARE;


-- 15.4.8  Confirm grants on the share

-- PROVIDER --
SHOW GRANTS TO SHARE BADGER_share;
-- PROVIDER --


-- 15.5.0  Data Consumer - Use A Shared Database
--         Perform all the steps in this section on [consumer-account] in
--         browser 2.

-- 15.5.1  Set context and alter warehouse to Small

-- CONSUMER --
USE ROLE training_role;

ALTER WAREHOUSE BADGER_wh SET WAREHOUSE_SIZE=Small;
-- CONSUMER --


-- 15.5.2  View available shares

-- CONSUMER --
SHOW SHARES LIKE 'BADGER_share';
DESCRIBE SHARE [provider-account].BADGER_share;
-- CONSUMER --


-- 15.5.3  Create a database from the share and grant privileges

-- CONSUMER --
CREATE DATABASE BADGER_ds_consumer
FROM SHARE [provider-account].BADGER_share;
--Note: This will generate an error, because the database already exists, from the
--first time the consumer ingested the share.  The provider did not create a
--new share, but added objects to an existing share making it easy for consumer to access new objects.

GRANT IMPORTED PRIVILEGES ON DATABASE BADGER_ds_consumer TO ROLE training_role;
-- CONSUMER --


-- 15.5.4  Validate shared objects

-- CONSUMER --
SHOW DATABASES LIKE 'BADGER_ds_consumer';
SHOW SCHEMAS IN DATABASE BADGER_ds_consumer;
SHOW TABLES IN SCHEMA BADGER_ds_consumer.ds_tpch_sf1;
SHOW VIEWS IN SCHEMA BADGER_ds_consumer.ds_tpch_sf1;


-- 15.5.5  Validate shared objects by running queries

-- CONSUMER --
SELECT COUNT(*)
FROM BADGER_ds_consumer.ds_tpch_sf1.cust_sensitive_data_vw;
-- expected row count =  20
SELECT COUNT(*)
FROM BADGER_ds_consumer.ds_tpch_sf1.customer;
-- expected row count = 150000
SELECT COUNT(*)
FROM BADGER_ds_consumer.ds_tpch_sf1.orders;
-- expected row count = 1500000
-- CONSUMER --


-- 15.6.0  Remove objects
--         Perform the next steps in the [provider-account] in browser 1.

-- 15.6.1  Remove the share

-- PROVIDER --
USE BADGER_share_db;
DESCRIBE SHARE BADGER_share;

-- Revoke access to the ORDERS table:
REVOKE SELECT ON TABLE BADGER_share_db.ds_tpch_sf1.orders
FROM SHARE BADGER_share;
-- PROVIDER --


-- 15.6.2  Confirm that the table was removed

-- PROVIDER --
DESCRIBE SHARE BADGER_share;
-- PROVIDER --


-- 15.6.3  Confirm that the ORDERS table was revoked
--         Perform the next steps in the [consumer-account] in browser 2.

-- CONSUMER --
DESC SHARE [provider-account].BADGER_SHARE;
SELECT COUNT (*) FROM BADGER_DS_CONSUMER.DS_TPCH_SF1.ORDERS;
-- should fail with SQL compilation error
-- CONSUMER --


-- 15.6.4  Verify table data

-- CONSUMER --
SELECT MIN(c_custkey)
  FROM BADGER_ds_consumer.ds_tpch_sf1.customer;
-- expected result = 1
-- CONSUMER --


-- 15.6.5  Delete a row in the CUSTOMER table shared with the consumer account
--         Perform this step on the [provider-account] in browser 1.

-- PROVIDER --
DELETE
FROM BADGER_share_db.ds_tpch_sf1.customer
WHERE c_custkey = 1;
-- expected result = 1 Rows deleted
-- PROVIDER --


-- 15.6.6  Verify the row was deleted
--         Perform this step on the [consumer-account] in browser 2.

-- CONSUMER --
SELECT MIN(c_custkey)
FROM BADGER_ds_consumer.ds_tpch_sf1.customer;

-- expected result = 2. This is different from before because the row
-- with C_CUSTKEY=1 was removed by the provider.
-- CONSUMER --


-- 15.7.0  The Power Of Secure User-defined Functions For Protecting Shared Data
--         Secure Views And Their Limitations
--         Today, most data sharing in Snowflake uses secure views. Secure views
--         are a great way for a data owner to grant other Snowflake users
--         secure access to select subsets of their data.
--         Secure views are effective for enforcing cell-level security in
--         multi-tenant situations. This includes software-as-a-service (SaaS)
--         providers granting access to each of their customers, while allowing
--         each customer to see only their specific rows of data from each
--         table. However, there is nothing preventing another user from running
--         a SELECT * query against the secure view and then exporting all the
--         data that’s visible to them.
--         In many situations, allowing a data consumer to see and export the
--         raw data is completely acceptable. However, in other situations, such
--         as when monetizing data, the most valuable analyses are often run
--         against low-level and raw data, and allowing a data consumer to
--         export the raw data is not desirable. Furthermore, when PII and PHI
--         are involved, privacy policies and government regulations often do
--         not permit providing data access to other parties.
--         Perform the next steps on the [provider-account] in browser 1.

-- 15.7.1  The Power Of Secure UDFs
--         Secure UDFs are small pieces of SQL or JavaScript code that securely
--         operate against raw data, but provide only a constrained set of
--         outputs in response to specific inputs. For example, imagine a
--         retailer that wants to allow its suppliers to see which items from
--         other suppliers are commonly sold together with theirs. This is known
--         as market basket analysis.
--         Using the TCP-DS sample data set that’s available to all users from
--         the Shares tab within Snowflake, we can run the following SQL
--         commands to create a test data set and perform a market basket
--         analysis:

-- PROVIDER --
CREATE DATABASE IF NOT EXISTS BADGER_udf_demo;
USE DATABASE BADGER_udf_demo;
CREATE SCHEMA IF NOT EXISTS BADGER_udf_demo.public;

-- The next create table command can take some time to run so let's bump up the warehouse to LARGE first.
ALTER WAREHOUSE BADGER_wh SET WAREHOUSE_SIZE=LARGE;

CREATE OR REPLACE TABLE BADGER_udf_demo.public.sales AS
  SELECT * FROM snowflake_sample_data.tpcds_sf10tcl.store_sales
    SAMPLE BLOCK (1);

SELECT 6139 AS input_item,
  ss_item_sk AS basket_item,
  COUNT(distinct ss_ticket_number) AS baskets
FROM BADGER_udf_demo.public.sales  
WHERE ss_ticket_number
  IN (SELECT ss_ticket_number
      FROM BADGER_udf_demo.public.sales
      WHERE ss_item_sk = 6139)
GROUP BY ss_item_sk
ORDER BY 3 DESC, 2;
-- PROVIDER --


-- 15.7.2  Create a Secure UDF
--         This example returns the items that sold together with item #6139.
--         This example outputs only aggregated data, which is the number of
--         times various other products are sold together, in the same
--         transaction, with item #6139. This SQL statement needs to operate
--         across all of the raw data to find the right subset of transactions.
--         To enable this type of analysis while preventing the user who is
--         performing the analysis from seeing the raw data, we wrap this SQL
--         statement in a secure UDF and add an input parameter to specify the
--         item number we are selecting for market basket analysis, as follows:

-- PROVIDER --
CREATE OR REPLACE SECURE FUNCTION
  BADGER_udf_demo.public.get_market_basket(INPUT_ITEM_SK NUMBER(38))
RETURNS TABLE (INPUT_ITEM NUMBER(38,0),
               BASKET_ITEM_SK NUMBER(38,0),
              NUM_BASKETS NUMBER(38,0))
AS
 'SELECT INPUT_ITEM_SK
       , SS_ITEM_SK BASKET_ITEM
       , COUNT(DISTINCT SS_TICKET_NUMBER) BASKETS
    FROM BADGER_UDF_DEMO.PUBLIC.SALES
    WHERE SS_TICKET_NUMBER IN
      (SELECT SS_TICKET_NUMBER
      FROM BADGER_UDF_DEMO.PUBLIC.SALES
      WHERE SS_ITEM_SK = INPUT_ITEM_SK)
GROUP BY SS_ITEM_SK
ORDER BY 3 DESC, 2';

SELECT *
FROM TABLE(BADGER_udf_demo.public.get_market_basket(6139));
-- PROVIDER --

--         We can then call this function and specify any item number as an
--         input, and we will get the same results we received when running the
--         SQL statement directly. Now, we can grant a specified user access to
--         this function while preventing the user from accessing the underlying
--         transactional data.

-- 15.7.3  How To Share Secure UDFs
--         To share a secure UDF, we can then grant usage rights on the secure
--         UDF to a Snowflake share. This gives other specified Snowflake
--         accounts the ability to run the secure UDF, but does not grant any
--         access rights to the data in the underlying tables.

-- PROVIDER --
USE DATABASE BADGER_udf_demo;

CREATE SHARE IF NOT EXISTS BADGER_udf_demo_share;

GRANT USAGE ON DATABASE BADGER_udf_demo
  TO SHARE BADGER_udf_demo_share;
GRANT USAGE ON SCHEMA BADGER_udf_demo.public
  TO SHARE BADGER_udf_demo_share;
GRANT USAGE ON FUNCTION BADGER_udf_demo.public.get_market_basket(number)
  TO SHARE BADGER_udf_demo_share;

ALTER SHARE BADGER_udf_demo_share ADD ACCOUNTS=[consumer-account];
-- PROVIDER --


-- 15.7.4  Create database from share and run query on the data consumer
--         Perform this step on the [consumer-account] in browser 2.
--         We can run the secure UDF from the share using the second account’s
--         virtual warehouse. However, from the second account, we cannot select
--         any data from the underlying tables, determine anything about the
--         names or structures of the underlying tables, or see the code behind
--         the secure UDF.

-- CONSUMER --
USE ROLE training_role;

CREATE DATABASE BADGER_udf_test
FROM SHARE [provider-account].BADGER_udf_demo_share;

GRANT IMPORTED PRIVILEGES ON DATABASE BADGER_udf_test to role public;

USE DATABASE BADGER_udf_test;
SELECT * FROM TABLE(BADGER_udf_test.public.get_market_basket(6139));

-- Reset warehouse back to Xsmall
ALTER WAREHOUSE BADGER_wh SET WAREHOUSE_SIZE=Xsmall;
-- CONSUMER --


-- 15.7.5  Examine Share from the data provider
--         Perform this step on the [provider-account] in browser 1.

-- PROVIDER --
DESCRIBE SHARE BADGER_udf_demo_share;

-- Reset warehouse back to Xsmall
ALTER WAREHOUSE BADGER_wh SET WAREHOUSE_SIZE=Xsmall;
-- PROVIDER --

--         The secure UDF is essentially using the data access rights of its
--         creator, but allowing itself to be run by another Snowflake account
--         that has access rights to run it. With Snowflake Data Sharing, the
--         compute processing for secure UDFs runs in the context of, and is
--         paid for by, the data consumer using the consumer’s virtual
--         warehouse, against the function provider’s single encrypted copy of
--         the underlying data.
--         This ability to share a secure UDF enables a myriad of secure data
--         sharing and data monetization use cases, including the ability to
--         share raw and aggregated data and powerful analytical functions,
--         while also protecting the secure UDF’s code. It also prevents other
--         parties from directly viewing or exporting the underlying encrypted
--         data.

