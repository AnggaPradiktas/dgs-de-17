
-- 5.0.0   Review the Query Profile
--         Expect this lab to take approximately 15 minutes.

-- 5.1.0   Run the first explain plan

-- 5.1.1   Navigate to [Worksheets] and create a new worksheet named Query
--         Profile.

-- 5.1.2   Set the worksheet context as follows:

-- 5.1.3   Alternatively, execute the following SQL:

USE ROLE TRAINING_ROLE;
CREATE WAREHOUSE IF NOT EXISTS BADGER_WH;
ALTER WAREHOUSE BADGER_WH SET WAREHOUSE_SIZE = xsmall;
USE SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL;


-- 5.1.4   Disable the query result cache:

ALTER SESSION SET USE_CACHED_RESULT=FALSE;


-- 5.1.5   Run an explain plan on the sample query with the WHERE clause using
--         the customer.c_customer_sh:

EXPLAIN
SELECT c_customer_sk,
        c_customer_id, 
        c_last_name, 
        (ca_street_number || ' ' || ca_street_name),
        ca_city,  ca_state  
    FROM customer, customer_address
    WHERE c_customer_id = ca_address_id
    AND c_customer_sk between 100000 and 600000
    ORDER BY ca_city, ca_state;


-- 5.1.6   Examine the explain plan

-- 5.2.0   Run the same query without the explain plan

-- 5.2.1   Run the SQL below:

SELECT c_customer_sk,
        c_customer_id, 
        c_last_name, 
        (ca_street_number || ' ' || ca_street_name),
        ca_city,  ca_state  
    FROM customer, customer_address
    WHERE c_customer_id = ca_address_id
    AND c_customer_sk between 100000 and 600000
    ORDER BY ca_state, ca_city;


-- 5.3.0   Perform a Review of the Query Profile

-- 5.3.1   In the worksheet in the Results section click Query ID.

-- 5.3.2   Once the ID shows, click on it; the detail page for the query is
--         displayed:
--         Query Id Details

-- 5.3.3   Click the profile tab and review the Query Profile:

-- 5.3.4   Navigate back to the worksheet.

-- 5.4.0   Modify the query to run with the where clause using the ca_address_id
--         on the customer_address table

-- 5.4.1   Run the SQL below:

EXPLAIN
SELECT c_customer_sk,
        c_customer_id, 
        c_last_name, 
        (ca_street_number || ' ' || ca_street_name),
        ca_city,  ca_state  
    FROM customer, customer_address
    WHERE c_customer_id = ca_address_id
    AND ca_address_sk between 100000 and 600000
    ORDER BY ca_city, ca_state;


-- 5.4.2   Compare this plan to the one created above.

-- 5.5.0   Run the same query without the explain plan

-- 5.5.1   Run the SQL below:

SELECT c_customer_sk,
        c_customer_id, 
        c_last_name, 
        (ca_street_number || ' ' || ca_street_name),
        ca_city,  ca_state  
    FROM customer, customer_address
    WHERE c_customer_id = ca_address_id
    AND ca_address_sk between 100000 and 600000
    ORDER BY ca_state, ca_city;


-- 5.5.2   Open the query profile for this.

-- 5.5.3   Navigate to [History]. You should see the query you just ran at the
--         top of the table.

-- 5.5.4   Click on another query ID from the list - this is another way to get
--         the query profile page.

-- 5.5.5   View the profile for the query you selected.

-- 5.5.6   Run some additional queries on your own or select other queries from
--         the [History] table, and view the query results.

-- 5.5.7   If you haven’t already, make sure you resize the warehouse to XSMALL
--         and then suspend it:

ALTER WAREHOUSE BADGER_WH SET WAREHOUSE_SIZE=XSmall;
ALTER WAREHOUSE BADGER_WH SUSPEND;

