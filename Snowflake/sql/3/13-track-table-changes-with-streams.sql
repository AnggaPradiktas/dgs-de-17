
-- 13.0.0  Track Table Changes with Streams
--         In this lab you will learn and practice the following:
--         - Create Basic Table Streams
--         - Query Table Streams
--         - Exploring Delta and Append Streams
--         - Pairing Streams and Tasks
--         In this exercise, we will introduce the basic workflow around streams
--         as well as explore the differences between delta and append-only
--         streams.

-- 13.1.0  Create Basic Table Streams

-- 13.1.1  Open a Worksheet or Create Worksheet from SQL File and set your
--         context

USE ROLE training_role;
CREATE WAREHOUSE IF NOT EXISTS BADGER_wh;
USE WAREHOUSE BADGER_wh;
USE SCHEMA BADGER_db.public;


-- 13.1.2  Create a source table

CREATE OR REPLACE TABLE data_staging
(
    cr_order_number     NUMBER(38,0),
    cr_item_sk          NUMBER(38,0),
    cr_return_quantity  NUMBER(38,0),
    cr_net_loss         NUMBER(7,2)
);


-- 13.1.3  Create downstream tables to split the source data for different needs
--         - in this case inventory and cost:

CREATE OR REPLACE TABLE data_quantity
(
    cr_order_number     NUMBER(38,0),
    cr_item_sk          NUMBER(38,0),
    cr_return_quantity  NUMBER(38,0)
);

CREATE OR REPLACE TABLE data_cost
(
    CR_ORDER_NUMBER     NUMBER(38,0),
    CR_NET_LOSS         NUMBER(7,2)
);


-- 13.1.4  Create the stream object on the source table:

CREATE OR REPLACE STREAM data_check ON TABLE data_staging;


-- 13.1.5  Load some sample data into the source table:

INSERT INTO data_staging
SELECT cr_order_number,
       cr_item_sk,
       cr_return_quantity,
       cr_net_loss
FROM snowflake_sample_data.tpcds_sf10tcl.catalog_returns LIMIT 50;


-- 13.1.6  After the data has loaded, query the table stream object.

SELECT
    cr_order_number,
    cr_item_sk,
    cr_return_quantity,
    cr_net_loss,
    metadata$action,
    metadata$isupdate,
    metadata$row_id
FROM data_check
ORDER BY cr_order_number LIMIT 10;

--         There are 3 metadata columns included in every row of a stream.

-- 13.2.0  Query Table Streams
--         Next we will access the stream and write the data into downstream
--         tables.

-- 13.2.1  Execute statements within a transaction block to consume the stream
--         data
--         The stream will reset to a new offset when the transaction is
--         committed.

BEGIN;

    INSERT INTO data_quantity (cr_order_number,
                               cr_item_sk,
                               cr_return_quantity)
    SELECT cr_order_number, cr_item_sk, cr_return_quantity
    FROM data_check t
    WHERE METADATA$ACTION = 'INSERT';

    INSERT INTO data_cost (cr_order_number, cr_net_loss)
    SELECT t.cr_order_number, t.cr_net_loss
    FROM data_staging t
    JOIN data_quantity q
        ON t.cr_order_number = q.cr_order_number
        AND t.cr_item_sk = q.cr_item_sk;

COMMIT;


-- 13.2.2  Take a look at the data in the downstream tables:

SELECT * FROM data_quantity;

SELECT * FROM data_cost;


-- 13.2.3  Query the table stream to see how it looks after records have been
--         consumed:

SELECT * FROM data_check;


-- 13.3.0  Explore Delta and Append Streams
--         Streams come in two (2) primary varieties, Delta and Append.
--         To illustrate the difference you’ll create a second source table and
--         stream, populating it with the same records as the original source
--         table.

-- 13.3.1  Create a new source table.

CREATE OR REPLACE TABLE data_staging_append
    (
     CR_ORDER_NUMBER        NUMBER(38,0),
     CR_ITEM_SK             NUMBER(38,0),
     CR_RETURN_QUANTITY     NUMBER(38,0),
     CR_NET_LOSS            NUMBER(7,2)
    );


-- 13.3.2  Load the same set of data into both source tables:

TRUNCATE TABLE data_staging;

INSERT INTO data_staging
    SELECT cr_order_number, cr_item_sk, cr_return_quantity, cr_net_loss
FROM snowflake_sample_data.tpcds_sf10tcl.catalog_returns LIMIT 50;

INSERT INTO data_staging_append
    SELECT * FROM data_staging;


-- 13.3.3  Create a delta stream on the table data_staging:

CREATE OR REPLACE STREAM delta ON TABLE data_staging;

--         A delta stream captures any cumulative changes to the source table.

-- 13.3.4  Create an append stream on the table data_staging_append:

CREATE OR REPLACE STREAM append_only ON TABLE data_staging_append APPEND_ONLY=TRUE;

--         An append stream only captures inserts to the source table.

-- 13.3.5  Perform the same UPDATE operation on both tables:
--         For this step look at the sample data you loaded into the
--         data_staging and pick a cr_return_quantity to update. Then change the
--         code where ?? is to the number you picked.

SELECT * FROM data_staging;

UPDATE data_staging
   SET cr_return_quantity = 0
   WHERE cr_return_quantity = ??;

UPDATE data_staging_append
   SET cr_return_quantity = 0
   WHERE cr_return_quantity = ??;


-- 13.3.6  Check the status of the table streams:

SELECT cr_order_number,
      cr_item_sk,
      cr_return_quantity,
      cr_net_loss,
      metadata$action,
      metadata$isupdate,
      metadata$row_id
FROM delta
LIMIT 10;

SELECT cr_order_number,
       cr_item_sk,
       cr_return_quantity,
       cr_net_loss,
       metadata$action,
       metadata$isupdate,
       metadata$row_id
FROM append_only
LIMIT 10;


-- 13.3.7  Undo the previous update:
--         Using the number you picked before and edit the SQL below to put the
--         data back to what it was.

UPDATE data_staging
   SET cr_return_quantity = ??
   WHERE cr_return_quantity = 0;

UPDATE data_staging_append
   SET cr_return_quantity = ??
   WHERE cr_return_quantity = 0;


-- 13.3.8  Check the status of the streams after the second update.

SELECT cr_order_number,
       cr_item_sk,
       cr_return_quantity,
       cr_net_loss,
       metadata$action,
       metadata$isupdate,
       metadata$row_id
FROM DELTA
LIMIT 10;

SELECT cr_order_number,
       cr_item_sk,
       cr_return_quantity,
       cr_net_loss,
       metadata$action,
       metadata$isupdate,
       metadata$row_id
FROM append_only
LIMIT 10;


-- 13.4.0  Pair Streams and Tasks
--         Streams and tasks can be used together to track changes to data over
--         time without the need for manual intervention.

-- 13.4.1  Create a basic data staging (source) table with a table stream and
--         downstream tables to use for the exercise:

CREATE OR REPLACE TABLE data_staging
(
     CR_ORDER_NUMBER        NUMBER(38,0),
     CR_ITEM_SK             NUMBER(38,0),
     CR_RETURN_QUANTITY     NUMBER(38,0),
     CR_NET_LOSS            NUMBER(7,2)
);

CREATE OR REPLACE STREAM data_check ON TABLE data_staging;


-- 13.4.2  Create downstream tables to use for the exercise:

CREATE OR REPLACE TABLE data_quantity
(
     CR_ORDER_NUMBER        NUMBER(38,0),
     CR_ITEM_SK             NUMBER(38,0),
     CR_RETURN_QUANTITY     NUMBER(38,0)
);

CREATE OR REPLACE TABLE data_cost
(
    CR_ORDER_NUMBER         NUMBER(38,0),
    CR_NET_LOSS             NUMBER(7,2)
);


-- 13.4.3  Create a stored procedure that performs your transformations.
--         The stored procedure moves data downstream from our staging table.

CREATE OR REPLACE PROCEDURE usp_load_prod()
RETURNS STRING NOT NULL
LANGUAGE javascript
AS
$$
  var my_sql_command = ""

  var my_sql_command = "INSERT INTO data_quantity \
      (cr_order_number, cr_item_sk, cr_return_quantity) \
      SELECT cr_order_number, cr_item_sk, cr_return_quantity \
      FROM data_check t \
      WHERE METADATA$ACTION = 'INSERT'";

  var statement1 = snowflake.createStatement( {sqlText: my_sql_command} );
  var result_set1 = statement1.execute();

  var my_sql_command = "INSERT INTO data_cost (cr_order_number, cr_net_loss) \
                        SELECT t.cr_order_number, t.cr_net_loss \
                        FROM data_staging t \
                          JOIN data_quantity q \
                        ON t.cr_order_number = q.cr_order_number \
                        AND t.cr_item_sk = q.cr_item_sk;";

  var statement2 = snowflake.createStatement( {sqlText: my_sql_command} );
  var result_set2 = statement2.execute();

  return my_sql_command; // Statement returned for info/debug purposes
$$;


-- 13.4.4  Create a task to run the stored procedure on a regular basis.
--         The task will run on a schedule. If it finds data in the table
--         stream, it will execute the stored procedure:

CREATE OR REPLACE TASK capture_data
  WAREHOUSE = animal_task_wh
  SCHEDULE = 'USING cron * * * * * UTC'
WHEN
  SYSTEM$STREAM_HAS_DATA('data_check')
AS
  CALL usp_load_prod();

ALTER TASK capture_data RESUME;

SHOW TASKS;


-- 13.4.5  To test the task, insert some data into the staging table:

INSERT INTO data_staging
  SELECT cr_order_number, cr_item_sk,
  cr_return_quantity, cr_net_loss
FROM snowflake_sample_data.tpcds_sf10tcl.catalog_returns LIMIT 50;

SELECT * FROM data_check LIMIT 20;


-- 13.4.6  After a couple of minutes, check the downstream tables to verify the
--         task executed:

SELECT TOP 10 * FROM data_quantity;

SELECT TOP 10 * FROM data_cost;


-- 13.4.7  Finally, suspend the task to avoid any unwanted credit spending:

ALTER TASK capture_data SUSPEND;


