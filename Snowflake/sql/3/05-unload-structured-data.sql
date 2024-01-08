
-- 5.0.0   Unload Structured Data
--         In this lab you will learn and practice the following:
--         - Unload a Pipe-Delimited File to an Internal Stage
--         - Unload Part of a Table
--         - JOIN and Unload a Table
--         Similar to data loading, Snowflake supports bulk export (i.e. unload)
--         of data from a database table into flat, delimited text files.

-- 5.1.0   Unload a Pipe-Delimited File to an Internal Stage

-- 5.1.1   Open a new worksheet or Create Worksheet from SQL File and set your
--         context:

USE ROLE training_role;
CREATE WAREHOUSE IF NOT EXISTS BADGER_wh;
USE WAREHOUSE BADGER_wh;
CREATE DATABASE IF NOT EXISTS BADGER_db;
USE BADGER_db.public;


-- 5.1.2   Create a fresh version of the REGION table with 5 records to unload:

CREATE OR REPLACE TABLE region AS
(SELECT * FROM snowflake_sample_data.tpch_sf1.region);


-- 5.1.3   Unload the data to the REGION table stage.
--         Remember that a table stage is automatically created for each table.
--         You will use MYPIPEFORMAT for the unload:

COPY INTO @%region
FROM region
FILE_FORMAT = (FORMAT_NAME = training_db.traininglab.mypipeformat);


-- 5.1.4   List the stage and verify the data is there:

LIST @%region;


-- 5.1.5   (OPTIONAL) Download the files to your local system.
--         Use the GET command to download all files in the REGION table stage
--         to local directory.
--         The Snowsight does not support the GET command. If you have the
--         SnowSQL command line client installed, use it to connect to Snowflake
--         and execute the GET command.

-- GET @%region file:///<path to dir> ; -- this is for Linux or macOS
-- GET @%region file://c:<path to dir>; -- this is for Windows

--         After the files are downloaded to your local file system, open them
--         with an editor and see what they contain.

-- 5.1.6   Remove the file from the REGION table’s stage:

REMOVE @%region;


-- 5.2.0   Unload Part of a Table

-- 5.2.1   Create a new table from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS:

CREATE TABLE new_orders AS
(SELECT * FROM snowflake_sample_data.tpch_sf1.orders);


-- 5.2.2   Unload the columns o_orderkey, o_orderstatus, and o_orderdate from
--         your new table, into the table’s stage:
--         Remember that a table stage is automatically created for every table.
--         Use the default file format.

COPY INTO @%new_orders FROM
(SELECT o_orderkey, o_orderstatus, o_orderdate FROM new_orders);


-- 5.2.3   Verify the output is in the stage:

LIST @%new_orders;


-- 5.2.4   (OPTIONAL) Download the files to your local system.
--         Use the GET command to download all files in the new_orders table
--         stage to local directory.
--         The Snowsight does not support the GET command. If you have the
--         SnowSQL command line client installed, use it to connect to Snowflake
--         and execute the GET command.

-- GET @%new_orders file:///<path> -- for Linux or macOS
-- GET @%new_orders file://c:<path -- For Windows

--         How many files did you get? At what point did COPY INTO decide to
--         split the files?

-- 5.2.5   Remove the files from the stage:

REMOVE @%new_orders;


-- 5.2.6   Repeat the unload with the selected columns, but this time specify
--         SINGLE=TRUE in your COPY INTO command.
--         Also provide a name for the output file as part of the COPY INTO:

COPY INTO @%new_orders/new_orders.csv.gz FROM
(SELECT o_orderkey, o_orderstatus, o_orderdate FROM new_orders)
SINGLE=TRUE;


-- 5.2.7   Verify the single file output is in the stage:

LIST @%new_orders;


-- 5.2.8   (OPTIONAL) Download the files to your local system.
--         Use the GET command to download all files in the new_orders table
--         stage to local directory.
--         The Snowsight does not support the GET command. If you have the
--         SnowSQL command line client installed, use it to connect to Snowflake
--         and execute the GET command.

-- GET @%new_orders file:///<path> -- for Linux or macOS
-- GET @%new_orders file://c:<path -- For Windows


-- 5.2.9   Remove the file from the stage:

REMOVE @%new_orders;


-- 5.3.0   JOIN and Unload a Table

-- 5.3.1   Run a SELECT with a JOIN on the REGION and NATION tables.

SELECT *
FROM snowflake_sample_data.tpch_sf1.region r
JOIN snowflake_sample_data.tpch_sf1.nation n ON r.r_regionkey = n.n_regionkey;


-- 5.3.2   Create a named internal stage.

CREATE STAGE BADGER_stage;


-- 5.3.3   Unload the JOINed data into the stage you created.

COPY INTO @BADGER_stage FROM
(SELECT * FROM snowflake_sample_data.tpch_sf1.region r 
JOIN snowflake_sample_data.tpch_sf1.nation n
ON r.r_regionkey = n.n_regionkey);


-- 5.3.4   Verify the file is in the stage.

LIST @BADGER_stage;


-- 5.3.5   (OPTIONAL) Download the files to your local system.
--         Use the GET command to download all files in the @mystage stage to
--         local directory.
--         The Snowsight does not support the GET command. If you have the
--         SnowSQL command line client installed, use it to connect to Snowflake
--         and execute the GET command.

-- GET @mystage file:///<path> -- for Linux or macOS
-- GET @mystage file://c:<path -- For Windows


-- 5.3.6   Remove the file from the stage.

REMOVE @BADGER_stage;


-- 5.3.7   Remove the stage.

DROP STAGE BADGER_stage;


