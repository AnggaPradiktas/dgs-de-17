
-- 10.0.0  SCENARIO: End-to-End Pipeline
--         The primary and mission-critical systems at SnowBearAir manage
--         flights at SnowBearAir. The large enterprise application is jokingly
--         referred to as the Bear Traffic Controller for the company. This
--         system pushes data regarding flights for the past minute to a cloud
--         storage location.
--         Your task is to build a pipeline that continuously loads and
--         processes flight data. This data supports and determines on-time
--         flight performance on certain SnowBearAir routes. The on-time flight
--         performance data is hosted externally and is in a semi-structured
--         Parquet format.
--         The pipeline will have the capability to continuously ingest the
--         flight route data and then will extract the Parquet data using
--         Streams & Tasks within minutes of the data arriving. After the flight
--         data is loaded, it will then be transformed and processed into three
--         (3) on-time flight categories - Early, Delayed, or Cancelled.
--         - Use Snowflake external stages to list, query, and load files.
--         - Have the PIPE object ingest data from files stored in stages.
--         - Utilize FILE FORMATs and VARIANT columns to ingest and transform
--         Parquet data efficiently.
--         - Employ prior knowledge of SQL to create tables.
--         - Use a STREAM object to query a table and then consume a set of
--         changes to the table, at the row level, between two transactional
--         points in time.
--         - Use a TASK object to define a recurring schedule for executing SQL
--         statements.
--         - Utilize prior knowledge of SQL to create and run SELECT and INSERT
--         statements.
--         - Test your basic ingestion pipeline and solve minor edge case
--         issues.
--         STARTING OBJECTS AND STATES
--         The following items are available for you to use:
--         An empty external stage and a stored procedure which generates the
--         on-time flight performance data sets in Parquet format.
--         A Snowflake stage object has been created allowing learners to bypass
--         required credentials.
--         A database named BADGER_DB, which contains the following schemas
--         that represent the data quality levels in a data engineering
--         pipeline:
--         The RAW schema contains tables that retain data in its rawest form,
--         sometimes referred to as a single source of truth.
--         The CONFORMED schema contains tables that retain data after
--         filtering, cleaning, or transformations are performed.
--         THE MODELED schema contains structured tables that are thoroughly
--         cleaned and transformed and ready for consumption by other workflows.
--         OUTCOMES
--         When you complete this lab you should have the following
--         deliverables:
--         A data pipeline that incrementally extracts Parquet data into
--         structured tables.
--         A data pipeline that automates many of the manual steps involved in
--         transforming and optimizing continuous data loads.
--         Raw flight data that is transformed and evaluated before it is
--         inserted into the correct modeled tables to determine which flights
--         are EARLY, DELAYED, or CANCELLED.

-- 10.1.0  Lab A: Generate the Flight Data for Development
--         One of the challenges for developing a continuous pipeline is
--         simulating the data that will be consumed by the pipeline. For
--         development purposes a stored procedure named stream_flight_data(...)
--         is provided. When invoked, the procedure will generate the flight
--         performance data and store it in the stage raw.class_stage as a set
--         of Parquet files.
--         Lab A Overview

-- 10.1.1  List the files in the stage
--         Use the LIST command to list files that have been staged
--         (i.e. uploaded from a local file system or unloaded from a table).
--         The command returns the name, size of the file compressed (in bytes),
--         an MD5 hash, and a timestamp when it was last updated in the stage,
--         for each file.
--         Also, specifying a path provides a scope for the LIST command,
--         reducing the amount of time required to run the command.
--         Set the context and list all the files in the BADGER path of the
--         CLASS_STAGE in the RAW schema.

ALTER SESSION SET QUERY_TAG='(BADGER) Lab - SCENARIO: End-to-End Pipeline';
USE ROLE training_role;
USE WAREHOUSE BADGER_wh;
ALTER WAREHOUSE BADGER_wh SET WAREHOUSE_SIZE = 'XSMALL';
USE DATABASE BADGER_db;
USE SCHEMA raw;

list @raw.class_stage/BADGER/;

--         Examining the output indicates there are no files in the BADGER path
--         of the CLASS_STAGE in the RAW schema.

-- 10.1.2  Generate the flight data
--         Add flight data files using the raw.stream_flight_data(...) stored
--         procedure. The procedure’s signature defines four (4) arguments:
--         RAW.stream_flight_data (FOLDER STRING, YEAR STRING, MONTH STRING,
--         AIRPORT STRING)
--         FOLDER: Output directory in our RAW.CLASS_STAGE
--         YEAR: Year between 2019 and 2015
--         MONTH: Month between 1 - 12
--         AIRPORT: Airport IATA code (SFO, SEA, ORD, etc..)
--         Try and invoke the stored procedure to generate a data set for all
--         flights in and out of Seattle-Tacoma International Airport (SEA) in
--         August of 2019, and list the contents of the stage it writes to.

CALL raw.stream_flight_data( 'BADGER' ,'2019','08','SEA');

LIST @raw.class_stage/BADGER/;

--         Examining the output now shows multiple files exist in the stage.
--         List Output
--         An external stage references data files stored in a cloud location
--         outside of Snowflake. Currently, an external stage can reference
--         cloud storage services on Amazon S3 buckets, Google Cloud Storage
--         buckets and Microsoft Azure containers.
--         Stages can also be internal, meaning the stage exists within the
--         cloud account powering your Snowflake account. Each (non-external)
--         table created automatically gets an internal stage referred to as a
--         table stage. Additionally, each user created automatically gets an
--         internal stage referred to as a user stage. Neither table stages nor
--         user stages can ever be external stages.
--         Take some time to explore the STAGE objects provided in the training
--         account.
--         Use the SHOW command to list all the stages for which you have access
--         privileges. This command can be used to list the stages for a
--         specified schema or database (or the current schema/database for the
--         session), or your entire account.
--         List the stages in the BADGER_db.RAW schema using the SHOW command:

SHOW STAGES;

--         The SHOW command output provides all the stage properties and
--         metadata. Given the output, try answering the following questions:
--         At what date and time was the stage created?
--         Which property indicates if this is an external stage?
--         Which role owns the stage?
--         To which cloud provider and region does the stage belong?
--         In addition, if you are interested in looking at the body of the
--         stored procedure named RAW.STREAM_FLIGHT_DATA(...) you can do so by
--         calling the GET_DDL(...) system function.

select get_ddl('procedure',
               'raw.stream_flight_data(varchar, varchar, varchar, varchar)');

--         The body of the function creates the files by unloading data from an
--         existing table using a file format of type Parquet.

-- 10.2.0  Lab B: Continuous Data Loading with Snowpipe
--         To set up Snowpipe to load the generated flight data, we will need to
--         create the remaining data loading objects:
--         TABLE
--         FILE FORMAT
--         PIPE
--         Lab B Overview
--         Snowpipe is Snowflake’s continuous data ingestion service. Snowpipe
--         loads data within minutes after files are added to a stage and
--         submitted for ingestion. Snowpipe enables loading data from files as
--         soon as they’re available in a stage. This means you can load data
--         from files in micro-batches, making it available to users within
--         minutes, rather than manually executing COPY statements on a schedule
--         to load larger batches.
--         Snowpipe Service

-- 10.2.1  Create the FLIGHTS table in the RAW schema
--         Snowflake natively supports semi-structured data, which means semi-
--         structured data can be loaded into relational tables without
--         requiring the definition of a schema in advance. Snowflake supports
--         loading semi-structured data directly into columns of type VARIANT.
--         Create the RAW.FLIGHTS table with a single column of type VARIANT
--         that will store the semi-structured Parquet flight data.

create or replace table raw.flights (v variant);

describe table raw.flights;

--         The output describes the single column in the table, its type, as
--         well as the default values.
--         The VARIANT data type provides native support for loading semi-
--         structured data without transformation, automatic conversion of data
--         to optimized internal storage format, and optimization for fast and
--         efficient SQL querying.
--         Explore the Semi-structured Data Types documentation for a complete
--         description of the semi-structured data types supported in Snowflake.

-- 10.2.2  Create a FILE FORMAT to parse Parquet files
--         Snowflake supports creating named file format objects, which are
--         schema-level objects that encapsulate all of the required format
--         information. Named file format objects can then be used in all the
--         places where you can specify individual file format options, thereby
--         helping to streamline the data loading process for similarly-
--         formatted data.
--         Create the RAW.FLIGHTS_PARQ named FILE FORMAT object that describes
--         the staged data as Parquet to load into the RAW.FLIGHTS table.

create or replace file format raw.flights_parq
  type = 'parquet'
  compression = auto
;

show file formats;

--         In the command above, the Parquet file format options are pretty
--         straight forward in that they specify the format type and
--         automatically determine the Parquet file compression used.
--         File format options specify the type of data contained in a file, as
--         well as other related characteristics about the format of the data.
--         The file format options you can specify are different depending on
--         the type of data you plan to load.
--         Explore the File Formats documentation for a complete description of
--         all file formats and their specific options that are supported in
--         Snowflake.

-- 10.2.3  Create a pipe to load data into the FLIGHTS table
--         A pipe is a named, first-class Snowflake object containing a COPY
--         INTO statement used by the Snowpipe service. The COPY INTO statement
--         identifies the source location of the data files (i.e., the stage)
--         and a target table. All data types are supported.
--         Create a new pipe called RAW.FLIGHTS_PIPE which defines the COPY INTO
--         statement to load the data from the @raw.class_stage/BADGER/ stage
--         into the RAW.FLIGHTS table.

CREATE OR REPLACE PIPE raw.flights_pipe AS
 COPY INTO raw.flights FROM @raw.class_stage/BADGER/
FILE_FORMAT= (FORMAT_NAME=raw.flights_parq);

show pipes;

--         The output of the show command confirms the pipe was created and
--         shows the COPY INTO command in the definition column.
--         For our pipe object, Snowflake establishes a single ingestion queue
--         to sequence data files awaiting loading. As new data files are
--         discovered in a stage Snowpipe appends them to the queue.
--         Explore the Introduction to Snowpipe topic for a complete description
--         and overview of Snowpipe.

-- 10.2.4  Refresh the pipe and check the data before and after loading it
--         Now that all the necessary objects have been created, the next step
--         is to add the staged files into the Snowpipe ingestion queue.
--         Use the ALTER PIPE command to refresh the pipe and send a
--         notification for each file staged file to the Snowpipe ingest queue.

ALTER PIPE raw.flights_pipe REFRESH;

--         The output of the command shows the file and the notification status.
--         ALTER PIPE REFRESH

-- 10.2.5  Check the file loading status
--         Use the system function system$pipe_status(...) to check the status
--         of the pipe and the files.

SELECT system$pipe_status('raw.flights_pipe');

--         The output returns a JSON object containing the execution state and
--         the number of files pending, in the ingestion queue.
--         Pipe Status
--         Explore the Pipe Status output documentation for a complete
--         description of the key/value pairs that make up the status.

-- 10.2.6  Check the load history
--         Use the COPY_HISTORY function to list the files Snowpipe loaded in
--         the last hour. If it returns zero (0) rows, wait a few minutes and
--         repeat the query.

SELECT *
FROM TABLE(information_schema.copy_history(table_name=>'raw.flights'
        , start_time=> dateadd(hours, -1, current_timestamp())));

--         The output shows pipe load activity for the COPY INTO statements for
--         the last hour.
--         Copy Load History
--         Query the rows in the table. Confirm there is now data within the
--         table.

SELECT * FROM raw.flights;
SELECT count(*) from raw.flights;

--         Query Flights Table
--         The PIPE object has its own metadata to track files that were
--         previously loaded so as to avoid loading duplicate data.
--         Try running the ALTER PIPE REFRESH command and verify the previously
--         loaded files are ignored.
--         In addition, truncating the table does not delete the Snowpipe
--         metadata. You need to drop the pipe entirely to remove the metadata.

-- 10.3.0  Lab C: Set Up a Stream to Transform Raw Flight Data Into Conformed
--         Flight Data
--         The purpose of a Snowflake stream object is to track any changes to a
--         table including inserts, updates, and deletes, which can then be
--         consumed by other DML statements. One of the more typical uses of
--         stream objects is CDC, also known as Change Data Capture. Our setup
--         in Snowpipe is configured to ingest the flight data automatically
--         from the Parquet files in the @RAW.CLASS_STAGE/BADGER stage and then
--         copy the data into the RAW.FLIGHTS table.
--         Lab C Overview
--         We now need to create a stream that will be responsible for capturing
--         the new rows of flight data and performing the transformation from
--         RAW data to CONFORMED data.

-- 10.3.1  Truncate the RAW.FLIGHTS table
--         Before setting up the stream on the table, truncate the table to
--         remove any remaining data loaded before the stream existed.

TRUNCATE TABLE raw.flights;

SELECT count(*) from raw.flights;

--         Truncating the table using the TRUNCATE TABLE command does NOT delete
--         the Snowpipe file loading metadata as stated above.

-- 10.3.2  Create streams to monitor data changes against the RAW.FLIGHTS table
--         Create three stream objects named RAW.STREAM_FLIGHTS_EARLY,
--         RAW.STREAM_FLIGHTS_DELAYED, RAW.STREAM_FLIGHTS_CANCELLED on the
--         RAW.FLIGHTS table.
--         Each stream object will be used to check when new flight data is
--         added to the raw.flights table and allow further processing to
--         determine if that particular flight arrival time was early, delayed,
--         or cancelled.

CREATE OR REPLACE STREAM raw.stream_flights_early ON table raw.flights;

CREATE OR REPLACE STREAM raw.stream_flights_delayed ON table raw.flights;

CREATE OR REPLACE STREAM raw.stream_flights_cancelled ON table raw.flights;

SHOW STREAMS;

--         Show Streams
--         Snowflake Streams objects do not physically store, contain, or copy
--         any table data. In fact, when the first Stream for a table is
--         created, a pair of hidden columns are actually added to the source
--         table and begin storing change tracking metadata there. The Stream
--         records timeline markers against the source table, with the
--         difference between the previous version of the table and current
--         version called the offset. The Stream then leverages this offset, and
--         the source table change tracking metadata, to show changes made over
--         time.
--         Query the data in the streams:

SELECT * FROM raw.stream_flights_early;

SELECT * FROM raw.stream_flights_delayed;

SELECT * FROM raw.stream_flights_cancelled;

--         The output from the above displays the same columns as the source
--         table RAW.FLIGHTS along with the following additional columns:
--         The output from the SELECT commands above show that no data has
--         changed in the RAW.FLIGHTS table. This means the stream is empty and
--         the three (3) metadata columns at the end of the table are blank.
--         Query Empty Stream

-- 10.3.3  Add files to the stage to update to RAW.FLIGHTS tables
--         To populate the stream, you will need to perform a DML operation on
--         the RAW.FLIGHTS table.
--         Use a stored procedure to generate a data set for all flights into
--         and out of Seattle-Tacoma International Airport (SEA) in July of
--         2019, list the contents of the stage, and refresh the
--         RAW.FLIGHTS_PIPE.

CALL raw.stream_flight_data( 'BADGER' ,'2019','07','SEA');

LIST @raw.class_stage/BADGER/;

ALTER PIPE raw.flights_pipe REFRESH;


-- 10.3.4  Check the pipe status

SELECT system$pipe_status('raw.flights_pipe');

--         Wait for the pipe status to show the submitted files and report there
--         are zero (0) pending files.
--         Zero Pending Files
--         Also, querying the INFORMATION_SCHEMA.COPY_HISTORY(...) table
--         function will provide further details of the COPY INTO command in the
--         pipe.

select *
from table(information_schema.copy_history(
  table_name=>'raw.flights',
  start_time=>dateadd(hour, -1, current_timestamp)));

--         Copy History

-- 10.3.5  Read the contents of each stream
--         Query each stream to identify any data changes in each:

SELECT * FROM raw.stream_flights_early;

SELECT * FROM raw.stream_flights_delayed;

SELECT * FROM raw.stream_flights_cancelled;

--         The following figure shows the RAW.STREAM_FLIGHTS_EARLY stream data
--         as the result of the pipe loading the flight data into the
--         RAW.FLIGHTS table.
--         Stream Query Results
--         Using each stream’s metadata columns, we can build the required logic
--         to load the MODELED.FLIGHTS_EARLY, MODELED.FLIGHTS_DELAYED and
--         MODELED.FLIGHTS_CANCELLED tables.

-- 10.4.0  Lab D: Load the EARLY, DELAYED, and CANCELLED Tables in the MODELED
--         Schema
--         Use each stream to write SQL statements to load the
--         MODELED.FLIGHTS_EARLY, MODELED.FLIGHTS_DELAYED AND
--         MODELED.FLIGHTS_CANCELLED tables with data which then reflects
--         changes made to the RAW.FLIGHTS table.
--         Lab D Overview

-- 10.4.1  Create a FLIGHTS_EARLY table in the MODELED schema
--         Use the following SQL statement to create the MODELED.FLIGHTS_EARLY
--         table.

CREATE OR REPLACE TABLE modeled.flights_early
(
 fl_date               date,
 tail_num              string,
 op_carrier_fl_num     int,
 origin                string,
 dest                  string,
 origin_city_name      string,
 dest_city_name        string,
 dep_time              string,
 dep_delay             number(38,2),
 arr_time              string,
 arr_delay             number(38,2),
 crs_elapsed_time      number(38,2),
 actual_elapsed_time   number(38,2),
 cancelled             int  
);


-- 10.4.2  Process the STREAM_FLIGHTS_EARLY stream
--         Now that the stream has data, we can write an INSERT statement to
--         load data into the MODELED.FLIGHTS_EARLY table for records in the
--         stream. In addition to using the stream metadata we can also filter
--         on the flight data arrival time column to determine which entries in
--         the flight arrival time were early.

insert into modeled.flights_early (
  fl_date,
  tail_num,
  op_carrier_fl_num,
  origin,
  dest,
  origin_city_name,
  dest_city_name,
  dep_time,
  dep_delay,
  arr_time,
  arr_delay,
  crs_elapsed_time,
  actual_elapsed_time,
  cancelled)
select
  v:FL_DATE::date,
  v:TAIL_NUM::string,
  v:OP_CARRIER_FL_NUM::int,
  v:ORIGIN::string,
  v:DEST::string,
  v:ORIGIN_CITY_NAME::string,
  v:DEST_CITY_NAME::string,
  v:DEP_TIME::string,
  v:DEP_DELAY::number(38,2),
  v:ARR_TIME::string,
  v:ARR_DELAY::number(38,2) as ARR_DELAY,
  v:CRS_ELAPSED_TIME::number(38,2),
  v:ACTUAL_ELAPSED_TIME::number(38,2),
  v:CANCELLED::int
from raw.stream_flights_early
where metadata$action = 'INSERT'
AND ARR_DELAY <= 0;

--         Verify the table was loaded and the stream has been purged.

SELECT * FROM modeled.flights_early;

SELECT * FROM raw.stream_flights_early;


-- 10.4.3  Create a FLIGHTS_DELAYED table in the MODELED schema
--         Use the following SQL statement to create the MODELED.FLIGHTS_DELAYED
--         table.

CREATE OR REPLACE TABLE modeled.flights_delayed
(
 fl_date               date,
 tail_num              string,
 op_carrier_fl_num     int,
 origin                string,
 dest                  string,
 origin_city_name      string,
 dest_city_name        string,
 dep_time              string,
 dep_delay             number(38,2),
 arr_time              string,
 arr_delay             number(38,2),
 crs_elapsed_time      number(38,2),
 actual_elapsed_time   number(38,2),
 cancelled             int  
);


-- 10.4.4  Process the STREAM_FLIGHTS_DELAYED stream
--         Run the following insert statement to query the
--         STREAM_FLIGHTS_DELAYED stream and insert each row into the
--         FLIGHTS_DELAYED table. Note also the change to the following
--         condition:
--         Run the following commands:

insert into modeled.flights_delayed (
  fl_date,
  tail_num,
  op_carrier_fl_num,
  origin,
  dest,
  origin_city_name,
  dest_city_name,
  dep_time,
  dep_delay,
  arr_time,
  arr_delay,
  crs_elapsed_time,
  actual_elapsed_time,
  cancelled)
select
  v:FL_DATE::date,
  v:TAIL_NUM::string,
  v:OP_CARRIER_FL_NUM::int,
  v:ORIGIN::string,
  v:DEST::string,
  v:ORIGIN_CITY_NAME::string,
  v:DEST_CITY_NAME::string,
  v:DEP_TIME::string,
  v:DEP_DELAY::number(38,2),
  v:ARR_TIME::string,
  v:ARR_DELAY::number(38,2) as ARR_DELAY,
  v:CRS_ELAPSED_TIME::number(38,2),
  v:ACTUAL_ELAPSED_TIME::number(38,2),
  v:CANCELLED::int
from raw.stream_flights_delayed
where metadata$action = 'INSERT'
AND ARR_DELAY > 0;

--         Verify the table was loaded and the stream has been purged.

SELECT * FROM modeled.flights_delayed;

SELECT * FROM raw.stream_flights_delayed;


-- 10.4.5  Create a FLIGHTS_CANCELLED table in the MODELED schema
--         Use the following SQL statement to create the
--         MODELED.FLIGHTS_CANCELLED table.

CREATE OR REPLACE TABLE modeled.flights_cancelled
(
 fl_date               date,
 tail_num              string,
 op_carrier_fl_num     int,
 origin                string,
 dest                  string,
 origin_city_name      string,
 dest_city_name        string,
 dep_time              string,
 dep_delay             number(38,2),
 arr_time              string,
 arr_delay             number(38,2),
 crs_elapsed_time      number(38,2),
 actual_elapsed_time   number(38,2),
 cancelled             int
);


-- 10.4.6  Process the STREAM_FLIGHTS_CANCELLED stream
--         Run the following insert statement to query the
--         STREAM_FLIGHTS_CANCELLED stream and insert each row into the
--         FLIGHTS_CANCELLED table. Note also the change to the following
--         condition:
--         Run the following commands:

insert into modeled.flights_cancelled (
  fl_date,
  tail_num,
  op_carrier_fl_num,
  origin,
  dest,
  origin_city_name,
  dest_city_name,
  dep_time,
  dep_delay,
  arr_time,
  arr_delay,
  crs_elapsed_time,
  actual_elapsed_time,
  cancelled)
select
  v:FL_DATE::date,
  v:TAIL_NUM::string,
  v:OP_CARRIER_FL_NUM::int,
  v:ORIGIN::string,
  v:DEST::string,
  v:ORIGIN_CITY_NAME::string,
  v:DEST_CITY_NAME::string,
  v:DEP_TIME::string,
  v:DEP_DELAY::number(38,2),
  v:ARR_TIME::string,
  v:ARR_DELAY::number(38,2),
  v:CRS_ELAPSED_TIME::number(38,2),
  v:ACTUAL_ELAPSED_TIME::number(38,2),
  v:CANCELLED::int as CANCELLED
from raw.stream_flights_cancelled
where metadata$action = 'INSERT'
AND CANCELLED = 1;

--         Verify the table was loaded and the stream has been purged.

SELECT * FROM modeled.flights_cancelled;

SELECT * FROM raw.stream_flights_cancelled;


-- 10.5.0  Lab E: Orchestrate the Pipeline Using Tasks
--         Manually executing each INSERT command is starting to get tiresome.
--         Luckily you don’t have to execute it manually. Tasks allow us to
--         define and orchestrate the logic.
--         Using a task you can schedule each INSERT statement to run on a
--         recurring basis and execute only if there is data in each stream.
--         Lab E Overview

-- 10.5.1  Create a warehouse to run tasks
--         Tasks require compute resources in order to run, and these can be
--         either user managed or Snowflake-managed. Here you will create a user
--         managed warehouse for your tasks:

create warehouse if not exists BADGER_task_wh
  auto_suspend = 60
  initially_suspended = true;

alter warehouse BADGER_task_wh set warehouse_size=xsmall;  

use warehouse BADGER_wh;


-- 10.5.2  Create a scheduled TASK object to run the INSERT commands
--         Execute the following SQL commands to create three (3) tasks. These
--         commands should not generate any errors.
--         The following task will execute every minute and run only if the
--         RAW.STREAM_FLIGHTS_EARLY stream has data in it.

create or replace task raw.task_early_flights
  warehouse = BADGER_task_wh
  schedule = '1 minute'
  when system$stream_has_data('raw.stream_flights_early')
as
insert into modeled.flights_early (
  fl_date,
  tail_num,
  op_carrier_fl_num,
  origin,
  dest,
  origin_city_name,
  dest_city_name,
  dep_time,
  dep_delay,
  arr_time,
  arr_delay,
  crs_elapsed_time,
  actual_elapsed_time,
  cancelled)
select
  v:FL_DATE::date,
  v:TAIL_NUM::string,
  v:OP_CARRIER_FL_NUM::int,
  v:ORIGIN::string,
  v:DEST::string,
  v:ORIGIN_CITY_NAME::string,
  v:DEST_CITY_NAME::string,
  v:DEP_TIME::string,
  v:DEP_DELAY::number(38,2),
  v:ARR_TIME::string,
  v:ARR_DELAY::number(38,2) as ARR_DELAY,
  v:CRS_ELAPSED_TIME::number(38,2),
  v:ACTUAL_ELAPSED_TIME::number(38,2),
  v:CANCELLED::int
from raw.stream_flights_early
where metadata$action = 'INSERT' AND ARR_DELAY < 0;

--         The following task will execute every minute and run only if the
--         RAW.STREAM_FLIGHTS_DELAYED stream has data in it.

create or replace task raw.task_delayed_flights
  warehouse = BADGER_task_wh
  schedule = '1 minute'
  when system$stream_has_data('raw.stream_flights_delayed')
as
insert into modeled.flights_delayed (
  fl_date,
  tail_num,
  op_carrier_fl_num,
  origin,
  dest,
  origin_city_name,
  dest_city_name,
  dep_time,
  dep_delay,
  arr_time,
  arr_delay,
  crs_elapsed_time,
  actual_elapsed_time,
  cancelled)
select
  v:FL_DATE::date,
  v:TAIL_NUM::string,
  v:OP_CARRIER_FL_NUM::int,
  v:ORIGIN::string,
  v:DEST::string,
  v:ORIGIN_CITY_NAME::string,
  v:DEST_CITY_NAME::string,
  v:DEP_TIME::string,
  v:DEP_DELAY::number(38,2),
  v:ARR_TIME::string,
  v:ARR_DELAY::number(38,2) as ARR_DELAY,
  v:CRS_ELAPSED_TIME::number(38,2),
  v:ACTUAL_ELAPSED_TIME::number(38,2),
  v:CANCELLED::int
from raw.stream_flights_delayed
where metadata$action = 'INSERT'
AND ARR_DELAY > 0;

--         The following task will execute every minute and run only if the
--         RAW.STREAM_FLIGHTS_CANCELLED stream has data in it.

create or replace task raw.task_cancelled_flights
  warehouse = BADGER_task_wh
  schedule = '1 minute'
  when system$stream_has_data('raw.stream_flights_cancelled')
as
insert into modeled.flights_cancelled (
  fl_date,
  tail_num,
  op_carrier_fl_num,
  origin,
  dest,
  origin_city_name,
  dest_city_name,
  dep_time,
  dep_delay,
  arr_time,
  arr_delay,
  crs_elapsed_time,
  actual_elapsed_time,
  cancelled)
select
  v:FL_DATE::date,
  v:TAIL_NUM::string,
  v:OP_CARRIER_FL_NUM::int,
  v:ORIGIN::string,
  v:DEST::string,
  v:ORIGIN_CITY_NAME::string,
  v:DEST_CITY_NAME::string,
  v:DEP_TIME::string,
  v:DEP_DELAY::number(38,2),
  v:ARR_TIME::string,
  v:ARR_DELAY::number(38,2),
  v:CRS_ELAPSED_TIME::number(38,2),
  v:ACTUAL_ELAPSED_TIME::number(38,2),
  v:CANCELLED::number(38,2) as CANCELLED
from raw.stream_flights_cancelled
where metadata$action = 'INSERT'
AND CANCELLED = 1;


-- 10.5.3  List the newly created tasks

show tasks;

--         The output of the SHOW command displays the task properties and
--         metadata. Examine the STATE column and notice the tasks are in the
--         SUSPENDED state.

-- 10.5.4  Make the tasks active
--         After creating a task, you must execute the command ALTER TASK ...
--         RESUME before the task will run based on the parameters specified in
--         the task creation.

ALTER TASK raw.task_early_flights RESUME;

ALTER TASK raw.task_delayed_flights RESUME;

ALTER TASK raw.task_cancelled_flights RESUME;

--         Repeat the next few steps to load more flight data into the pipeline
--         similar to what we did previously.

-- 10.5.5  Clear the RAW.FLIGHTS table and generate the flight data
--         Invoke the stored procedure to generate a data set for all flights in
--         and out of Seattle-Tacoma International Airport (SEA) in June of 2019
--         and list the contents of the stage:

TRUNCATE TABLE raw.flights;
SELECT COUNT(*) FROM raw.flights;

SELECT system$pipe_status('raw.flights_pipe');

CALL raw.stream_flight_data( current_user() ,'2019','06','SEA');

LIST @raw.class_stage/BADGER/;

ALTER PIPE raw.flights_pipe REFRESH;


-- 10.5.6  View the task history

select * from table(information_schema.task_history())
  where scheduled_time > dateadd(minute, -5, current_time())
  and state <> 'SCHEDULED'
  and DATABASE_NAME = 'BADGER_DB'
  order by completed_time desc;


-- 10.5.7  Check the status of the pipe

SELECT system$pipe_status('raw.flights_pipe');

--         How long is it until the next task runs?

select timestampdiff(second, current_timestamp, scheduled_time) next_run, scheduled_time, name, state
  from table(information_schema.task_history())
  where state = 'SCHEDULED' order by completed_time desc;

--         Query the modeled tables and streams.

SELECT COUNT(*) FROM modeled.flights_early;
SELECT * FROM raw.stream_flights_early;


SELECT COUNT(*) FROM modeled.flights_delayed;
SELECT * FROM raw.stream_flights_delayed;


SELECT COUNT(*) FROM modeled.flights_cancelled;
SELECT * FROM raw.stream_flights_cancelled;

--         You should be good to go. Check your work. If everything looks good,
--         continue on…
--         As data is loaded into the stage our Snowpipe ingests the data files
--         into the raw.flights table and our three (3) tasks kick off
--         automatically to insert the correct flight data to the correct
--         modeled.flights_early, modeled.flights_delayed AND
--         modeled.flights_cancelled tables.

-- 10.5.8  Stop the tasks
--         To save on compute resources it’s a good idea to SUSPEND each task.
--         Run the ALTER command to change each TASK state from STARTED to
--         SUSPENDED.

ALTER TASK raw.task_early_flights SUSPEND;

ALTER TASK raw.task_delayed_flights SUSPEND;

ALTER TASK raw.task_cancelled_flights SUSPEND;

SHOW TASKS;


-- 10.5.9  Finally, let’s remove the query tag as the required SQL work for this
--         lab is complete.

ALTER SESSION UNSET QUERY_TAG;


-- 10.6.0  Lab F: (OPTIONAL) You Try It - The Snowpipe Auto Ingest Feature
--         The remaining steps require your own AWS Account access to the S3
--         bucket, which is not provided or in scope of this lab.
--         For this lab we needed to manually refresh the pipe to notify it to
--         query the stage and process any pending files. This process is not
--         practical in a production environment. This is where the Snowpipe
--         Auto Ingest feature is used.
--         The AUTO_INGEST=true parameter states Snowpipe should read event
--         notifications sent from an S3 bucket to an SQS queue when new data is
--         ready to load.
--         The completed AUTO_INGEST steps require AWS access to the S3 bucket,
--         which is not in scope of this lab. For informational purposes the
--         syntax is provided here to set the AUTO_INGEST parameter on the PIPE.

CREATE OR REPLACE PIPE raw.flights_pipe AUTO_INGEST=true AS
 COPY INTO raw.flights FROM @raw.class_stage/BADGER/
FILE_FORMAT= (FORMAT_NAME=raw.flights_parq);

--         Follow these instructions to complete the full setup of Snowpipe data
--         loads automatically using Amazon SQS (Simple Queue Service)
--         notifications for an S3 bucket in your own environment.
