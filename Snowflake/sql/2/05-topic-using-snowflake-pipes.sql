
-- 5.0.0   TOPIC: Using Snowflake Pipes
--         There are two types of Snowpipes; basic pipes and continuous load
--         pipes. In this exercise you will work with basic pipes.

-- 5.1.0   Setup a Basic Snowpipe Lab

-- 5.1.1   Set your context and create the CITIBIKE schema:

ALTER SESSION SET QUERY_TAG='(BADGER) Lab - TOPIC: Using Snowflake Pipes';
USE ROLE training_role;
USE WAREHOUSE BADGER_query_wh;
ALTER WAREHOUSE BADGER_query_wh SET WAREHOUSE_SIZE = 'XSMALL';
USE DATABASE BADGER_db;
CREATE SCHEMA IF NOT EXISTS citibike;
USE SCHEMA citibike;


-- 5.1.2   Create the CITIBIKE.TRIPS table:

create or replace table trips (
  tripduration integer
  ,starttime timestamp
  ,stoptime timestamp
  ,start_station_id integer
  ,start_station_name string
  ,start_station_latitude float
  ,start_station_longitude float
  ,end_station_id integer
  ,end_station_name string
  ,end_station_latitude float
  ,end_station_longitude float
  ,bikeid integer
  ,membership_type string
  ,usertype string
  ,birth_year integer
  ,gender integer);


-- 5.1.3   Create a pipe for the CITIBIKE.TRIPS table and verify that the pipe
--         exists:

CREATE OR REPLACE PIPE BADGER_db.citibike.trips_pipe  AS
  COPY INTO BADGER_DB.citibike.trips
    FROM
    (
      SELECT *
      FROM @TRAINING_DB.TRAININGLAB.CLASS_STAGE/COURSE/ADVANCED/BADGER
    )
    FILE_FORMAT=(FORMAT_NAME=training_db.traininglab.MYGZIPPIPEFORMAT);

SHOW PIPES;


-- 5.1.4   Unload data from the existing CITIBIKE database onto the stage
--         referenced when creating the pipe:

COPY INTO @TRAINING_DB.TRAININGLAB.CLASS_STAGE/COURSE/ADVANCED/BADGER/citibike1_
FROM
  (
    SELECT *
    FROM CITIBIKE.SCHEMA1.TRIPS SAMPLE(10 ROWS)
  )
  FILE_FORMAT = (FORMAT_NAME = training_db.traininglab.MYGZIPPIPEFORMAT);


-- 5.1.5   List the files on the cloud storage stage managed by Snowflake
--         (a.k.a. internal stage).
--         Note that you can use either list or its abbreviated form ls to show
--         the files in a stage.

ls @TRAINING_DB.TRAININGLAB.CLASS_STAGE/COURSE/ADVANCED/BADGER;

--         You should see new files loaded into the stage location specified in
--         the copy statement above.

-- 5.1.6   Refresh the stage with new files and list the stage contents before
--         and after loading it:

COPY INTO @TRAINING_DB.TRAININGLAB.CLASS_STAGE/COURSE/ADVANCED/BADGER/citibike2_
  FROM  (SELECT *  FROM CITIBIKE.SCHEMA1.TRIPS SAMPLE(10 ROWS))
   FILE_FORMAT = (FORMAT_NAME = training_db.traininglab.MYGZIPPIPEFORMAT);

ls @TRAINING_DB.TRAININGLAB.CLASS_STAGE/COURSE/ADVANCED/BADGER;


-- 5.1.7   Stage more data onto the internal stage and refresh the pipe to
--         trigger its loading activity. It will detect new, unloaded files in
--         this stage, add to its ingest queue, then process through them,
--         reading data from these files and loading into the CITIBIKE.TRIPS
--         table:

COPY INTO @TRAINING_DB.TRAININGLAB.CLASS_STAGE/COURSE/ADVANCED/BADGER/citibike3_
  FROM  (SELECT *  FROM CITIBIKE.SCHEMA1.TRIPS SAMPLE(10 ROWS))
   FILE_FORMAT = (FORMAT_NAME = training_db.traininglab.MYGZIPPIPEFORMAT);

ALTER PIPE BADGER_db.citibike.trips_pipe REFRESH;


-- 5.1.8   List files in the stage:

ls @TRAINING_DB.TRAININGLAB.CLASS_STAGE/COURSE/ADVANCED/BADGER;


-- 5.1.9   Query the table to see the freshly loaded data (this should take
--         around ~1 minute to populate):

SELECT * FROM trips;


-- 5.1.10  Check load history:

SELECT *
FROM TABLE(information_schema.copy_history(table_name=>'trips', start_time=> dateadd(hours, -1, current_timestamp())));


-- 5.1.11  Check the pipe status:
--         You can run the following system function to check the current status
--         of your pipe. If you query this at a point in time when the pipe is
--         actively loading you should see pendingFileCount > 0:

select system$pipe_status('BADGER_db.citibike.trips_pipe');


-- 5.1.12  Finally, let’s remove the query tag as the work for this lab is
--         complete.

ALTER SESSION UNSET QUERY_TAG;


