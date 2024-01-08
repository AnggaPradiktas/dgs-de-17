
-- 4.0.0   Using Snowflake Pipes
--         In this lab you will learn and practice the following:
--         - How to use basic pipes
--         - How to manually refresh a basic pipe
--         There are two (2) types of Snowpipes, namely basic pipes and
--         continuous load pipes. In this exercise you will work with basic
--         pipes which you must manually refresh. A continuous load pipe will be
--         refreshed automatically either from an API or the cloud provider’s
--         notification system.

-- 4.1.0   Set Up a Basic Snowpipe

-- 4.1.1   Open a new worksheet or Create Worksheet from SQL File and set your
--         context

USE ROLE training_role;
USE WAREHOUSE BADGER_wh;
USE DATABASE BADGER_db;
CREATE SCHEMA citibike;
USE SCHEMA citibike;


-- 4.1.2   Create a Citibike trips table.

CREATE OR REPLACE TABLE trips (
    tripduration              INTEGER,
    starttime                 TIMESTAMP,
    stoptime                  TIMESTAMP,
    start_station_id          INTEGER,
    start_station_name        STRING,
    start_station_latitude    FLOAT,
    start_station_longitude   FLOAT,
    end_station_id            INTEGER,
    end_station_name          STRING,
    end_station_latitude      FLOAT,
    end_station_longitude     FLOAT,
    bikeid                    INTEGER,
    membership_type           STRING,
    usertype                  STRING,
    birth_year                INTEGER,
    gender                    INTEGER);


-- 4.1.3   Create a pipe to load the Citibike Trips table.

CREATE OR REPLACE PIPE BADGER_db.citibike.trips_pipe  AS
  COPY INTO BADGER_db.citibike.trips
    FROM (SELECT *
    FROM @TRAINING_DB.TRAININGLAB.CLASS_STAGE/COURSE/ADVANCED/BADGER)
    FILE_FORMAT=(FORMAT_NAME=training_db.traininglab.mygzippipeformat);

SHOW PIPES;


-- 4.2.0   Load the Trips Table

-- 4.2.1   Unload data from the existing CITIBIKE database onto the stage.

COPY INTO @TRAINING_DB.TRAININGLAB.CLASS_STAGE/COURSE/ADVANCED/BADGER/citibike1_
  FROM  (SELECT *  FROM CITIBIKE.SCHEMA1.TRIPS SAMPLE(10 ROWS))
   FILE_FORMAT = (FORMAT_NAME = training_db.traininglab.mygzippipeformat);


-- 4.2.2   List files on the stage.

LIST @TRAINING_DB.TRAININGLAB.CLASS_STAGE/COURSE/ADVANCED/BADGER;

--         You should see new files loaded onto the stage location specified in
--         the COPY statement.

-- 4.2.3   Refresh the stage and check the data after loading it:

ALTER PIPE BADGER_db.citibike.trips_pipe REFRESH;

SELECT system$pipe_status('BADGER_db.citibike.trips_pipe');

SELECT * FROM trips;


-- 4.2.4   Stage more data and refresh the pipe.

COPY INTO @TRAINING_DB.TRAININGLAB.CLASS_STAGE/COURSE/ADVANCED/BADGER/citibike2_
  FROM  (SELECT *  FROM CITIBIKE.SCHEMA1.TRIPS SAMPLE(10 ROWS))
   FILE_FORMAT = (FORMAT_NAME = training_db.traininglab.MYGZIPPIPEFORMAT)
   OVERWRITE = TRUE;

ALTER PIPE BADGER_db.citibike.trips_pipe REFRESH;


-- 4.2.5   List files on the stage.

LS @TRAINING_DB.TRAININGLAB.CLASS_STAGE/COURSE/ADVANCED/BADGER;


-- 4.2.6   Query the table to see the freshly loaded data (this might take a few
--         minutes).

SELECT * FROM trips;


-- 4.2.7   Check the load history.

SELECT *
   FROM TABLE(information_schema.copy_history(
     TABLE_NAME =>'trips',
     START_TIME => dateadd(hours, -1, current_timestamp()))
);


