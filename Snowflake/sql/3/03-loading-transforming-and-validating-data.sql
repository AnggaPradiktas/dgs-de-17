
-- 3.0.0   Loading, Transforming and Validating Data
--         In this lab you will learn and practice the following:
--         - How to load data into Snowflake using external stages and file
--         formats
--         - How to find detect load errors VALIDATION_MODE option on a COPY
--         statement

-- 3.1.0   Load Structured Data
--         This exercise will load the region.tbl file into a REGION table in
--         your Database. The region.tbl file is pipe (|) delimited. It has no
--         header and contains the following five (5) rows:
--         Note that in the region.tbl file there is a delimiter at the end of
--         every line, which by default is interpreted as an additional column
--         by the COPY INTO statement.
--         The files required for this lab are in an external stage.

-- 3.1.1   Navigate to Worksheets and create a new worksheet or Create Worksheet
--         from SQL File

-- 3.1.2   Set the Worksheet contexts as follows:

USE ROLE training_role;
CREATE WAREHOUSE IF NOT EXISTS BADGER_load_wh
   WAREHOUSE_SIZE=XSmall
   INITIALLY_SUSPENDED=True
   AUTO_SUSPEND=300;
USE WAREHOUSE BADGER_load_wh;
CREATE DATABASE IF NOT EXISTS BADGER_db;
USE DATABASE BADGER_db;
USE SCHEMA public;


-- 3.1.3   Create the staging tables by running the following statements

CREATE OR REPLACE TABLE region (
       r_regionkey NUMBER(38,0) NOT NULL,
       r_name      VARCHAR(25)  NOT NULL,
       r_comment   VARCHAR(152)
);
CREATE OR REPLACE TABLE nation (
       n_nationkey NUMBER(38,0) NOT NULL,
       n_name      VARCHAR(25)  NOT NULL,
       n_regionkey NUMBER(38,0) NOT NULL,
       n_comment   VARCHAR(152)
);
CREATE OR REPLACE TABLE supplier (
       s_suppkey   NUMBER(38,0) NOT NULL,
       s_name      VARCHAR(25)  NOT NULL,
       s_address   VARCHAR(40)  NOT NULL,
       s_nationkey NUMBER(38,0) NOT NULL,
       s_phone     VARCHAR(15)  NOT NULL,
       s_acctbal   NUMBER(12,2) NOT NULL,
       s_comment   VARCHAR(101)
);
CREATE OR REPLACE TABLE part (
       p_partkey     NUMBER(38,0) NOT NULL,
       p_name        VARCHAR(55)  NOT NULL,
       p_mfgr        VARCHAR(25)  NOT NULL,
       p_brand       VARCHAR(10)  NOT NULL,
       p_type        VARCHAR(25)  NOT NULL,
       p_size        NUMBER(38,0) NOT NULL,
       p_container   VARCHAR(10)  NOT NULL,
       p_retailprice NUMBER(12,2) NOT NULL,
       p_comment     VARCHAR(23)
);
CREATE OR REPLACE TABLE partsupp (
       ps_partkey    NUMBER(38,0) NOT NULL,
       ps_suppkey    NUMBER(38,0) NOT NULL,
       ps_availqty   NUMBER(38,0) NOT NULL,
       ps_supplycost NUMBER(12,2) NOT NULL,
       ps_comment    VARCHAR(199)
);
CREATE OR REPLACE TABLE customer (   
       c_custkey    NUMBER(38,0) NOT NULL,   
       c_name       VARCHAR(25)  NOT NULL,   
       c_address    VARCHAR(40)  NOT NULL,   
       c_nationkey  NUMBER(38,0) NOT NULL,   
       c_phone      VARCHAR(15)  NOT NULL,   
       c_acctbal    NUMBER(12,2) NOT NULL,   
       c_mktsegment VARCHAR(10),   
       c_comment    VARCHAR(117)  
);
CREATE OR REPLACE TABLE orders (
       o_orderkey      NUMBER(38,0) NOT NULL,
       o_custkey       NUMBER(38,0) NOT NULL,
       o_orderstatus   VARCHAR(1)   NOT NULL,
       o_totalprice    NUMBER(12,2) NOT NULL,
       o_orderdate     DATE         NOT NULL,
       o_orderpriority VARCHAR(15)  NOT NULL,
       o_clerk         VARCHAR(15)  NOT NULL,
       o_shippriority  NUMBER(38,0) NOT NULL,
       o_comment       VARCHAR(79)  NOT NULL
);
CREATE OR REPLACE TABLE lineitem (
       l_orderkey      NUMBER(38,0) NOT NULL,
       l_partkey       NUMBER(38,0) NOT NULL,
       l_suppkey       NUMBER(38,0) NOT NULL,
       l_linenumber    NUMBER(38,0) NOT NULL,
       l_quantity      NUMBER(12,2) NOT NULL,
       l_extendedprice NUMBER(12,2) NOT NULL,
       l_discount      NUMBER(12,2) NOT NULL,
       l_tax           NUMBER(12,2) NOT NULL,
       l_returnflag    VARCHAR(1)   NOT NULL,
       l_linestatus    VARCHAR(1)   NOT NULL,
       l_shipdate      DATE         NOT NULL,
       l_commitdate    DATE         NOT NULL,
       l_receiptdate   DATE         NOT NULL,
       l_shipinstruct  VARCHAR(25)  NOT NULL,
       l_shipmode      VARCHAR(10)  NOT NULL,
       l_comment       VARCHAR(44)  NOT NULL
);
CREATE OR REPLACE TABLE countrygeo (
       cg_nationkey NUMBER(38,0),
       cg_capital   VARCHAR(100),
       cg_lat       NUMBER(20,10),
       cg_lon       NUMBER(20,10),
       cg_altitude  NUMBER(38,0)
);


-- 3.1.4   Find the region.tbl file in the external stage with list and a regex
--         pattern.

LIST @training_db.traininglab.ed_stage/load/lab_files/ pattern='.*region.*';


-- 3.1.5   Load the data from the external stage to the REGION Table using the
--         MYPIPEFORMAT file format.

DESCRIBE FILE FORMAT training_db.traininglab.mypipeformat;

COPY INTO region
  FROM @training_db.traininglab.ed_stage/load/lab_files/
  FILES = ( 'region.tbl' )
  FILE_FORMAT = ( FORMAT_NAME = training_db.traininglab.mypipeformat );

--         The file formats required for the lab steps have been created and are
--         all located in the TRAINING_DB.TRAININGLAB schema.

-- 3.1.6   Select and review the data in the REGION Table:

SELECT * FROM region;


-- 3.1.7   Preview the REGION table in the WebUI using the sidebar.
--         First, click on your database BADGER_db in the navigator. Locate and
--         click on the PUBLIC schema. In the list of tables, find the REGION
--         table and click to select. Then click the small magnifying class icon
--         in bottom left pane to preview the data:
--         Select Preview Data

-- 3.2.0   Loading Data and File Sizes
--         When loading data, file size matters. Snowflake recommends using
--         files sizes of 100MB to 250MB of compressed data for both bulk
--         loading using COPY and for streaming using Snowpipe. Both macOS and
--         Linux support a file splitting utility.

-- 3.2.1   Set Context:

USE ROLE training_role;
USE SCHEMA BADGER_db.public;
USE WAREHOUSE BADGER_load_wh;


-- 3.2.2   Create a named stage:

CREATE OR REPLACE TEMPORARY STAGE BADGER_stage;


-- 3.2.3   Change your warehouse size to small:

ALTER WAREHOUSE BADGER_load_wh SET WAREHOUSE_SIZE = SMALL;


-- 3.2.4   Download a file from the Citibike table to the stage you created:

COPY INTO @BADGER_stage/citibike/singlefile/citibike.tbl
  FROM citibike.schema1.trips
  FILE_FORMAT=(FORMAT_NAME=training_db.traininglab.mypipeformat)
  SINGLE=true
  MAX_FILE_SIZE=5368709120;

--         You will receive an error message - this is because the file is too
--         large to be downloaded as a single file.


-- 3.2.5   Confirm the error:
--         Run the LIST command on the stage location and the query will produce
--         no results confirming what we expected.

LIST @BADGER_stage/citibike/singlefile;


-- 3.2.6   Re-run the command to limit the amount of data unloaded:

COPY INTO @BADGER_stage/citibike/singlefile/citibike.tbl
  FROM (SELECT * FROM citibike.schema1.trips LIMIT 20000000)
  FILE_FORMAT=(FORMAT_NAME=training_db.traininglab.mypipeformat)
  SINGLE=true
  MAX_FILE_SIZE=5368709120;


-- 3.2.7   List the file on the stage:

LIST @BADGER_stage/citibike/singlefile;


-- 3.2.8   Now resize your warehouse to LARGE and unload the data to the stage
--         without using the SINGLE option:

ALTER WAREHOUSE BADGER_load_wh SET WAREHOUSE_SIZE = LARGE;

COPY INTO @BADGER_STAGE/citibike/multiplefiles/citibike_
  FROM (SELECT * FROM citibike.schema1.trips)
  FILE_FORMAT=(FORMAT_NAME=training_db.traininglab.mypipeformat)
  SINGLE=FALSE;

LIST @BADGER_stage/citibike/multiplefiles;

ALTER WAREHOUSE BADGER_load_wh set warehouse_size = SMALL;


-- 3.3.0   Load Semi-Structured Data
--         This exercise will load tables from text files that are in an
--         external stage. You will load text files from an external stage using
--         the Web UI.

-- 3.3.1   View files in stage training_db.traininglab.ed_stage/coredata stage

LIST @training_db.traininglab.ed_stage/coredata/TCPH/TCPH_SF100;

--Set a variable to hold the query id of the ls command
SET sf100 = LAST_QUERY_ID();

--         There are many files to load for some of the larger tables. For these
--         larger tables, you will get better performance using more cores (a
--         larger virtual warehouse). Therefore, for this load exercise you will
--         alter the size of the virtual warehouse to an X-Large. In a real-
--         world application, you would create a virtual warehouse for data
--         loading workloads sized to approximately match the number of files
--         you plan to load.

-- 3.3.2   Alter the BADGER_load_wh and increase its size:

ALTER WAREHOUSE BADGER_load_wh SET WAREHOUSE_SIZE='X-LARGE';


-- 3.3.3   Query all of the unique directories that follow TCP_SF100 in the
--         stage.

SELECT DISTINCT REGEXP_SUBSTR(  -- see note below
                     "name"     -- column name that contains the string
                     ,'.*TCPH_SF100\/(.*)\/'  --regular expression string
                     ,1         -- start from the beginning of the string
                     ,1         -- find the first occurrance match
                     ,'e'       -- extract the sub-matches
                     ,1         -- return the first sub match
                    ) AS  DIRECTORY_NAMES
   FROM TABLE(RESULT_SCAN($sf100));

--         For more information see documentation at REGEXP_SUBSTR
--         Also, to understand how regular expression string works visit
--         Regex101

-- 3.3.4   Load data into the CUSTOMER table.

COPY INTO BADGER_db.public.customer
  FROM @training_db.traininglab.ed_stage
  PATTERN='.*/CUSTOMER/.*tbl'
  FILE_FORMAT = (FORMAT_NAME = training_db.traininglab.mypipeformat)
  ON_ERROR = 'CONTINUE';


-- 3.3.5   Load the remaining tables.
--         Using the above COPY INTO command as a template, run additional COPY
--         INTO statements to load each table in your BADGER_db.public schema.
--         Remaining tables:
--         - PARTSUPP
--         - ORDERS
--         - LINEITEM
--         - NATION
--         - SUPPLIER
--         - PART

-- 3.3.6   Count the number of rows from the newly populated tables:

SELECT table_name, row_count
FROM information_schema.tables
WHERE TABLE_SCHEMA = CURRENT_SCHEMA();


-- 3.3.7   BONUS Try re-writing the above query to also get the number of files
--         in each directory.

-- ANSWER AVAILABLE IN ANSWER KEY


-- 3.4.0   Load Semi-Structured Parquet Data
--         This exercise will load a Parquet data file using different methods.

-- 3.4.1   Empty the REGION Table in the PUBLIC schema of your BADGER_db:

TRUNCATE TABLE region;


-- 3.4.2   Confirm that the region.parquet file is in the External Stage:

LIST @training_db.traininglab.ed_stage/load/lab_files
    PATTERN = '.*region.*';

LIST @training_db.traininglab.ed_stage/load/lab_files
    PATTERN = '.*region.*parquet$';


-- 3.4.3   Create the file format for the Parquet file in the current schema:

CREATE OR REPLACE FILE FORMAT myparquetformat
    TYPE = PARQUET
    COMPRESSION = NONE;

SELECT *
FROM @training_db.traininglab.ed_stage/load/lab_files/region.parquet
(FILE_FORMAT => myparquetformat);


-- 3.4.4   Query the data in the region.parquet file from the external stage:

SELECT
      $1,
      $1:_COL_0::number,
      $1:_COL_1::varchar,
      $1:_COL_2::varchar
 FROM @training_db.traininglab.ed_stage/load/lab_files/region.parquet
 (FILE_FORMAT => myparquetformat);


-- 3.4.5   Reload the REGION Table from the region.parquet file:

COPY INTO region
FROM (
      SELECT $1:_COL_0::number,
             $1:_COL_1::varchar,
             $1:_COL_2::varchar
      FROM @training_db.traininglab.ed_stage/load/lab_files/
     )
FILES = ('region.parquet')
FILE_FORMAT = (FORMAT_NAME = myparquetformat);


-- 3.4.6   View the data:

SELECT * FROM region;


-- 3.5.0   Load Semi-Structured JSON Data
--         This exercise will load a JSON data file.

-- 3.5.1   Confirm that the countrygeo.json file is in the External Stage:

LIST @training_db.traininglab.ed_stage PATTERN = '.*countrygeo.*';


-- 3.5.2   Query the file directly from the stage:

SELECT *
FROM @training_db.traininglab.ed_stage/load/lab_files/countrygeo.json
(FILE_FORMAT => 'training_db.traininglab.myjsonformat');


-- 3.5.3   Load the COUNTRYGEO Table from the countrygeo.json file:

CREATE OR REPLACE TABLE countrygeo (CG_V variant);

COPY INTO countrygeo (CG_V)
FROM (SELECT $1
      FROM @training_db.traininglab.ed_stage/load/lab_files/countrygeo.json )
      FILE_FORMAT = ( FORMAT_NAME = training_db.traininglab.myjsonformat )
      ON_ERROR = 'continue';


-- 3.5.4   View the data:

SELECT * FROM countrygeo;


-- 3.6.0   Load Fixed Format Data
--         Loading fixed format data takes advantage of Snowflake’s ability to
--         transform data upon load. The approach is to load the data into a
--         VARCHAR or STRING column and use Snowflake functions to transform the
--         data and load it.

-- 3.6.1   Set Context

USE ROLE training_role;
USE WAREHOUSE BADGER_load_wh;
USE DATABASE BADGER_db;
USE SCHEMA public;


-- 3.6.2   Create the target NATION table from TCPH data

CREATE TABLE nation_tbl LIKE training_db.traininglab.nation;


-- 3.6.3   Validate that the source file exists on the stage

LIST @training_db.traininglab.ed_stage/coredata/TCPH/FIXFORMAT;


-- 3.6.4   Create a File Format object.

CREATE FILE FORMAT BADGER_fixed TYPE = 'CSV'
                   COMPRESSION = 'AUTO'
                   FIELD_DELIMITER = 'NONE'
                   RECORD_DELIMITER = '\n'
                   FIELD_OPTIONALLY_ENCLOSED_BY = 'NONE'
                   ESCAPE = 'NONE' ;


-- 3.6.5   Use the copy statement to load the data from Stage

COPY INTO nation_tbl
  FROM (SELECT CAST(SUBSTR($1,1,2) AS NUMBER),
               SUBSTR($1,3,12),
               CAST(SUBSTR($1,19,1) AS NUMBER),
               SUBSTR($1,20,114)
        FROM '@training_db.traininglab.ed_stage/coredata/tcph/fixformat'
  )
          FILE_FORMAT=(FORMAT_NAME=BADGER_fixed);


-- 3.6.6   Query the loaded data

SELECT * FROM nation_tbl;

--         Sample data:

-- 3.7.0   Detect File Format Problems with VALIDATION_MODE
--         Use Snowflake’s VALIDATION_MODE option on a COPY statement to
--         demonstrate Snowflake’s pre-load error detection mechanism.

-- 3.7.1   Set the following context:

USE ROLE training_role;
USE DATABASE BADGER_db;
CREATE OR REPLACE schema validate;
USE SCHEMA validate;
USE WAREHOUSE BADGER_load_wh;


-- 3.7.2   Create a table
--         Create a table that will have data loaded into.

CREATE OR REPLACE TABLE aircraft_types (
    ac_typeid        NUMBER,
    ac_group         NUMBER,
    ssd_name         VARCHAR,
    manufacturer     VARCHAR,
    long_name        VARCHAR,
    short_name       VARCHAR,
    begin_date       DATE,
    end_date         DATE
 );


-- 3.7.3   Create a CSV file format
--         Create a file format to define the CSV properties of the data to be
--         loaded.

CREATE OR REPLACE FILE FORMAT aircraft_types_csv
      TYPE = 'CSV'
      COMPRESSION = 'NONE'
      FIELD_DELIMITER = ','
      RECORD_DELIMITER = '\n'
      SKIP_HEADER = 1
      FIELD_OPTIONALLY_ENCLOSED_BY = '\042'
      TRIM_SPACE = FALSE
      ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
      ESCAPE = 'NONE'
      ESCAPE_UNENCLOSED_FIELD = '\134'
      DATE_FORMAT = 'YYYY-MM-DD'
      TIMESTAMP_FORMAT = 'AUTO'
      NULL_IF = ('\\N')
;


-- 3.7.4   List the files in the stages
--         Using the list command check to see what files already exist in the
--         stage.

LIST @training_db.traininglab.datasets_stage/sba_data/air_craft/t_aircraft_types;


-- 3.7.5   Load the CSV file into the table with VALIDATION_MODE
--         Using the copy command load the t_aircraft_types.csv file from the
--         external stage into the table and provide the file format. Set the
--         VALIDATION_MODE to test if the table, file and file format are valid
--         and not load any data.

COPY INTO aircraft_types 
FROM
@training_db.traininglab.datasets_stage/sba_data/air_craft/t_aircraft_types.csv
       FILE_FORMAT = (FORMAT_NAME = aircraft_types_csv)
       VALIDATION_MODE = 'RETURN_ERRORS'
;

--         Examine the results. The ERROR column indicates a conversion issue
--         with the date format in the CSV file and the file format object. The
--         file format defined the DATA_FORMAT as YYYY-MM-DD but the BEGIN_DATE
--         column value has a DATE_FORMAT of MM/DD/YY

-- 3.7.6   Fix the DATE_FORMAT

CREATE OR REPLACE FILE FORMAT aircraft_types_csv
       TYPE = 'CSV'
       COMPRESSION = 'NONE'
       FIELD_DELIMITER = ','
       SKIP_HEADER = 1
       FIELD_OPTIONALLY_ENCLOSED_BY = '\042'
       TRIM_SPACE = FALSE
       ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
       ESCAPE = 'NONE'
       ESCAPE_UNENCLOSED_FIELD = '\134'
       DATE_FORMAT = 'MM/DD/YY'
       TIMESTAMP_FORMAT = 'AUTO'
       NULL_IF = ('\\N')
;


-- 3.7.7   Load the CSV file into the table with VALIDATION_MODE

COPY INTO aircraft_types
FROM
@training_db.traininglab.datasets_stage/sba_data/air_craft/t_aircraft_types.csv
FILE_FORMAT = (FORMAT_NAME = aircraft_types_csv)
VALIDATION_MODE = 'RETURN_ERRORS'
;

--         Examine the result. No errors occurred in VALIDATION_MODE so we could
--         address the issue with the file format changed.

-- 3.7.8   Load the CSV file into the table

COPY INTO aircraft_types
FROM
@training_db.traininglab.datasets_stage/sba_data/air_craft/t_aircraft_types.csv
FILE_FORMAT = (FORMAT_NAME = aircraft_types_csv)
;


-- 3.7.9   Query the table
--         Count the number of rows in the table.

SELECT COUNT(*) FROM aircraft_types;


-- 3.7.10  How to check the load status
--         You can check the load status using information_schema.copy_history
--         and information_schema.load_history

SELECT *
FROM table(information_schema.copy_history(table_name=>'aircraft_types',
  start_time=> dateadd(hours, -1, current_timestamp())))
WHERE status = 'Loaded';

SELECT * FROM information_schema.load_history
ORDER BY last_load_time desc
LIMIT 10;

--         Examine the results of each query. Review and change the arguments to
--         explore getting addition information on loading status using the copy
--         command.

-- 3.8.0   Detect load failures with ON_ERROR set to CONTINUE
--         In this exercise, we will create similar table and load data with
--         valid and invalid data. The valid data will be loaded into the table
--         and the invalid data row will be identified but not loaded.

-- 3.8.1   Create a table
--         Create a table that will have data loaded into.

CREATE or replace TABLE aircraft_types_2 (
   ac_typeid        NUMBER,
   ac_group         NUMBER,
   ssd_name         VARCHAR,
   manufacturer     VARCHAR,
   long_name        VARCHAR,
   short_name       VARCHAR,
   begin_date       DATE,
   end_date         DATE
 );


-- 3.8.2   Load the CSV file into the table using ON_ERROR
--         Using the copy command load the t_aircraft_types_broken.csv file from
--         the external stage into the table and provide the file format.
--         Set ON_ERROR to CONTINUE to load the valid rows only.

COPY INTO aircraft_types_2
FROM
@training_db.traininglab.datasets_stage/sba_data/air_craft/t_aircraft_types_broken.csv
FILE_FORMAT = (FORMAT_NAME = aircraft_types_csv)
ON_ERROR = CONTINUE
;

--         Examine the results. The STATUS column shows the PARTIALLY_LOADED and
--         additional information on where first error that occurred during the
--         load.

-- 3.8.3   Capture all load error from a query ID
--         Set the COPY statement QUERY ID variable.

SET last_copy_query_id = (select last_query_id());
SELECT $last_copy_query_id;


-- 3.8.4   Use VALIDATE table function
--         Pass the COPY command query id to VALIDATE table function and returns
--         all the errors encountered during the load.

SELECT * FROM table( 
VALIDATE(aircraft_types_2, job_id => $last_copy_query_id)
);

--         Examine the results. The ERROR column indicates there is an invalid
--         numeric value in multiple rows of the CSV file.

-- 3.9.0   Diagnose load errors
--         Loading error can be loaded into a table, file or query the file
--         directly to help diagnose data loading issues.

-- 3.9.1   Create table containing load errors

CREATE OR REPLACE TABLE error_aircraft_types_2 AS
SELECT rejected_record
FROM table(VALIDATE(aircraft_types_2, job_id => $last_copy_query_id));

SELECT * FROM error_aircraft_types_2;

--         Examine the results. Having the result in a table allows you perform
--         data correction on the failed data.

-- 3.9.2   Unload errors to a file

COPY INTO @~/aircraft_types_load_error.csv_ FROM (
  SELECT rejected_record
  FROM table(VALIDATE(aircraft_types_2, job_id => $last_copy_query_id)))
  FILE_FORMAT=(format_name='aircraft_types_csv')
  OVERWRITE = true;

SELECT $1,$2,$3,$4,$5,$6,$7,$8 FROM @~/aircraft_types_load_error.csv__0_0_0.csv;

--         Examine the results. Having the result in a file allows you perform
--         data correction on the failed data.

-- 3.9.3   Query the file directly
--         Query the file directly from the stage using a where clause allow you
--         add a where clause or perform transformations.

SELECT $1,$2,$3,$4,$5,$6,$7,$8
FROM
@training_db.traininglab.datasets_stage/sba_data/air_craft/t_aircraft_types_broken.csv
WHERE $2 = 'SIX';

--         Examine the results.

-- 3.9.4   Transform the rejected rows
--         Create a query that transform the bad column value with a valid
--         numeric value.

SELECT $1,'6',$3,$4,$5,$6,$7,$8
FROM
@training_db.traininglab.datasets_stage/sba_data/air_craft/t_aircraft_types_broken.csv
WHERE $2 = 'SIX';


-- 3.9.5   Insert rows with the sub query
--         Insert transformed rows of data from the t_aircraft_types_broken.csv
--         file into the aircraft_types_2 table.

INSERT INTO aircraft_types_2( ac_typeid,
                              ac_group,
                              ssd_name,
                              manufacturer,
                              long_name,
                              short_name,
                              begin_date,
                              end_date )
SELECT
    $1,'6',$3,$4,$5,$6,$7,$8
FROM
@training_db.traininglab.datasets_stage/sba_data/air_craft/t_aircraft_types_broken.csv
WHERE $2 = 'SIX';


-- 3.9.6   Compare row count of the tables
--         Count and compare the rows of the aircraft_types and the
--         aircraft_types_2

SELECT COUNT(*) FROM aircraft_types_2;

SELECT COUNT(*) FROM aircraft_types;

--         Both counts are the same for both tables!

