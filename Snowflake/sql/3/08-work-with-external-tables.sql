
-- 8.0.0   Work with External Tables
--         In this lab you will learn and practice the following:
--         - Unloading Data to Cloud Storage as a Data Lake
--         - Unload Data to Cloud Storage using different Virtual Warehouses
--         - Executing Queries against External Tables
--         - Working with External Tables
--         - Create a Partitioned External Tables
--         This lab provide concepts as well as detailed instructions for using
--         external tables. External tables reference data files located in a
--         cloud storage (Amazon S3, Google Cloud Storage, or Microsoft Azure)
--         data lake. External tables store file-level metadata about the data
--         files such as the file path, a version identifier, and partitioning
--         information. This enables querying data stored in files in a data
--         lake as if it were inside a database.

-- 8.1.0   Unload Data to Cloud Storage as a Data Lake
--         To start, you will unload data from the Citibike table onto an
--         external stage.

-- 8.1.1   Open a new worksheet or Create Worksheet from SQL File and set your
--         context

--Note that we can select the database and schema in one statement
USE ROLE training_role;
CREATE SCHEMA IF NOT EXISTS BADGER_db.citibike;
USE schema BADGER_db.citibike;
USE WAREHOUSE BADGER_wh;


-- 8.1.2   Determine the size of the Citibike database
--         Create a query to get row count and size from Citibike table. Record
--         the results for later.

SELECT table_name,
       row_count,
       bytes,
       bytes / (50*1024*1024) as num_chunks,
       num_chunks/8 as max_nodes
FROM snowflake.account_usage.tables
WHERE table_name like 'TRIPS'
     AND table_schema like 'SCHEMA1'
     AND table_catalog like 'CITIBIKE';

--         The result likely indicates that the maximum number of nodes required
--         is between 4 and 5. A medium cluster is 4 nodes, and a large cluster
--         is 8 nodes. A large cluster should run the dump faster, but the
--         medium will use fewer credits.

-- 8.2.0   Unload Data to Cloud Storage with Different Warehouse Sizes

-- 8.2.1   Set your warehouse

CREATE WAREHOUSE IF NOT EXISTS BADGER_load_wh;
USE WAREHOUSE BADGER_load_wh;


-- 8.2.2   Set the warehouse size to MEDIUM for the first test

ALTER WAREHOUSE BADGER_load_wh SET WAREHOUSE_SIZE=MEDIUM;


-- 8.2.3   Copy data to the external stage

COPY INTO '@TRAINING_DB.TRAININGLAB.CLASS_STAGE/COURSE/ADVANCED/student/BADGER/citibike1'
  FROM (SELECT * FROM citibike.schema1.trips)
  FILE_FORMAT=(FORMAT_NAME=training_db.traininglab.mygzippipeformat)
  MAX_FILE_SIZE=49000000
;

LIST @TRAINING_DB.TRAININGLAB.CLASS_STAGE/COURSE/ADVANCED/student/BADGER/citibike1;


-- 8.2.4   Set the warehouse size to LARGE for the second test

ALTER WAREHOUSE BADGER_load_wh SUSPEND;

ALTER WAREHOUSE BADGER_load_wh SET WAREHOUSE_SIZE=LARGE;


-- 8.2.5   Copy the data to the stage

COPY INTO '@TRAINING_DB.TRAININGLAB.CLASS_STAGE/COURSE/ADVANCED/student/BADGER/citibike2'
  FROM (SELECT * FROM citibike.schema1.trips)
  FILE_FORMAT=(FORMAT_NAME=training_db.traininglab.mygzippipeformat)
  MAX_FILE_SIZE=49000000
;

LIST @TRAINING_DB.TRAININGLAB.CLASS_STAGE/COURSE/ADVANCED/student/BADGER/citibike2;


-- 8.2.6   View the query profiles to see the performance difference
--         Looking at the results of the two approaches we do see that the large
--         cluster took less clock time than the medium cluster. We also see
--         files of varying sizes. Both approaches ended up with approximately
--         64 files.

-- 8.2.7   Clean up the stage

REMOVE @TRAINING_DB.TRAININGLAB.CLASS_STAGE/COURSE/ADVANCED/student/BADGER/citibike1;

REMOVE @TRAINING_DB.TRAININGLAB.CLASS_STAGE/COURSE/ADVANCED/student/BADGER/citibike2;


-- 8.2.8   Unload the CITIBIKE data into Parquet files

COPY INTO
'@TRAINING_DB.TRAININGLAB.CLASS_STAGE/COURSE/ADVANCED/student/BADGER/citibike/trips'
  FROM (SELECT * FROM citibike.schema1.trips)
  FILE_FORMAT=(FORMAT_NAME=training_db.traininglab.myparquetformat)
  MAX_FILE_SIZE=49000000;

LIST @TRAINING_DB.TRAININGLAB.CLASS_STAGE/COURSE/ADVANCED/student/BADGER/citibike;


-- 8.2.9   Create an external table using the unloaded Parquet files

USE DATABASE BADGER_DB;
CREATE OR REPLACE EXTERNAL TABLE ext_parquet_trips
  location= @TRAINING_DB.TRAININGLAB.CLASS_STAGE/COURSE/ADVANCED/student/BADGER/citibike/
  FILE_FORMAT=(TYPE=PARQUET);

ALTER EXTERNAL TABLE ext_parquet_trips REFRESH;

SELECT * FROM ext_parquet_trips LIMIT 10;


-- 8.2.10  Unload the data using a join between the tpch customer and nation
--         tables

COPY INTO
'@TRAINING_DB.TRAININGLAB.CLASS_STAGE/COURSE/ADVANCED/student/BADGER/parquet/tpch/myjoin'
  FROM (SELECT c_name,
       c_nationkey,
       c_address,
       c_acctbal,
       n_nationkey,
       n_name
  FROM snowflake_sample_data.tpch_sf1.customer join snowflake_sample_data.tpch_sf1.nation
    on c_nationkey = n_nationkey)
  FILE_FORMAT=(TYPE='PARQUET');

CREATE OR REPLACE EXTERNAL TABLE ext_parquet_myjoin
  location=
  @TRAINING_DB.TRAININGLAB.CLASS_STAGE/COURSE/ADVANCED/student/BADGER/parquet/tpch/
  FILE_FORMAT=(TYPE=PARQUET);

ALTER EXTERNAL TABLE ext_parquet_myjoin REFRESH;

SELECT * FROM ext_parquet_myjoin limit 10;


-- 8.3.0   Execute Queries Against External Tables and Metadata

-- 8.3.1   Set your context.

USE ROLE training_role;
USE WAREHOUSE BADGER_wh;
CREATE DATABASE BADGER_tpcdi_stg;
USE SCHEMA public;


-- 8.3.2   List the staged files

LIST @training_db.traininglab.ed_stage/finwire;


-- 8.3.3   Create a file format to query the data

CREATE OR REPLACE FILE FORMAT BADGER_tpcdi_stg.public.txt_fixed_width
  TYPE = CSV
  COMPRESSION = 'AUTO'
  FIELD_DELIMITER = NONE
  RECORD_DELIMITER = '\\n'
  SKIP_HEADER = 0
  TRIM_SPACE = FALSE
  ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
  NULL_IF = ('\\N');


-- 8.3.4   Create an external table

CREATE OR REPLACE EXTERNAL TABLE finwire
  LOCATION = @training_db.traininglab.ed_stage/finwire
  REFRESH_ON_CREATE = FALSE
  FILE_FORMAT = (FORMAT_NAME = 'txt_fixed_width');


-- 8.3.5   Explore TABLE and EXTERNAL TABLE metadata

SHOW TABLES;
SHOW EXTERNAL TABLES;


-- 8.3.6   Execute a simple query against the external table

SELECT * FROM finwire LIMIT 10;

--         Note: There are no results because REFRESH_ON_CREATE = FALSE was
--         specified when the table was created.

-- 8.3.7   Manually refresh the external table metadata

ALTER EXTERNAL TABLE finwire REFRESH;

--         Refreshing the external table synchronizes the metadata with the
--         current list of data files in the specified stage. This action is
--         required for the metadata to register any existing data files in the
--         named external stage.
--         Examine the query results, that shows the files loaded to the
--         external table.

-- 8.3.8   Rerun the query

SELECT * FROM finwire LIMIT 10;

--         The result set is a single variant column. Take a look at the query
--         profile.

-- 8.4.0   Work with External Tables

-- 8.4.1   Create a table with columns

CREATE OR REPLACE EXTERNAL TABLE finwire
    (
     pts                VARCHAR(15)  AS SUBSTR($1, 8, 15),
     rec_type           VARCHAR(3)   AS SUBSTR($1, 23, 3),
     company_name       VARCHAR(60)  AS SUBSTR($1, 26, 60),
     cik                VARCHAR(10)  AS SUBSTR($1, 86, 10),
     status             VARCHAR(4)   AS IFF(SUBSTR($1, 23, 3) = 'CMP', SUBSTR($1, 96, 4),SUBSTR($1, 47, 4)),
     industry_id        VARCHAR(2)   AS SUBSTR($1, 100, 2),
     sp_rating          VARCHAR(4)   AS SUBSTR($1, 102, 4),
     founding_date      VARCHAR(8)   AS SUBSTR($1, 106, 8),
     addr_line1         VARCHAR(80)  AS SUBSTR($1, 114, 80),
     addr_line2         VARCHAR(80)  AS SUBSTR($1, 194, 80),
     postal_code        VARCHAR(12)  AS SUBSTR($1, 274, 12),
     city               VARCHAR(25)  AS SUBSTR($1, 286, 25),
     state_province     VARCHAR(20)  AS SUBSTR($1, 311, 20),
     country            VARCHAR(24)  AS SUBSTR($1, 331, 24),
     ceo_name           VARCHAR(46)  AS SUBSTR($1, 355, 46),
     description        VARCHAR(150) AS SUBSTR($1, 401, 150),
     year               VARCHAR(4)   AS SUBSTR($1, 8, 4),
     quarter            VARCHAR(1)   AS SUBSTR($1, 30, 1),
     qtr_start_date     VARCHAR(8)   AS SUBSTR($1, 31, 8),
     posting_date       VARCHAR(8)   AS SUBSTR($1, 39, 8),
     revenue            VARCHAR(17)  AS SUBSTR($1, 47, 17),
     earnings           VARCHAR(17)  AS SUBSTR($1, 64, 17),
     eps                VARCHAR(12)  AS SUBSTR($1, 81, 12),
     diluted_eps        VARCHAR(12)  AS SUBSTR($1, 93, 12),
     margin             VARCHAR(12)  AS SUBSTR($1, 105, 12),
     inventory          VARCHAR(17)  AS SUBSTR($1, 117, 17),
     assets             VARCHAR(17)  AS SUBSTR($1, 134, 17),
     liabilities        VARCHAR(17)  AS SUBSTR($1, 151, 17),
     sh_out             VARCHAR(13)  AS IFF(SUBSTR($1, 23, 3) = 'FIN', SUBSTR($1, 168, 13), SUBSTR($1, 127, 13)),
     diluted_sh_out     VARCHAR(13)  AS SUBSTR($1, 181, 13),
     co_name_or_cik     VARCHAR(60)  AS IFF(SUBSTR($1, 23, 3) = 'FIN', SUBSTR($1, 194, 10), SUBSTR($1, 168, 10)),
     symbol             VARCHAR(15)  AS SUBSTR($1, 26, 15),
     issue_type         VARCHAR(6)   AS SUBSTR($1, 41, 6),
     name               VARCHAR(70)  AS SUBSTR($1, 51, 70),
     ex_id              VARCHAR(6)   AS SUBSTR($1, 121, 6),
     first_trade_date   VARCHAR(8)   AS SUBSTR($1, 140, 8),
     first_trade_exchg  VARCHAR(8)   AS SUBSTR($1, 148, 8),
     dividend           VARCHAR(12)  AS SUBSTR($1, 156, 12)
    )
LOCATION = @training_db.traininglab.ed_stage/finwire
FILE_FORMAT = (format_name = 'txt_fixed_width');


-- 8.4.2   Refresh the external table

ALTER EXTERNAL TABLE finwire REFRESH;


-- 8.4.3   Query the revised table

SELECT *
FROM finwire
WHERE rec_type = 'CMP' limit 10;

SELECT co_name_or_cik,
       year,
       quarter,
       sum(revenue::number)
FROM finwire  
WHERE rec_type='FIN' and year='1967' and quarter='2' group by 1,2,3;

--         Again, examine the query profile for each query and see how efficient
--         the queries are in taking advantage of partition pruning.

-- 8.4.4   Examine the file metadata

SELECT year,
       quarter,
       co_name_or_cik,
       metadata$filename
  FROM finwire
  WHERE rec_type='FIN' LIMIT 10;


-- 8.5.0   Create an External Table with Partitions Based on the File Name

-- 8.5.1   Create the external table

CREATE OR REPLACE EXTERNAL TABLE finwire
(
    year                VARCHAR(4)   AS SUBSTR(METADATA$FILENAME, 16, 4),
    quarter             VARCHAR(1)   AS SUBSTR(METADATA$FILENAME, 21, 1),
    thestring           VARCHAR(90)  AS  SUBSTR(METADATA$FILENAME, 1, 50),
    pts                 VARCHAR(15)  AS SUBSTR($1, 8, 15),
    rec_type            VARCHAR(3)   AS SUBSTR($1, 23, 3),
    company_name        VARCHAR(60)  AS SUBSTR($1, 26, 60),
    cik                 VARCHAR(10)  AS SUBSTR($1, 86, 10),
    status              VARCHAR(4)   AS IFF(SUBSTR($1, 23, 3) = 'CMP', SUBSTR($1, 96, 4),SUBSTR($1, 47, 4)),
    industry_id         VARCHAR(2)   AS SUBSTR($1, 100, 2),
    sp_rating           VARCHAR(4)   AS SUBSTR($1, 102, 4),
    founding_date       VARCHAR(8)   AS SUBSTR($1, 106, 8),
    addr_line1          VARCHAR(80)  AS SUBSTR($1, 114, 80),
    addr_line2          VARCHAR(80)  AS SUBSTR($1, 194, 80),
    postal_code         VARCHAR(12)  AS SUBSTR($1, 274, 12),
    city                VARCHAR(25)  AS SUBSTR($1, 286, 25),
    state_province      VARCHAR(20)  AS SUBSTR($1, 311, 20),
    country             VARCHAR(24)  AS SUBSTR($1, 331, 24),
    ceo_name            VARCHAR(46)  AS SUBSTR($1, 355, 46),
    description         VARCHAR(150) AS SUBSTR($1, 401, 150),
    qtr_start_date      VARCHAR(8)   AS SUBSTR($1, 31, 8),
    posting_date        VARCHAR(8)   AS SUBSTR($1, 39, 8),
    revenue             VARCHAR(17)  AS SUBSTR($1, 47, 17),
    earnings            VARCHAR(17)  AS SUBSTR($1, 64, 17),
    eps                 VARCHAR(12)  AS SUBSTR($1, 81, 12),
    diluted_eps         VARCHAR(12)  AS SUBSTR($1, 93, 12),
    margin              VARCHAR(12)  AS SUBSTR($1, 105, 12),
    inventory           VARCHAR(17)  AS SUBSTR($1, 117, 17),
    assets              VARCHAR(17)  AS SUBSTR($1, 134, 17),
    liabilities         VARCHAR(17)  AS SUBSTR($1, 151, 17),
    sh_out              VARCHAR(13)  AS IFF(SUBSTR($1, 23, 3) = 'FIN', SUBSTR($1, 168, 13), SUBSTR($1, 127, 13)),
    diluted_sh_out      VARCHAR(13)  AS SUBSTR($1, 181, 13),
    co_name_or_cik      VARCHAR(60)  AS IFF(SUBSTR($1, 23, 3) = 'FIN', SUBSTR($1, 194, 10), SUBSTR($1, 168, 10)),
    symbol              VARCHAR(15)  AS SUBSTR($1, 26, 15),
    issue_type          VARCHAR(6)   AS SUBSTR($1, 41, 6),
    name                VARCHAR(70)  AS SUBSTR($1, 51, 70),
    ex_id               VARCHAR(6)   AS SUBSTR($1, 121, 6),
    first_trade_date    VARCHAR(8)   AS SUBSTR($1, 140, 8),
    first_trade_exchg   VARCHAR(8)   AS SUBSTR($1, 148, 8),
    dividend            VARCHAR(12)  AS SUBSTR($1, 156, 12)
)
PARTITION BY (year,quarter)
LOCATION = @training_db.traininglab.ed_stage/finwire
FILE_FORMAT = (format_name = 'txt_fixed_width');


-- 8.5.2   Refresh the table

ALTER EXTERNAL TABLE finwire REFRESH;


-- 8.5.3   Execute some queries and examine their profiles

SELECT co_name_or_cik,
       year,
       quarter,
       sum(revenue::number)
FROM finwire
WHERE rec_type='FIN' and year='1967' and quarter='3' group by 1,2,3;

SELECT co_name_or_cik,
       year,
       quarter,
       sum(revenue::number)
FROM finwire
WHERE rec_type='FIN' and year='1989' and quarter='3' group by 1,2,3;


