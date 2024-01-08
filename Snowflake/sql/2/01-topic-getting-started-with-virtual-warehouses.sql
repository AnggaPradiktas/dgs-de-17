
-- 1.0.0   TOPIC: Getting Started with Virtual Warehouses
--         A very important Snowflake concept is the ability to allocate virtual
--         warehouses (of varying sizes and cluster depth) to different tasks or
--         business functions. This is called zero contention workload
--         segmentation. This allows processes like ETL/ELT to run completely
--         separately from end user queries or other tasks, thereby allowing
--         better management and monitoring.

-- 1.1.0   Create Virtual Warehouses

-- 1.1.1   This exercise uses SQL to create a virtual warehouse for different
--         workloads and for data loading.

-- 1.1.2   Select the Worksheets tab.

-- 1.1.3   Ensure your active role is set to TRAINING_ROLE in the upper right
--         corner of the UI. The active role will be the owner when using the UI
--         wizard to create the warehouse.

-- 1.1.4   Click on the (+) directly next to the dropdown arrow to create a new
--         worksheet.

-- 1.1.5   Click inside of the name of the worksheet tab (New Worksheet by
--         default) and name it Compute.
--         We recommend turning on the code highlight feature in the Web UI. You
--         can access this via the ellipsis in the upper right-hand section of
--         the interface (see the following). With this, blocks of SQL code,
--         delineated by a semi-colon, will be highlighted automatically for
--         execution. This means you do not have to manually select statements
--         to run, but only need to place your cursor somewhere inside a block.
--         Turn on Code Highlight

-- 1.1.6   In each lab we will set a query tag to help you easily identify
--         specific lab work within your query history:

ALTER SESSION SET QUERY_TAG='(BADGER) Lab - TOPIC: Getting Started with Virtual Warehouses';


-- 1.1.7   Now, use SQL to create a new warehouse with the following parameters:

use role training_role;
CREATE OR REPLACE WAREHOUSE BADGER_LOAD_WH
WAREHOUSE_SIZE = 'LARGE'
WAREHOUSE_TYPE = 'STANDARD'
AUTO_SUSPEND = 300
AUTO_RESUME = true
MIN_CLUSTER_COUNT = 1
MAX_CLUSTER_COUNT = 2
INITIALLY_SUSPENDED = TRUE
SCALING_POLICY = 'STANDARD'
COMMENT = 'Warehouse used for Data Loading';

--         Note: In this code we are setting the INITIALLY_SUSPENDED option to
--         TRUE. When you create a warehouse with the UI, the warehouse
--         automatically resumes. However, with this option set to true the
--         warehouse will not start immediately after creation.

-- 1.1.8   Enter the following SQL command:

SHOW WAREHOUSES;


-- 1.2.0   Alter a Virtual Warehouse Using SQL

-- 1.2.1   Use SQL to issue an alter on your warehouse to change the following
--         parameters:
--         Size: MEDIUM (4 credits/hour)
--         Maximum Clusters: 1
--         AUTO_SUSPEND = 60 seconds

ALTER WAREHOUSE BADGER_LOAD_WH SET
WAREHOUSE_SIZE = 'MEDIUM'
MAX_CLUSTER_COUNT = 1
AUTO_SUSPEND = 60;

--         Note that the AUTO_SUSPEND value is in seconds.

-- 1.2.2   Select the Warehouses tab.

-- 1.2.3   Locate your warehouse and confirm the parameters are as follows:
--         Status: Suspended
--         Name: BADGER_LOAD_WH
--         Size: Medium
--         Clusters: min: 1, max: 1
--         Auto Suspend: 1 minutes
--         Auto Resume: Yes
--         Owner: TRAINING_ROLE
--         Comment: Warehouse used for Data Loading

-- 1.3.0   Using the Auto_Resume Feature

-- 1.3.1   Select the Warehouses tab.

-- 1.3.2   Locate your warehouse and confirm that the status is now Suspended.

-- 1.3.3   Execute the following queries:

ALTER SESSION SET USE_CACHED_RESULT = FALSE;
USE WAREHOUSE BADGER_LOAD_WH;
SELECT *
FROM TRAINING_DB.TRAININGLAB.REGION;


-- 1.3.4   Select the Warehouses tab once again.

-- 1.3.5   Locate your warehouse and confirm that the status is now Started.
--         Because the warehouse is configured to auto_resume, Snowflake will
--         resume it automatically when a query that requires compute uses this
--         warehouse.

-- 1.4.0   Suspend a Virtual Warehouse Using SQL

-- 1.4.1   Select the Worksheets tab.

-- 1.4.2   Use SQL to issue a command to suspend your warehouse.

ALTER WAREHOUSE BADGER_LOAD_WH SUSPEND;

--         If you get the message, Invalid State. Warehouse BADGER_LOAD_WH
--         cannot be suspended, that means that the warehouse is already
--         suspended. You can ignore this message whenever it appears.

-- 1.4.3   Select the Warehouses tab.

-- 1.4.4   Locate your warehouse and confirm that the status is now Suspended.

-- 1.5.0   Sizing a Virtual Warehouse Dynamically

-- 1.5.1   Size up (scale up) a virtual warehouse using the Snowflake Web UI
--         Note: Warehouses are sized in t-shirt sizing. Each larger size is
--         equivalent to the preceding size x 2 in Snowflake credits consumed.

-- 1.5.2   Navigate to the Warehouses tab

-- 1.5.3   Highlight your warehouse, BADGER_LOAD_WH

-- 1.5.4   Click on Configure

-- 1.5.5   Click on Size to drop down the list of sizes

-- 1.5.6   Select Large and click on Finish
--         Configure Warehouse

-- 1.6.0   Effects of resizing
--         Suspended warehouses: Will not have immediate impact as they will
--         start at the new size when the warehouse is resumed.
--         Running warehouse: Will have an immediate impact as running queries
--         complete at the original size while queued, and future queries run at
--         the new size.

-- 1.6.1   Select the worksheet tab named Compute.

-- 1.6.2   Use SQL to issue a command to resize one of your warehouses

ALTER WAREHOUSE BADGER_LOAD_WH SET WAREHOUSE_SIZE = 'MEDIUM';


-- 1.6.3   Select the Warehouses tab.

-- 1.6.4   Locate your warehouse and confirm that the size is now MEDIUM.

-- 1.7.0   Configure Scale Out of Multi-Clustered Virtual Warehouse
--         Scaling out with multi-cluster virtual warehouses is a feature that
--         must be enabled in your account, and requires Enterprise Edition or
--         above. By default, all Snowflake virtual warehouses have a set amount
--         of clusters with servers assigned to them. For a given warehouse, a
--         user can set both the minimum and maximum number of compute clusters
--         to allocate to that warehouse.

-- 1.7.1   Select the worksheet tab that you named Compute.

-- 1.7.2   Use the following SQL to create a new virtual warehouse for automatic
--         concurrency scale out.

CREATE OR REPLACE WAREHOUSE BADGER_SCALE_OUT_WH
WAREHOUSE_SIZE = 'SMALL'
AUTO_SUSPEND = 60
AUTO_RESUME = true
MIN_CLUSTER_COUNT = 1
MAX_CLUSTER_COUNT = 3
INITIALLY_SUSPENDED = TRUE
SCALING_POLICY = 'STANDARD'
COMMENT = 'Virtual Warehouse for completing concurrency tests';

--         Note: Because you are setting MAX_CLUSTER_COUNT greater than
--         MIN_CLUSTER_COUNT, you will be configuring the warehouse in Auto-
--         Scale Mode and allowing Snowflake to scale the virtual warehouse as
--         needed to handle fluctuating concurrency loads. If you set the
--         Maximum clusters and Minimum clusters to the same number, you would
--         be running in Maximized Mode.
--         The SCALING_POLICY controls when and how Snowflake will turn on/off
--         additional clusters in the virtual warehouse, up to the specified
--         maximum. Using a STANDARD policy means Snowflake automatically starts
--         additional clusters when it detects queries are being queued. This is
--         designed to maximize query responsiveness by minimizing queuing time
--         for queries.

-- 1.8.0   Top Considerations for Managing Virtual Warehouses
--         Independently size virtual warehouses for workload separation; for
--         example, an ETL workload does not need to impact a dashboard
--         workload.
--         Use AUTO_SUSPEND and AUTO-RESUME options to pay only for running
--         workloads and to optimize compute saving.
--         Independently size up or size down each virtual warehouse according
--         to performance needed or data volume. These scenarios will be
--         exploited in multiple labs throughout the course.
--         Automatically scale out using multi-clustered virtual warehouse for
--         more users and higher concurrency. This scenario will be exploited
--         later in the concurrency lab.
--         Choose the appropriate scaling policy of multi-clustered virtual
--         warehouse to match workload types.

-- 1.9.0   Clone and Get Objects Ready for the Rest of the Class
--         We haven’t covered much of what we’re about to do in this section;
--         these statements will CLONE a database with a bunch of existing
--         objects for your use in the class into your own database BADGER_DB.
--         If the specifics of the statements are unclear, do not worry; over
--         the course of this class we will fill in the details of what’s going
--         on. For now, just know you’re creating your own version of a variety
--         of objects from a templated db snowbearair_db. In addition you are
--         creating new warehouses, to be used for specific activities, and
--         setting user default context options.

use role training_role;
CREATE WAREHOUSE IF NOT EXISTS BADGER_wh
 AUTO_SUSPEND = 60
 INITIALLY_SUSPENDED = TRUE;
ALTER WAREHOUSE BADGER_wh SET WAREHOUSE_SIZE = XSMALL;

CREATE WAREHOUSE IF NOT EXISTS BADGER_QUERY_WH
 AUTO_SUSPEND = 180
 INITIALLY_SUSPENDED = TRUE;
ALTER WAREHOUSE BADGER_QUERY_WH SET WAREHOUSE_SIZE = XSMALL;

CREATE WAREHOUSE IF NOT EXISTS BADGER_task_wh
 AUTO_SUSPEND = 60
 INITIALLY_SUSPENDED = TRUE;
ALTER WAREHOUSE BADGER_task_wh SET WAREHOUSE_SIZE = XSMALL;

create or replace database BADGER_db clone snowbearair_db;

alter task BADGER_db.raw.LOAD_WEATHER_FROM_RAW_TO_CONFORMED
  SET WAREHOUSE = BADGER_TASK_WH;

ALTER USER BADGER    
SET default_warehouse=BADGER_wh
    default_namespace=BADGER_db.public
    default_role=training_role;  

--         After you have executed this CLONE operation you should have a
--         database, BADGER_DB with a variety of objects you can explore via
--         commands or the UI.

use database BADGER_DB;
show schemas;
use schema raw;
show tables;


-- 1.9.1   Finally, let’s remove the query tag as the work for this lab is
--         complete.

ALTER SESSION UNSET QUERY_TAG;


