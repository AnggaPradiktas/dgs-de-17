
-- 14.0.0  Database Replication and Disaster Recovery (OPTIONAL)
--         In this lab you will learn and practice the following:
--         - Setup Primary Database to Another Account
--         - Enable Replication and Failover of Primary Database
--         - Create Replica of Primary Database
--         - Monitor Replication Manually
--         - Schedule Replication Automatically (OPTIONAL)
--         - Changing Replication Direction
--         In this exercise you will learn how to set up two Snowflake accounts
--         for replication, create a primary database, and perform the initial
--         replication of this primary database to a secondary database on
--         another account.
--         Database Replication Diagram
--         You will also perform the steps necessary to fail over to the
--         secondary account for disaster recovery.
--         Database Failover Diagram
--         Before you can configure database replication, two or more accounts
--         must be linked to an organization. The instructor will provide the
--         Primary and Secondary account URLs for this exercise.

-- 14.1.0  Set up Browsers for Database Replication

-- 14.1.1  Open two browser windows side-by-side
--         You could also open two tabs, but the exercise is easier to complete
--         if you can see both browser windows at the same time.

-- 14.1.2  In browser 1, enter the Primary account URL (provided by your
--         instructor) and log in with your assigned credentials

-- 14.1.3  Navigate to worksheets and load the script for this lab

-- 14.1.4  Rename the worksheet to PRIMARY
--         In the PRIMARY worksheet, you will only execute statements that are
--         surrounded by the PRIMARY comments, as shown below.

-- PRIMARY --
-- SQL statement 1;
-- SQL statement 2;
-- PRIMARY --


-- 14.1.5  Determine the region and account locator name for your primary
--         account.

-- PRIMARY --
SELECT CURRENT_REGION() AS "primary.region",
       CURRENT_ACCOUNT() AS "primary.account_locator";
-- PRIMARY --


-- 14.1.6  In browser 2, enter the Secondary account URL and log in with your
--         assigned credentials

-- 14.1.7  Navigate to worksheets and load the script for this lab

-- 14.1.8  Rename the worksheet to SECONDARY
--         In the SECONDARY worksheet, you will only execute statements that are
--         surrounded by the SECONDARY comments, as shown below.

-- SECONDARY --
-- SQL command 1;
-- SQL command 2;
-- SECONDARY --


-- 14.1.9  Determine the region and account locator names for your secondary
--         account

-- SECONDARY --
SELECT CURRENT_REGION() AS "secondary.region",
    CURRENT_ACCOUNT() AS "secondary.account_locator";
-- SECONDARY --


-- 14.1.10 Examine the query results
--         The results in Browser 1 show the primary.region and
--         primary.account_locator.
--         The results in Browser 2 show the secondary.region and
--         secondary.account_locator.
--         For this lab, the primary and secondary accounts are in the same
--         region. However, you can have the secondary account on a different
--         cloud provider, or in a different region, from the primary account.

-- 14.2.0  Set Account Locator and Region Names
--         The SQL commands in this lab use an account identifier in the format,
--         snowflake_region.account_locator. For information about account
--         identifiers, see Account Identifiers.
--         This lab contains placeholders for the primary and secondary account
--         locators and regions, because those values will be different for
--         every class. You need to replace those placeholders with the correct
--         names.
--         - Replace [PRIMARY-REGION] with the region for the primary account.
--         - Replace [PRIMARY-ACCOUNT-LOCATOR] with the account locator for the
--         primary account.
--         - Replace [SECONDARY-REGION] with the region for the secondary
--         account.
--         - Replace [SECONDARY-ACCOUNT-LOCATOR] with the account name for the
--         secondary account.

-- 14.3.0  Set Up the Primary Database

-- 14.3.1  On the PRIMARY account, create a database and objects to replicate

-- PRIMARY --
USE ROLE training_role;

CREATE WAREHOUSE IF NOT EXISTS BADGER_repl_wh  
   WITH WAREHOUSE_SIZE = 'XSMALL'
   AUTO_SUSPEND = 300;
CREATE DATABASE IF NOT EXISTS BADGER_repl_db;
CREATE SCHEMA IF NOT EXISTS repl_schema;
USE BADGER_repl_db.repl_schema;

-- create a table with 1000 rows
CREATE OR REPLACE TABLE marketing_a
    ( cust_number INT, cust_name CHAR(50), cust_address VARCHAR(100),
      cust_purchase_date DATE ) CLUSTER BY (cust_purchase_date)
AS (  SELECT UNIFORM(1,999,RANDOM(10002)),
             UUID_STRING(),
             UUID_STRING(),
             CURRENT_DATE
      FROM TABLE(GENERATOR(ROWCOUNT => 1000))
);

-- create a procedure to insert 100 rows into the table
CREATE OR REPLACE PROCEDURE insert_marketing_rows()
RETURNS VARCHAR NOT NULL
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
var result = "";
try {
    var sql_command =
        "INSERT INTO marketing_a SELECT UNIFORM(1,999,RANDOM(10002)), UUID_STRING(), UUID_STRING(), CURRENT_DATE FROM TABLE(GENERATOR(ROWCOUNT => 100))"
    stmt = snowflake.createStatement(
        {sqlText: sql_command});
    rs = stmt.execute();
    }
catch (err) {
    result =  "Failed: Code: " + err.code + "\n  State: " + err.state;
    result += "\n  Message: " + err.message;
    result += "\nStack Trace:\n" + err.stackTraceTxt;
    }
return result;
$$
;
-- PRIMARY --


-- 14.3.2  View the primary account identifiers

-- PRIMARY --
USE ROLE accountadmin;

SHOW  REPLICATION ACCOUNTS LIKE '[PRIMARY-ACCOUNT-LOCATOR]';
-- PRIMARY --

--         Examine the results. Note the ACCOUNT_LOCATOR values and confirm that
--         they match the Primary account URL provided by your instructor.
--         Show Replication Account Results
--         Note each value of the SNOWFLAKE_REGION, ACCOUNT_LOCATOR,
--         ORGANIZATION_NAME and ACCOUNT_NAME columns to determine the Primary
--         account identifiers in the form org_name.account_name or
--         snowflake_region.account_locator.

-- 14.3.3  View the secondary account identifiers

-- SECONDARY --
USE ROLE accountadmin;

SHOW  REPLICATION ACCOUNTS LIKE '[SECONDARY-ACCOUNT-LOCATOR]';
-- SECONDARY --

--         Examine the results. Note the ACCOUNT_LOCATOR values and confirm that
--         they match the Secondary account URL provided by your instructor.
--         Note each value of the SNOWFLAKE_REGION, ACCOUNT_LOCATOR,
--         ORGANIZATION_NAME and ACCOUNT_NAME columns to determine the Secondary
--         account identifiers in the form org_name.account_name or
--         snowflake_region.account_locator.

-- 14.3.4  Promote BADGER_REPL_DB to serve as a primary database

-- PRIMARY --
ALTER DATABASE BADGER_repl_db enable replication
    TO ACCOUNTS [SECONDARY-REGION].[SECONDARY-ACCOUNT-LOCATOR];
-- PRIMARY --


-- 14.3.5  Examine the results of the SHOW REPLICATION DATABASES statement

-- PRIMARY --
SHOW REPLICATION DATABASES;
-- PRIMARY --

--         Verify that IS_PRIMARY is TRUE and that the
--         REPLICATION_ALLOWED_TO_ACCOUNTS column contains both the primary and
--         secondary account identifiers in one of the the following two
--         formats: org_name.account_name or snowflake_region.account_locator.

-- 14.3.6  Enable the ability to fail over from the primary to the secondary
--         account

-- PRIMARY --
ALTER DATABASE BADGER_repl_db ENABLE FAILOVER
    TO ACCOUNTS [SECONDARY-REGION].[SECONDARY-ACCOUNT-LOCATOR];
-- PRIMARY --


-- 14.3.7  Examine the results of the SHOW REPLICATION DATABASES statement

-- PRIMARY --
SHOW REPLICATION DATABASES;
-- PRIMARY --

--         Verify that the FAILOVER_ALLOWED_TO_ACCOUNTS column contains both the
--         primary and secondary account identifiers in one of the following two
--         formats: org_name.account_name or snowflake_region.account_locator.

-- 14.4.0  Creation and Replication To the Secondary Database
--         In this exercise, you will perform the steps to create the secondary
--         database, and replicate the database from the primary account to the
--         secondary account.
--         Initial Database Replica

-- 14.4.1  Create a replica database on the secondary account
--         This step creates an empty database with the same structure as the
--         database on the primary account. No data will be transferred during
--         this step.

-- SECONDARY --
CREATE DATABASE BADGER_REPL_DB AS REPLICA
    OF [PRIMARY-REGION].[PRIMARY-ACCOUNT-LOCATOR].BADGER_repl_db;

SHOW REPLICATION DATABASES;
-- SECONDARY --

--         Examine the results. Check to see that is_primary is FALSE for the
--         secondary account.

-- 14.4.2  Transfer ownership of the replica objects to TRAINING_ROLE

-- SECONDARY --
GRANT OWNERSHIP ON DATABASE BADGER_repl_db TO ROLE training_role;
GRANT OWNERSHIP ON SCHEMA BADGER_repl_db.public TO ROLE training_role;
-- SECONDARY --


-- 14.4.3  Start the initial replication
--         Replication is a pull operation, so the process is initiated on the
--         secondary account.

-- SECONDARY --
USE ROLE training_role;
ALTER DATABASE BADGER_repl_db REFRESH;
-- SECONDARY --


-- 14.4.4  Query the secondary database to verify the replication has completed

-- SECONDARY --
-- verify data and objects
CREATE WAREHOUSE IF NOT EXISTS BADGER_repl_wh
    WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 300;
USE DATABASE BADGER_repl_db;
USE SCHEMA repl_schema;

SELECT COUNT(cust_number) FROM marketing_a;

SHOW TABLES;
SHOW PROCEDURES;
-- SECONDARY --

--         The COUNT(cust_number) value should be 1000.

-- 14.4.5  Verify that the replica is read-only

-- SECONDARY --
USE WAREHOUSE BADGER_repl_wh;
CALL insert_marketing_rows();
-- SECONDARY --

--         When the primary database is replicated, a snapshot of its database
--         objects and data is transferred to the secondary database.

-- 14.5.0  Monitor Replication
--         In this exercise, you will perform the steps to determine the current
--         status of the initial database replication or a subsequent secondary
--         database refresh.

-- 14.5.1  Set your context

-- SECONDARY --
USE ROLE training_role;
USE WAREHOUSE BADGER_repl_wh;
USE DATABASE BADGER_repl_db;
USE SCHEMA repl_schema;
-- SECONDARY --


-- 14.5.2  Monitor the database refresh progress

-- SECONDARY --
-- show the steps for the latest refresh on the database, in seconds
SELECT
    phase_name,
    result,
    start_time,
    end_time,
    DATEDIFF(second, start_time, end_time) AS duration,
    details
FROM TABLE(information_schema.database_refresh_progress('BADGER_repl_db'));

-- show the steps for the latest refresh on the database, in minutes
SELECT value:phaseName::string as Phase,
    value:resultName::string as Result,
    to_timestamp_ltz(value:startTimeUTC::numeric,3) as startTime,
    to_timestamp_ltz(value:endTimeUTC::numeric,3) as endTime,
    datediff(mins, startTime, endTime) as Minutes
FROM TABLE (flatten(input=>parse_json(
    SYSTEM$database_refresh_progress('BADGER_repl_db'))));
-- SECONDARY --


-- 14.5.3  Monitor the database refresh history

-- SECONDARY --
SELECT *
FROM TABLE(information_schema.database_refresh_history('BADGER_repl_db'));
-- SECONDARY --

--         You can also monitor database refresh progress by job id, by
--         providing the value of the JOB_UUID column from the
--         database_refresh_history to investigate to a specific refresh in the
--         last 14 days.

-- SECONDARY --
SELECT
    phase_name,
    result,
    start_time,
    end_time,
    DATEDIFF(SECOND, START_TIME, END_TIME) AS duration
FROM TABLE(information_schema.database_refresh_progress_by_job
    ('<JOB_UUID_VALUE_FROM_REFRESH_HISTORY_QUERY>'));
-- SECONDARY --


-- 14.6.0  Schedule Automatic Refreshes of the Replica
--         In the previous exercise, you learned how to manually perform the
--         initial refresh and validated the replication between the primary and
--         secondary databases. As a best practice, Snowflake recommends
--         scheduling your secondary database refreshes. In this exercise you
--         will perform the steps for starting a database refresh automatically
--         on a specified schedule.

-- 14.6.1  Create a database on the secondary account where the task will be
--         created

-- SECONDARY --
USE ROLE training_role;

CREATE DATABASE IF NOT EXISTS BADGER_db;
CREATE SCHEMA IF NOT EXISTS repl_tasks;
USE DATABASE BADGER_db;
USE SCHEMA repl_tasks;

-- create a task to refresh on a regular basis
CREATE OR REPLACE TASK BADGER_repl_db_refresh_task  
    WAREHOUSE = BADGER_repl_wh
    SCHEDULE = '1 MINUTE'
AS ALTER DATABASE BADGER_repl_db REFRESH;
-- SECONDARY --


-- 14.6.2  Start the task
--         After creating a task, you must RESUME the task before it will run.

-- SECONDARY --
SHOW TASKS;

ALTER TASK BADGER_repl_db_refresh_task RESUME;

SHOW TASKS;
-- SECONDARY --


-- 14.6.3  Monitor the task history and the database refresh history

-- SECONDARY --
-- monitor task history
USE WAREHOUSE BADGER_repl_wh;
SELECT * FROM TABLE(information_schema.task_history())
  WHERE DATABASE_NAME LIKE UPPER('BADGER_db');

-- monitor database refresh history
-- LOOK AT ALL REFRESH OPERATIONS FOR THIS DB IN LAST 14 DAYS
SELECT *
   FROM TABLE(information_schema.database_refresh_history('BADGER_repl_db'));
-- SECONDARY --


-- 14.6.4  Examine the results after about 5 minutes

-- 14.7.0  Verify that a Refresh Picks Up Changes

-- 14.7.1  Insert new rows into the primary database

-- PRIMARY --
USE ROLE training_role;
USE DATABASE BADGER_repl_db;
USE SCHEMA repl_schema;

CALL insert_marketing_rows();
SELECT COUNT(CUST_NUMBER) FROM marketing_a;
CALL insert_marketing_rows();
SELECT COUNT(CUST_NUMBER) FROM marketing_a;
-- PRIMARY --

--         COUNT(cust_number) now has the value of 1200.

-- 14.7.2  Check that the secondary database was updated
--         The task to refresh the secondary database runs every minute. Wait a
--         minute or so, then use the command below to check the refresh history
--         until you see the new refresh operation start, and then complete.

-- SECONDARY --
-- check the database refresh history
SELECT *
  FROM TABLE(information_schema.database_refresh_history('BADGER_repl_db'));

-- count the rows in the table
USE WAREHOUSE BADGER_repl_wh;
USE DATABASE BADGER_repl_db;
USE SCHEMA repl_schema;

SELECT COUNT(cust_number) FROM marketing_a;
-- SECONDARY --

--         The COUNT(cust_number) will have a value of 1200 after the refresh
--         completes.

-- 14.7.3  Suspend the Task

-- SECONDARY --
USE DATABASE BADGER_db;
USE SCHEMA repl_tasks;

SHOW TASKS;

ALTER TASK BADGER_repl_db_refresh_task SUSPEND;

SHOW TASKS;
-- SECONDARY --


-- 14.8.0  Change Replication Direction
--         In this exercise, you will perform the steps to promote the secondary
--         database to act as the primary. When promoted, the secondary database
--         becomes writeable. At the same time, the previous primary database
--         becomes a read-only replica database.
--         Changing Replication Direction

-- 14.8.1  Promote the secondary database

-- SECONDARY --
-- view replication databases
SHOW REPLICATION DATABASES;

-- fail over to the secondary database
ALTER DATABASE BADGER_repl_db PRIMARY;
-- SECONDARY --


-- 14.8.2  Verify the database on the secondary account is now the primary

-- SECONDARY --
SHOW REPLICATION DATABASES;
-- SECONDARY --

--         Check query results to see that is_primary is TRUE for the secondary
--         account.

-- 14.8.3  Verify that the replica database is now writeable

-- SECONDARY --
-- verify database can be written to
USE ROLE training_role;
USE WAREHOUSE BADGER_repl_wh;
USE DATABASE BADGER_repl_db;
USE SCHEMA repl_schema;

CALL insert_marketing_rows();
SELECT COUNT(cust_number) FROM marketing_a;
CALL insert_marketing_rows();
SELECT COUNT(cust_number) FROM marketing_a;

-- Drop a column from the table
ALTER TABLE marketing_a DROP COLUMN cust_address;

DESC TABLE marketing_a;

-- recreate the stored procedure without the column that you just dropped
CREATE OR REPLACE PROCEDURE insert_marketing_rows()
RETURNS VARCHAR NOT NULL
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
var result = "";
try {
    var sql_command =
        "INSERT INTO marketing_a SELECT UNIFORM(1,999,RANDOM(10002)),UUID_STRING(), CURRENT_DATE FROM TABLE(GENERATOR(ROWCOUNT => 100))"
    stmt = snowflake.createStatement(
        {sqlText: sql_command});
    rs = stmt.execute();
    }
catch (err) {
    result =  "Failed: Code: " + err.code + "\n  State: " + err.state;
    result += "\n  Message: " + err.message;
    result += "\nStack Trace:\n" + err.stackTraceTxt;
    }
return result;
$$
;

CALL insert_marketing_rows();

SELECT COUNT(cust_number) FROM marketing_a;

-- SECONDARY --

--         The COUNT(cust_number) value is now 1500.

-- 14.8.4  Refresh the new secondary database

-- PRIMARY --
USE ROLE training_role;
USE DATABASE BADGER_repl_db;
USE SCHEMA repl_schema;

SELECT COUNT(cust_number) FROM marketing_a;

ALTER DATABASE BADGER_repl_db REFRESH;

-- Wait a minute or so to give the refresh time to complete

DESC TABLE marketing_a;

SELECT COUNT(cust_number) FROM marketing_a;
-- PRIMARY --

--         You should see the record count increase from 1200 before the
--         REFRESH, to 1500 after the REFRESH.

-- 14.9.0  Clean Up

-- 14.9.1  Run the following statements on the primary account

-- PRIMARY --
USE ROLE training_role;
DROP DATABASE BADGER_repl_db;
DROP WAREHOUSE BADGER_repl_wh;
-- PRIMARY --


-- 14.9.2  Run the following statements on the secondary account

-- SECONDARY --
USE ROLE training_role;
DROP DATABASE BADGER_repl_db;
DROP SCHEMA BADGER_db.repl_tasks;
DROP WAREHOUSE BADGER_repl_wh;
-- SECONDARY --


