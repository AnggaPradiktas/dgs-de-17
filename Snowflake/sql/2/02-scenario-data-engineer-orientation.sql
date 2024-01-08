
-- 2.0.0   SCENARIO: Data Engineer Orientation
--         You are an experienced Data Engineer (DE), recently hired at
--         SnowBearAir, a regional airline in the United States. SnowBearAir
--         uses Snowflake, the cloud data platform, for all of its data
--         processing and analytics. While you have extensive experience in
--         building data pipelines using SQL and databases, this is your first
--         time using Snowflake. In this lab, during your orientation at
--         SnowBearAir, you’ve been asked to get familiar with Snowflake. You’ll
--         explore the UI, and Hello World SQL statements, and explore core
--         Snowflake objects such as warehouses and databases.
--         - Setting the context for an initial Snowflake database.
--         - Understanding the basics of accessing Snowflake’s Web UI.
--         - Creating and modifying a warehouse on the Snowflake infrastructure.
--         - Examine Snowflake’s data organization structure, storage, compute,
--         and metadata in the Cloud Services layer.

-- 2.1.0   Two Environments
--         In this course we’ll use two environments to learn Snowflake:

-- 2.1.1   Log into the class Snowflake account if you haven’t already
--         The instructor should have already completed a brief tour of the
--         Snowflake UI prior to starting this lab.
--         Log in to your Snowflake account using the information provided by
--         the instructor. An example URL will look something like this:
--         https://xy12345.snowflakecomputing.com/console#/internal/worksheet
--         Use the provided username (i.e., an animal name such as BADGER,
--         COBRA, or PENGUIN assigned to you) and the default password to access
--         this account. This animal will be your spirit animal going forward.
--         You should use this ANIMAL name when logging into Snowflake. The lab
--         instructions require you to replace your login (e.g., MONGOOSE) when
--         it refers to BADGER.
--         For instance, if your animal spirit is MONGOOSE you see the following
--         SQL instruction:

-- DO NOT EXECUTE, JUST AN EXAMPLE
--create database
--  BADGER_example_db;

--         you would actually execute the following:

-- DO NOT EXECUTE, JUST AN EXAMPLE
--create database
--  MONGOOSE_example_db; --notice replaced BADGER with MONGOOSE

--         Occasionally and COMPLETELY OPTIONALLY you will also have a chance to
--         test your knowledge and understanding of what’s going on both with a
--         particular command or Snowflake in general. You answer the question
--         by uncommenting the line you wish to submit as your answer. Open a
--         new worksheet in Snowflake for this lab and try running the
--         following:

ALTER SESSION SET QUERY_TAG='(BADGER) Lab - SCENARIO: Data Engineer Orientation';
use warehouse BADGER_WH;
ALTER WAREHOUSE BADGER_WH SET WAREHOUSE_SIZE = 'XSMALL';

select quiz( '9e7a228326a263b3c8bbeec6086cf9da',

'What is the correct answer?'

-- ,'A) - Not this one'
-- ,'B) - DEFINITELY not this one'
,'C) - This is CORRECT, uncomment it to check the answer'
-- ,'D) - External'

) as ANSWER;

--         Correct Quiz Question

-- 2.1.2   Education Notebook Server
--         The instructor should have already completed a brief tour of the
--         Education Notebook server prior to starting this lab.
--         Log in to your notebook server using the information provided by the
--         instructor. An example URL will look something like this:
--         https://labs.snowflakeuniversity.com

-- 2.2.0   Review the UI Components
--         This ribbon contains labeled icons and includes Databases, Shares,
--         Data Marketplace, and so on. These icons are used to activate
--         different areas of the UI. Identify each and learn their function.
--         They are fairly straightforward and user-friendly in their respective
--         purposes.
--         Locate your login and current role in the upper-right portion of the
--         top ribbon. The down arrow located directly to the right of your
--         login and role can be used to view or modify your preferences, change
--         your password, switch roles, or log out.

-- 2.2.1   Make sure the role in the upper right corner is set to the role
--         TRAINING_ROLE.
--         Training Role Context

-- 2.2.2   Click the Warehouses button in the UI ribbon and your screen should
--         display as follows:
--         View Warehouses
--         Explore this view to see the warehouses that you or others have
--         created.

-- 2.3.0   Create a Warehouse Using SQL
--         Clicking buttons is all very good, but much of the power of the
--         platform is in having programmatic SQL access, so we will create a
--         warehouse using SQL commands. But first we will need to drop the
--         existing warehouse, created back in compulsory Lab 1, before running
--         the create statement. Note that we could achieve this with a single
--         CREATE OR REPLACE statement, but here we will consciously execute two
--         independent commands.

-- 2.3.1   Execute the following warehouse create statement in your worksheet.
--         You will need to edit it so that it is named according to your
--         BADGER_query_wh:
--         Note: You should use the provided .sql file. Don’t attempt to copy &
--         paste from this PDF. Invisible characters could cause compiler
--         errors. Load the corresponding .sql file into the worksheet or drag-
--         n-drop the file into your worksheet.

DROP WAREHOUSE IF EXISTS BADGER_QUERY_WH;

CREATE WAREHOUSE BADGER_QUERY_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 180
  AUTO_RESUME = TRUE;       


-- 2.3.2   Confirm the new warehouse was created by clicking on the Warehouses
--         icon in the ribbon.

-- 2.4.0   Working with Context Functions

-- 2.4.1   Example context functions
--         Context functions are informational functions that can be used to
--         obtain information about your working Snowflake environment and
--         session. They provide outputs on client, region, version, and many
--         other informational attributes.
--         See Context Functions in the Snowflake documentation for additional
--         information.

-- 2.4.2   Run the following commands.

SELECT CURRENT_CLIENT();
SELECT CURRENT_REGION();
SELECT CURRENT_USER();
SELECT CURRENT_DATE();


select quiz( '0280236152da5f2037497a595cceb602',

'What is the name of the context function to return the role active? (refer to doc link above)'

-- ,'A) - SESSION_ROLE()'
-- ,'B) - ROLE()'
-- ,'C) - ACTIVE_ROLE()'
-- ,'D) - CURRENT_ROLE()'

) as ANSWER;


-- 2.5.0   Explore Snowflake’s Data Organization Structure

-- 2.5.1   Select the Databases tab.

-- 2.5.2   Locate and select the TRAINING_DB database.

-- 2.5.3   Take note of the different categories available within the database:
--         Tables, Views, Schemas, Stages, File Formats, Sequences, and Pipes.

-- 2.5.4   Select the Schemas category and review the list of schemas in the
--         TRAINING_DB database.

-- 2.5.5   Select the Tables category.

-- 2.5.6   Review the table list and related information: Schema, Creation Time,
--         Owner, Rows, Size, and Comment.

-- 2.5.7   Locate and select the LINEITEM table in the TPCH_SF10 schema. Take
--         note of its structure.

-- 2.5.8   Navigate to the Views category. Note that each view resides within a
--         schema contained in the TRAINING_DB database.
--         : Navigate through the Stages, File Formats, and Sequences
--         categories. Note that you would receive an error if you tried to
--         create new objects (views, tables, stages, etc) in this database. Why
--         might this be? Don’t worry if you do not know. This will be covered
--         later in the course.

-- 2.6.0   Examine Snowflake Storage

-- 2.6.1   Ensure that you have TRAINING_ROLE selected in the upper right of the
--         Web UI:
--         Training Role Context

-- 2.6.2   Select the Account button on the top ribbon.

-- 2.6.3   Navigate to the Billing and Usage section.

-- 2.6.4   Toggle on the Average Storage Used option.

-- 2.6.5   Toggle between months and review the change in total storage
--         throughout the month.

-- 2.6.6   Toggle between the Database, Stage, and Fail Safe category buttons
--         and review the change in storage for each throughout the month.
--         It is possible the account you are using, animal spirits and all, has
--         little storage usage so far, given it was setup up specially for you
--         in this course. Consider checking back in throughout your work as a
--         SnowBearAir Data Engineer during the week to see if you see
--         additional storage usage reported in this section!

-- 2.7.0   Examine Snowflake Compute

-- 2.7.1   In the Warehouses tab, locate the EDMGT_TASK_WH warehouse and click
--         on its name.

-- 2.7.2   Review the Warehouse Load Over Time information.
--         Take note of the hourly fluctuations in usage in the top graph.
--         Take note of the daily fluctuations in usage in the bottom graph.

-- 2.7.3   Change the time frame of the bottom graph to span various time frames
--         and notice the impact this change has on the top graph.

-- 2.7.4   Select the Account tab.
--         As you are likely using a newly created account with only some
--         limited usage to setup for the course, you may not see a ton of usage
--         for now. You should see a couple of warehouses that have utilization,
--         but continue to keep your eye on these screens throughout the course
--         to gather more usage information.
--         Make sure the Billing and Usage section is selected. Review the
--         information about Snowflake credits used for the given time period:
--         Total number of warehouses / total credits used.
--         Credits used by warehouse.
--         Credits used by day.
--         Click on EDMGT_WH to see details of its credits used by day, and
--         click into a day (magnifying glass icon) to see usage by hour

-- 2.7.5   Use DESC objects in a database
--         Snowflake also provides the DESC (or DESCRIBE) command to output
--         detailed information about session/query, account and database
--         objects.
--         Select a worksheet tab and execute the DESC and DESCRIBE commands.

use database TRAINING_DB;

describe table TPCH_SF1.LINEITEM;
desc VIEW information_schema.TABLES;


-- 2.7.6   Finally, let’s remove the query tag as the work for this lab is
--         complete.

ALTER SESSION UNSET QUERY_TAG;

