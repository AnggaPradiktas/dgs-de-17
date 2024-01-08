
-- 1.0.0   Course Lab Setup and Snowsight Review
--         At the end of this lab, you will be able to:
--         - Navigate through the Snowsight UI
--         - Create and rename worksheets, and set your context
--         - Use SQL to create objects in Snowflake
--         - Create a Snowsight Dashboard
--         To complete the labs in this course, you have access to .sql files
--         (that contain some of the SQL you will need to complete the labs), as
--         well as a workbook in PDF format. The primary difference between the
--         workbook and the .sql files is that the workbook contains
--         illustrations. So if you are stuck on a step - check the workbook to
--         see if it provides more clarification.
--         Before you get started, here are a few things to keep in mind:
--         - Do not copy and paste from the PDF - this can result in an error
--         even if there is nothing visibly wrong with the command you pasted.
--         However, in Google Chrome you can right-click with your mouse and use
--         Paste and Match Style to have it paste properly.
--         - We provide all of the SQL for this lab and will show you in a
--         moment how to create a worksheet from an SQL file. For subsequent
--         labs, try doing the exercises on your own. Most of the SQL will be in
--         the PDF workbook that has workbook-with-answers in the name, so look
--         there for help if you get stuck. Also, you may see – ANSWER AVAILABLE
--         IN ANSWER KEY. That means the SQL code is not provided, but can be
--         found in same answers workbook.

-- 1.1.0   Explore Snowsight

-- 1.1.1   Sign in to Snowsight.
--         Open the Snowflake account url in a browser and enter the user
--         credentials provided by the instructor to sign in to Snowsight. Once
--         logged in you will be in the Worksheets area. Along the left-hand
--         side you will see the main menu.

-- 1.1.2   Update your user profile.
--         Find your user name in the upper left-hand corner. Click there and
--         select the Profile option.
--         In your profile, enter your name and email. You’re also welcome to
--         upload a profile picture if you would like to! Save your profile when
--         you are finished.
--         Check your inbox to verify your email address.

-- 1.1.3   Explore the main menu.
--         The main menu for Snowsight appears down the left-hand side of the
--         interface. Explore each of the sections to see what they do. See if
--         you can find answers to the questions below.
--         The workbook does not provide the answers to the questions below,
--         because they may change over time. You should be able to tell when
--         you have found the part of the UI where you could get the answer.
--         - What is the size of the table
--         SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CUSTOMER_DEMOGRAPHICS?
--         - What are the names of the user-defined functions in the
--         SNOWBEARAIR_DB.MODELED schema?
--         - What data sets have been directly shared with this Snowflake
--         account?
--         - Are there any Resource Monitors on this account?
--         - Where can you see a diagram of how the roles for this account are
--         connected?

-- 1.2.0   Explore Worksheets
--         First, you’ll create a worksheet from the SQL file provided with your
--         course materials. Then you will explore features of worksheets.

-- 1.2.1   Make sure you are on the Home page and have Worksheets selected.

-- 1.2.2   In the upper right-hand corner, next to Search, click the ellipsis
--         (three dots).

-- 1.2.3   In the list that appears, select Create Worksheet from SQL File.

-- 1.2.4   Navigate to the location where you donwloaded the lab files. Locate
--         lab 01 and click Open.
--         This will open a new worksheet, named after the file you opened.

-- 1.2.5   Note the following across the top of the opened worksheet screen:
--         - The Home Icon in the upper-left corner will return you to the main
--         menu.
--         - To the right of the Home Icon is the worksheet name, which you can
--         change by clicking it.
--         - In the top right corner you will see your current role and
--         warehouse, and can change it from there.
--         - The Share button can be used to share a worksheet.
--         - The Run button on the far right will run the selected SQL in the
--         SQL pane.
--         It’s easier to use the keyboard shortcut to run commands in the
--         worksheet, than it is to mouse back and forth between the SQL pane
--         and the Run button. To use the keyboard shortcut, place your cursor
--         on the line you want to run and click either CTRL+Return (for
--         Windows) or CMD+Return (for macOS).

-- 1.2.6   Familiarize yourself with the main sections of the worksheet:
--         - The object browser runs down the left side, and displays objects
--         that your currently active role(s) can access.
--         - The main part of the screen is the SQL pane, where you enter
--         queries you want to run.
--         - Just above line 1 of the SQL pane is where you can change your
--         default database and schema.

-- 1.2.7   Run the following queries:

USE SCHEMA snowbearair_db.modeled;

SELECT MIN(age) FROM members;

--         Question: You have not yet chosen a virtual warehouse, yet these
--         queries ran anyway. Why?
--         Answer: These are metadata-based queries, so you don’t need a virtual
--         warehouse to run them.

-- 1.2.8   Customize the workshet interface.
--         Click the buttons above the result pane, labeled Objects, Query, and
--         Results.
--         These can be used to hide and display different parts of the
--         interface, so you can maximize space for the area you are most
--         interested in. When a section of the UI is hidden, the dark blue
--         background will disappear.
--         When you are done, make sure that all three sections (Objects, Query,
--         and Results) of the UI are displayed.

-- 1.3.0   Create Lab Objects
--         In this lab, you will create a few objects you will be using
--         throughout the labs. We provide all of the SQL for you in this first
--         exercise, but later exercises will move some of the answers to the
--         end of the file, so you can try coming up with the SQL on your own.

-- 1.3.1   Create a virtual warehouse.
--         You will create a single virtual warehouse, and change the size of
--         the warehouse in different exercises, depending on the amount of
--         compute power needed. In the lab environment, your default role
--         (TRAINING_ROLE) has privileges to create and modify warehouses. In
--         your production account, you will probably not have this ability.
--         Instead, it is likely that you will have privileges to use one or
--         more warehouses of different sizes.

USE ROLE training_role;

CREATE WAREHOUSE BADGER_wh WITH WAREHOUSE_SIZE=xsmall AUTO_SUSPEND=300 INITIALLY_SUSPENDED=TRUE;



-- 1.3.2   Set the virtual warehouse in your context.

USE WAREHOUSE BADGER_wh;


-- 1.3.3   Create a database and schema.

CREATE DATABASE BADGER_db;
CREATE SCHEMA BADGER_db.my_schema;


-- 1.3.4   Set your user defaults.

ALTER USER BADGER    
SET DEFAULT_WAREHOUSE=BADGER_wh
    DEFAULT_NAMESPACE=BADGER_db.public
    DEFAULT_ROLE=training_role;  


-- 1.3.5   Refresh the object browser.
--         You should now see your database and schema listed in the object
--         browser (as well as others that were created by other students in the
--         class).
--         On the far left object pane there are two tabs at the top: Worksheets
--         and Databases. Make sure you select Databases.

-- 1.3.6   Create a table using cloning.
--         Set your context, then use cloning to create a table in your new
--         schema.

USE DATABASE BADGER_db;
USE SCHEMA my_schema;
USE WAREHOUSE BADGER_wh;

CREATE TABLE members_clone CLONE snowbearair_db.modeled.members;

SELECT COUNT(*) FROM members_clone;


-- 1.3.7   Modify the data in the table you created.
--         Delete some rows from the members table you created, and see how many
--         remain:

DELETE FROM members_clone
WHERE age < 30;

SELECT COUNT(*) FROM members_clone;


-- 1.3.8   Query historical data
--         Use time travel to count the rows that existed in the table before
--         you deleted the items. Note that the command below queries the table
--         as it was 60 seconds ago; you may have to adjust the offset a bit to
--         account for how long ago the table was created, and when you modified
--         it.

SELECT COUNT(*) FROM members_clone
AT(OFFSET=>-60);


-- 1.3.9   Run the following query:

SELECT COUNT(*), cd_gender AS gender, cd_marital_status AS marital_status
FROM snowflake_sample_data.tpcds_sf10TCL.customer_demographics
GROUP BY gender, marital_status
ORDER BY gender, marital_status;

--         Question: Do you notice anything odd about the results?
--         Answer: All combinations of gender and marital status have exactly
--         the same count. In an actual data set, this might raise some
--         suspicions about the accuracy of the data, or whether the query was
--         written correctly. All of the data in snowflake_sample_data is
--         manufactured benchmarking data, so these results are actually
--         correct.

-- 1.3.10  View results in the query profile.
--         Look at the query profile for the query you just ran.
--         To the right of the result pane, find the box labeled Query Details.
--         Click the ellipsis (three dots) to the right of the title, and select
--         View Query Profile. This is how you can access the profile for any
--         query.
--         Question: Was the query executed, or did the result come from the
--         query result cache? Why?
--         Answer: If you remember from the Fundamentals course, the query
--         result cache will cache the results of every query. If the exact same
--         query is run again (and the data has not changed), Snowflake will
--         return the result from the cache rather than running the query again.
--         If your query profile showed that the query executed - then you were
--         the first student to run this step in the lab. If your query profile
--         shows QUERY RESULT RE-USE, then you used the result cache because
--         some else had previously run the exact same query.

-- 1.3.11  Navigate back to your worksheet.
--         The Query profile opens in another browser tab. You can just close
--         the tab which will take you back to the Worksheet.

-- 1.4.0   Create a Dashboard

-- 1.4.1   Create some transient tables.
--         Clone some tables from the schema SNOWBEARAIR_DB.PROMO_CATALOG_SALES
--         as transient tables in your own database:

CREATE SCHEMA BADGER_db.chart_test;
USE BADGER_db.chart_test;

CREATE TRANSIENT  TABLE BADGER_db.chart_test.nation
CLONE snowbearair_db.promo_catalog_sales.nation;

CREATE TRANSIENT TABLE BADGER_db.chart_test.customer
CLONE snowbearair_db.promo_catalog_sales.customer;

CREATE TRANSIENT TABLE BADGER_db.chart_test.orders
CLONE snowbearair_db.promo_catalog_sales.orders;


-- 1.4.2   Query the tables to find total sales for each country.
--         Run the following SQL:

SELECT n_name nation
     , SUM(o_totalprice) total_sales
FROM customer
JOIN nation ON c_nationkey=n_nationkey
JOIN orders ON c_custkey=o_custkey
WHERE n_nationkey<10
GROUP BY n_name
ORDER BY SUM(o_totalprice) DESC;


-- 1.4.3   Adjust the TOTAL_SALES column to show only whole dollar amounts.
--         Click in the heading of the TOTAL_SALES column, then click the three
--         dots on the far left of the heading. Using the middle two icons that
--         appear, adjust the precision of the column values until only the full
--         dollar amounts are displayed.

-- 1.4.4   Change the chart to a bar chart.
--         Click the chart button above the results pane. Change the Chart type
--         to a bar chart.

-- 1.4.5   Add the chart to your dashboard.
--         Click on the worksheet name in the upper left corner of the screen.
--         From the drop-down menu that appears, select Move to > + New
--         dashboard. Name the dashboard whatever you want, and then click
--         Create Dashboard.

-- 1.4.6   Display your chart in the dashboard.
--         Click Return to  in the upper left-hand corner. Your dashboard will
--         appear, with your new chart in it.

-- 1.4.7   Return to the Worksheets area.
--         Click the home icon in the upper left-hand corner, then click
--         Worksheets from the left-side main menu.

-- 1.4.8   Open a new worksheet.
--         Click the +Worksheet button in upper right to open a new worksheet.

-- 1.4.9   Set your context, then delete some rows from the orders table.

USE BADGER_db.chart_test;
USE WAREHOUSE BADGER_wh;

DELETE FROM orders
WHERE o_totalprice < 1000;


-- 1.4.10  Return to the dashboards area and view your dashboard.
--         Click the home icon in the upper-left corner, then go to the
--         Dashboards menu item. Select your dashboard from the list.

-- 1.4.11  Refresh the dashboard to see the changes.
--         Check the totals under the bars in the dashboard before refreshing.
--         Then click the blue run button (in the upper right-hand corner) to
--         refresh the data.
