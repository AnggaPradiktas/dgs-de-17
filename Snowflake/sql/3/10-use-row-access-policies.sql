
-- 10.0.0  Use Row Access Policies
--         In this lab you will learn and practice the following:
--         - Create Objects
--         - Work with Row Access Policies
--         Snowbear Air has 5 sales regions, and a Snowflake role for each
--         region. Each sales region needs to be able to access data that is
--         relevant to their region, but not be able to access information from
--         other regions. In this exercise, you will create roles for each of
--         the 5 regions, and then create a view that shows certain data from
--         the required tables. You will then create and apply a row access
--         policy that limits their access to only the customers in their
--         region.

-- 10.1.0  Create Objects

-- 10.1.1  Open a new worksheet or Create Worksheet from SQL File and set your
--         context

USE ROLE training_role;
USE WAREHOUSE BADGER_wh;
CREATE SCHEMA BADGER_db.rap;


-- 10.1.2  Look at the tables you will be working with:

USE SCHEMA snowbearair_db.promo_catalog_sales;
SELECT * FROM customer LIMIT 10;
SELECT * FROM nation_and_region LIMIT 10;


-- 10.1.3  Create the sales roles you will use to test your row access policy:

USE ROLE useradmin;
CREATE ROLE BADGER_sales_africa;
CREATE ROLE BADGER_sales_us;
CREATE ROLE BADGER_sales_apac;
CREATE ROLE BADGER_sales_eur;
CREATE ROLE BADGER_sales_mideast;


-- 10.1.4  Grant the roles to yourself, and to the role SYSADMIN

GRANT ROLE BADGER_sales_africa,
           BADGER_sales_us,
           BADGER_sales_apac,
           BADGER_sales_eur,
           BADGER_sales_mideast
TO USER BADGER;

GRANT ROLE BADGER_sales_africa,
           BADGER_sales_us,
           BADGER_sales_apac,
           BADGER_sales_eur,
           BADGER_sales_mideast
TO ROLE sysadmin;


-- 10.1.5  Create a secure view
--         Using role training_role create a secure view (called
--         BADGER_db.rap.sales_view) that JOINs the nation_and_region table to
--         the customer table (located in snowbearair_db.promo_catalog_sales
--         schema), and produces output showing the customer key, customer first
--         and last name, region, and nation. Select from the view to test it.
--         Try this one on your own - the answer is provided in the answer
--         section.

-- ANSWER AVAILABLE IN ANSWER KEY


-- 10.1.6  Grant privileges on the new view to the role PUBLIC
--         Snowflake does not generally recommend that you grant privileges to
--         the PUBLIC role. In this case, however, it is easier to grant select
--         on the view to PUBLIC, than to grant select to all five of the new
--         roles. All privileges granted to PUBLIC roll up to all other roles -
--         so this is a quick and easy way to give all the new roles access to
--         your virtual warehouse for this lab. Once the row access policy is in
--         place, anyone using the PUBLIC role would not get any rows returned
--         from the view.

GRANT USAGE ON DATABASE BADGER_db TO ROLE public;
GRANT USAGE ON SCHEMA BADGER_db.rap TO ROLE public;
GRANT SELECT ON BADGER_db.rap.sales_view TO ROLE public;


-- 10.1.7  Create a new virtual warehouse
--         Create a new virtual warehouse called BADGER_sales_wh, and grant
--         usage on the warehouse to the role PUBLIC.

USE ROLE sysadmin;
CREATE WAREHOUSE BADGER_sales_wh;
GRANT USAGE ON WAREHOUSE BADGER_sales_wh TO ROLE public;


-- 10.1.8  Use the role BADGER_sales_africa, and the sales warehouse you
--         created, to select from the view. The row access policy has not yet
--         been applied, so the role should be able to see all the rows in the
--         view, regardless of the region.

-- ANSWER AVAILABLE IN ANSWER KEY


-- 10.2.0  Work with Row Access Policies

-- 10.2.1  Create a row access policy
--         Create a row access policy so that each sales role can see only the
--         entries for their region. We have provided some hints below, but you
--         can turn to the answer if you need help.
--         When creating a role access policy, role names and database names
--         must be in UPPER CASE to be recognized correctly. It’s generally best
--         practice to put ALL object names in upper case in a row access
--         policy.
--         To create a row access policy, you must be an ACCOUNTADMIN. However,
--         the role TRAINING_ROLE has needed privileges for our class so use
--         this role. Look back at the slides for syntax examples. For your row
--         access policy, you will check both the user’s current role, and the
--         region listed in the view.

-- ANSWER AVAILABLE IN ANSWER KEY


-- 10.2.2  Apply the row access policy you just created.

-- ANSWER AVAILABLE IN ANSWER KEY


-- 10.2.3  Test your row access policy
--         Now test your row access policy with each of the sales roles - they
--         should only see customers from their own sales region.

-- ANSWER AVAILABLE IN ANSWER KEY


-- 10.2.4  Bonus step:
--         Create a new view, that joins your sales_view to the orders table,
--         and aggregates the total price spent by each customer.
--         Verify whether or not the row access policy on the sales_view is
--         carried forward into your new view. Call your new view orders_view.
--         You can test this new view with just one of the sales roles, but
--         remember you will need to give that role privileges to select from
--         the new view.

-- ANSWER AVAILABLE IN ANSWER KEY


-- 10.2.5  Clean Up

USE ROLE useradmin;
DROP ROLE BADGER_sales_africa;
DROP ROLE BADGER_sales_us;
DROP ROLE BADGER_sales_eur;
DROP ROLE BADGER_sales_mideast;
DROP ROLE BADGER_Sales_apac;

USE ROLE sysadmin;
DROP WAREHOUSE BADGER_sales_wh;
DROP SCHEMA BADGER_db.rap;

