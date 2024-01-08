
-- 7.0.0   Access Control and User Management
--         Expect this lab to take approximately 25 minutes.
--         Lab Purpose: Students will work with the Snowflake security model and
--         learn how to create roles, grant privileges, build, and implement
--         basic security models.

-- 7.1.0   Determine Privileges (GRANTs)

-- 7.1.1   Navigate to [Worksheets] and create a new worksheet named Managing
--         Security.

-- 7.2.0   14.1.2 If you haven’t created the class database or warehouse, do it
--         now

CREATE WAREHOUSE IF NOT EXISTS BADGER_WH;
CREATE DATABASE IF NOT EXISTS BADGER_DB;


-- 7.2.1   Run these commands to see what has been granted to you as a user, and
--         to your roles:

SHOW GRANTS TO USER BADGER;
SHOW GRANTS TO ROLE TRAINING_ROLE;
SHOW GRANTS TO ROLE SYSADMIN;
SHOW GRANTS TO ROLE SECURITYADMIN;

--         NOTE: The TRAINING_ROLE has some specific privileges granted - not
--         all roles in the system would be able to see these results.

-- 7.3.0   Work with Role Permissions

-- 7.3.1   Change your role to USERADMIN:

USE ROLE USERADMIN;


-- 7.3.2   Create two new custom roles, called BADGER_CLASSIFIED and
--         BADGER_GENERAL:

CREATE OR REPLACE ROLE BADGER_CLASSIFIED;
CREATE OR REPLACE ROLE BADGER_GENERAL;


-- 7.3.3   GRANT both roles to SYSADMIN, and to your user:

GRANT ROLE BADGER_CLASSIFIED, BADGER_GENERAL TO ROLE SYSADMIN;
GRANT ROLE BADGER_CLASSIFIED, BADGER_GENERAL TO USER BADGER;


-- 7.3.4   Change to the role SYSADMIN, so you can assign permissions to the
--         roles you created:

USE ROLE SYSADMIN;


-- 7.3.5   Create a warehouse named BADGER_SHARED_WH:

CREATE OR REPLACE WAREHOUSE BADGER_SHARED_WH;
USE WAREHOUSE BADGER_SHARED_WH;


-- 7.3.6   Grant both new roles privileges to use the shared warehouse:

GRANT USAGE ON WAREHOUSE BADGER_SHARED_WH
  TO ROLE BADGER_CLASSIFIED;
GRANT USAGE ON WAREHOUSE BADGER_SHARED_WH
  TO ROLE BADGER_GENERAL;


-- 7.3.7   Create a database called BADGER_CLASSIFIED_DB:

CREATE OR REPLACE DATABASE BADGER_CLASSIFIED_DB;


-- 7.3.8   Grant the role BADGER_CLASSIFIED all necessary privileges to create
--         tables on any schema in BADGER_CLASSIFIED_DB:

GRANT USAGE ON DATABASE BADGER_CLASSIFIED_DB
TO ROLE BADGER_CLASSIFIED;
GRANT USAGE ON ALL SCHEMAS IN DATABASE BADGER_CLASSIFIED_DB
TO ROLE BADGER_CLASSIFIED;
GRANT CREATE TABLE ON ALL SCHEMAS IN DATABASE BADGER_CLASSIFIED_DB
TO ROLE BADGER_CLASSIFIED;


-- 7.3.9   Use the role BADGER_CLASSIFIED, and create a table called
--         SUPER_SECRET_TBL inside the BADGER_CLASSIFIED_DB.PUBLIC schema:

USE ROLE BADGER_CLASSIFIED;
USE WAREHOUSE BADGER_SHARED_WH;
USE BADGER_CLASSIFIED_DB.PUBLIC;
CREATE OR REPLACE TABLE SUPER_SECRET_TBL (id INT);


-- 7.3.10  Insert some data into the table:

INSERT INTO SUPER_SECRET_TBL VALUES (1), (10), (30);


-- 7.3.11  Check if the BADGER_CLASSIFIED role can access the table

SELECT * FROM BADGER_CLASSIFIED_DB.PUBLIC.SUPER_SECRET_TBL;


-- 7.3.12  Switch to the BADGER_GENERAL role and try accessing the table

USE  ROLE BADGER_GENERAL;
SELECT * FROM BADGER_CLASSIFIED_DB.PUBLIC.SUPER_SECRET_TBL;


-- 7.3.13  Because we haven’t given any permissions to the database, schema, or
--         the table, this doesn’t work.

-- 7.3.14  Before we give BADGER_GENERAL permissions, try using SECONDARY_ROLES
--         to access the table

USE SECONDARY ROLES ALL;
SELECT * FROM BADGER_CLASSIFIED_DB.PUBLIC.SUPER_SECRET_TBL;


-- 7.3.15  Since SECONDARY ROLES ALL allows the user to use any permissions on
--         any role they’ve been granted, This will work.

-- 7.3.16  Now try using the SECONDARY ROLES to create a new table in the
--         database.

USE SCHEMA BADGER_CLASSIFIED_DB.PUBLIC;
CREATE TABLE NOT_SO_SECRET_TBL (id INT);


-- 7.3.17  This will fail since SECONDARY ROLES doesn’t support CREATE

-- 7.3.18  SECONDARY ROLES will support ALTER TABLE.

ALTER TABLE SUPER_SECRET_TBL ADD COLUMN name STRING(20);


-- 7.3.19  To continue this lab, turn off SECONDARY ROLES

USE SECONDARY ROLES NONE;


-- 7.3.20  Switch back to the BADGER_CLASSIFIED

USE ROLE BADGER_CLASSIFIED;


-- 7.3.21  Assign GRANT SELECT privileges on SUPER_SECRET_TBL to the role
--         BADGER_GENERAL:

GRANT SELECT ON SUPER_SECRET_TBL TO ROLE BADGER_GENERAL;


-- 7.3.22  Use the role BADGER_GENERAL to SELECT * from the table
--         SUPER_SECRET_TBL:

USE ROLE BADGER_GENERAL;
SELECT * FROM BADGER_CLASSIFIED_DB.PUBLIC.SUPER_SECRET_TBL;

--         What happens? Why?

-- 7.3.23  Grant role BADGER_GENERAL usage on all schemas in
--         BADGER_CLASSIFIED_DB:

USE ROLE SYSADMIN;
GRANT USAGE ON DATABASE BADGER_CLASSIFIED_DB TO ROLE BADGER_GENERAL;
GRANT USAGE ON ALL SCHEMAs IN DATABASE BADGER_CLASSIFIED_DB TO ROLE BADGER_GENERAL;


-- 7.3.24  Now try again:

USE ROLE BADGER_GENERAL;
SELECT * FROM BADGER_CLASSIFIED_DB.PUBLIC.SUPER_SECRET_TBL;


-- 7.3.25  Drop the database BADGER_CLASSIFIED_DB:

USE ROLE SYSADMIN;
DROP DATABASE BADGER_CLASSIFIED_DB;


-- 7.3.26  Drop the roles BADGER_CLASSIFIED and BADGER_GENERAL:

USE ROLE USERADMIN;
DROP ROLE BADGER_CLASSIFIED;
DROP ROLE BADGER_GENERAL;

--         HINT: What role do you need to use to do this?

-- 7.4.0   Create Parent and Child Roles

-- 7.4.1   Change your role to USERADMIN:

USE ROLE USERADMIN;


-- 7.4.2   Create a parent and child role, and GRANT the roles to the role
--         SYSADMIN. At this point, the roles are peers (neither one is below
--         the other in the hierarchy):

CREATE OR REPLACE ROLE BADGER_CHILD;
CREATE OR REPLACE ROLE BADGER_PARENT;
GRANT ROLE BADGER_CHILD, BADGER_PARENT TO ROLE SYSADMIN;


-- 7.4.3   Give your user name privileges to use the roles:

GRANT ROLE BADGER_CHILD, BADGER_PARENT TO USER BADGER;


-- 7.4.4   Change your role to SYSADMIN:

USE ROLE SYSADMIN;


-- 7.4.5   Grant the following object permissions to the child role:

GRANT USAGE ON WAREHOUSE BADGER_WH TO ROLE BADGER_CHILD;
GRANT USAGE ON DATABASE BADGER_DB TO ROLE BADGER_CHILD;
GRANT USAGE ON SCHEMA BADGER_DB.PUBLIC TO ROLE BADGER_CHILD;
GRANT CREATE TABLE ON SCHEMA BADGER_DB.PUBLIC TO ROLE BADGER_CHILD;


-- 7.4.6   Use the child role to create a table:

USE ROLE BADGER_CHILD;
USE WAREHOUSE BADGER_WH;
USE SCHEMA BADGER_DB.PUBLIC;
CREATE TABLE genealogy (name STRING, age INTEGER, mother STRING, father STRING);


-- 7.4.7   Verify that you can see the table:

SHOW TABLES LIKE '%genealogy%';


-- 7.4.8   Use the parent role and view the table:

USE ROLE BADGER_PARENT;
SHOW TABLES LIKE '%genealogy%';

--         You will not see the table, because the parent role has not been
--         granted access.

-- 7.4.9   Change back to the USERADMIN role and change the hierarchy so the
--         child role is beneath the parent role:

USE ROLE USERADMIN;
GRANT ROLE BADGER_CHILD to ROLE BADGER_PARENT;


-- 7.4.10  Use the parent role, and verify the parent can now see the table
--         created by the child:

USE ROLE BADGER_PARENT;
SHOW TABLES LIKE '%genealogy%';


-- 7.4.11  Clean up by dropping the roles, warehouse and table created in this
--         lab

USE ROLE SYSADMIN;
DROP WAREHOUSE BADGER_SHARED_WH;
DROP TABLE BADGER_DB.public.genealogy;


-- 7.4.12  Drop the roles BADGER_CHILD and BADGER_PARENT:

USE ROLE USERADMIN;
DROP ROLE BADGER_CHILD;
DROP ROLE BADGER_PARENT;


-- 7.4.13  Suspend and resize the warehouse

USE ROLE TRAINING_ROLE;
ALTER WAREHOUSE BADGER_WH SET WAREHOUSE_SIZE=XSmall;
ALTER WAREHOUSE BADGER_WH SUSPEND;

