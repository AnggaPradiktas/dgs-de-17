
-- 9.0.0   Use Dynamic Data Masking
--         In this lab you will learn and practice the following:
--         - Identifying PII Data
--         - Create Masking Policies
--         - Use Data Masking Policies
--         - Test Masking Policies
--         The Snowbear Air membership team needs an analytic application using
--         data that contains Personally Identifiable Information (PII). In this
--         lab, you will create a DEV environment that securely provides the
--         required production data to the development team.

-- 9.1.0   Create the Development Environment

-- 9.1.1   Set your context for the lab.

USE ROLE training_role;
USE DATABASE BADGER_db;
USE WAREHOUSE BADGER_wh;


-- 9.1.2   Create a schema under your BADGER_db database, called MEMBERS_DEV,
--         to create the development environment.

-- ANSWER AVAILABLE IN ANSWER KEY


-- 9.1.3   Clone the MEMBERS table from SNOWBEARAIR_DB.MODELED, into your new
--         schema.
--         This will create a MEMBERS table in the new schema, but the table
--         will be owned by the TRAINING_ROLE (because that’s the role you will
--         use when you create it).

-- ANSWER AVAILABLE IN ANSWER KEY


-- 9.1.4   Describe the new table to see if it contains any PII data.

-- ANSWER AVAILABLE IN ANSWER KEY


-- 9.2.0   Create Masking Policies
--         In the steps below, you will create masking policies for several
--         columns of sensitive data. Remember that by default, only the
--         SYSADMIN and ACCOUNTADMIN roles can create masking policies.
--         As a reminder, here is the basic syntax for creating a masking policy

-- CREATE MASKING POLICY <name> AS
-- (val <type>) RETURNS <type> ->
-- <expression, such as a CASE statement, that defines the masking conditions>;


-- 9.2.1   Set your context, including the appropriate role for creating the
--         policy.

USE ROLE sysadmin;
USE BADGER_db.members_dev;


-- 9.2.2   Create a masking policy for the FIRSTNAME and LASTNAME columns, with
--         these conditions:

-- ANSWER AVAILABLE IN ANSWER KEY


-- 9.2.3   Create a masking policy for the email address, with these conditions:

-- ANSWER AVAILABLE IN ANSWER KEY


-- 9.2.4   Create a masking policy for age, with these conditions:

-- ANSWER AVAILABLE IN ANSWER KEY


-- 9.2.5   Finally, create a masking policy for the phone number, with these
--         conditions:

-- ANSWER AVAILABLE IN ANSWER KEY


-- 9.3.0   Apply and Test Masking Policies

-- 9.3.1   Make sure your context is set correctly.

USE ROLE SYSADMIN;
USE BADGER_db.MEMBERS_DEV;


-- 9.3.2   Set masking policies on the FIRSTNAME, LASTNAME, AGE, EMAIL, and
--         PHONE columns of the MEMBERS table in your MEMBERS_DEV schema.

-- ANSWER AVAILABLE IN ANSWER KEY


-- 9.3.3   As the role TRAINING_ROLE
--         View the masking policy metadata using the SHOW and DESCRIBE
--         commands. The syntax is presented below, since it was not explicitly
--         covered in the slides.

USE ROLE TRAINING_ROLE;

SHOW MASKING POLICIES;

DESCRIBE MASKING POLICY name_mask;
DESCRIBE MASKING POLICY age_mask;
DESCRIBE MASKING POLICY email_mask;
DESCRIBE MASKING POLICY phone_mask;


-- 9.3.4   View the grants on the masking policies.

SHOW GRANTS ON MASKING POLICY name_mask;
SHOW GRANTS ON MASKING POLICY age_mask;
SHOW GRANTS ON MASKING POLICY email_mask;
SHOW GRANTS ON MASKING POLICY phone_mask;


-- 9.3.5   Test the masking policies as TRAINING_ROLE
--         Select the relevant columns from the MEMBERS table and limit the
--         output to the 10 records. As TRAINING_ROLE, you should see the first
--         and last names, the age, the email address, and the last four digits
--         of the phone number.

-- ANSWER AVAILABLE IN ANSWER KEY


-- 9.3.6   Test the masking policies as SYSADMIN
--         In this case, you should see a partially masked value for first name,
--         last name, and email address. The age field should be empty, and the
--         phone field should show a fully masked value.

-- ANSWER AVAILABLE IN ANSWER KEY


-- 9.3.7   Grant object privileges to the PUBLIC role, so you can also test the
--         policy as that role.

GRANT USAGE ON WAREHOUSE BADGER_wh TO ROLE PUBLIC;
GRANT USAGE ON DATABASE BADGER_db TO ROLE PUBLIC;
GRANT USAGE ON SCHEMA members_dev TO ROLE PUBLIC;
GRANT SELECT ON TABLE members TO ROLE PUBLIC;


-- 9.3.8   Test the masking policies as PUBLIC
--         All values should be fully masked or not displayed at all.

-- ANSWER AVAILABLE IN ANSWER KEY

