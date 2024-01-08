
-- 7.0.0   Use Unstructured Data
--         In this lab you will learn and practice the following:
--         - Set up the development environment
--         - Collect unstructured data
--         - Build a view using unstructured data
--         Snowbear Air would like to add pictures of their members to the
--         MEMBERS table - so they are going to test out the process in a
--         development environment. In this lab, you will create that
--         development environment, locate and load pictures of 5 members, and
--         create a view that includes the standard member information and their
--         picture.

-- 7.1.0   Set Up the Development Environment

-- 7.1.1   Open a new Worksheet or Create Worksheet from SQL File and set your
--         context
--         As TRAINING_ROLE, set your context and create a schema called
--         UNSTRUCTURED in your database.

-- ANSWER AVAILABLE IN ANSWER KEY


-- 7.1.2   Create a test version of the SNOWBEARAIR_DB.MODELED.MEMBERS table,
--         with just 5 rows

CREATE TABLE members AS SELECT * FROM snowbearair_db.modeled.members LIMIT 5;


-- 7.1.3   Select from the table and record the last name of the 5 members

SELECT * FROM members;


-- 7.1.4   Change role and context
--         Change to the role SYSADMIN and set your context to use your database
--         and the UNSTRUCTURED schema.

-- ANSWER AVAILABLE IN ANSWER KEY


-- 7.1.5   Create an internal stage that will hold unstructured data
--         Call the stage member_pics and enable a directory table. Since this
--         is an internal stage, be sure to use server-side encryption (TYPE =
--         'SNOWFLAKE_SSE').

-- ANSWER AVAILABLE IN ANSWER KEY


-- 7.1.6   Grant TRAINING_ROLE read and write access to the stage

-- ANSWER AVAILABLE IN ANSWER KEY


-- 7.2.0   Collect Unstructured Data

-- 7.2.1   Find and download 5 pictures
--         Find and download to your desktop 5 pictures (business appropriate!)
--         that will be pictures of the people in your test MEMBERS table. Name
--         the pictures with the syntax: <last name>.jpeg (for example,
--         Adams.jpeg if the last name is Adams). If your pictures have a
--         different extension than .jpeg, substitute that - but make sure that
--         all of your pictures have the same extension.
--         Next, you will upload the pictures to your member_pics stage.
--         The remaining steps in this section walk you through using
--         Snowflake’s Jupyter notebook to connect via SnowSQL and PUT your
--         pictures to the member_pics stage. If you already have SnowSQL or
--         another command-line client installed on your system, you can use
--         those tools instead.

-- 7.2.2   Open a web browser and navigate to
--         https://labs.snowflakeuniversity.com

-- 7.2.3   Login to Jupyterhub
--         Log in using the lab account name, and the same credentials you are
--         currently using to log in to Snowsight. Wait a few minutes for the
--         JupyterLab environment to come up.

-- 7.2.4   Once the environment is up, upload the pictures you collected to the
--         JupyterLab.
--         To do this, click the upload icon just above the search box in the
--         top left corner and select the pictures to upload. After you upload
--         the pictures, you should see them appear in the list of files on the
--         left side.

-- 7.2.5   Click the terminal icon in the main panel and connect to Snowflake
--         using the following command:

/*
snowsql -a <account name> -u <user name> -r TRAINING_ROLE
*/

--         Enter your password when prompted.

-- 7.2.6   Once connected to Snowflake, set your context to use the UNSTRUCTURED
--         schema and your warehouse.

USE BADGER_db.unstructured;
USE warehouse BADGER_wh;


-- 7.2.7   Use the PUT command to put the 5 pictures you downloaded onto the
--         members_pic stage.
--         The syntax for the JupyterLab environment (assuming you loaded the
--         pictures at the top level of the file system) is:

/*
PUT file://*.jpeg @member_pics;
*/

--         You should see the list of files output in your terminal window with
--         a status of UPLOADED.
--         The pictures will now have a .gz extension, in additional to the
--         original extension.

-- 7.2.8   When the files are uploaded, exit from SnowSQL
--         To exit from SnowSQL use !exit and exit from the JupyterLab by
--         clicking File > Log Out from the top menu bar.

-- 7.3.0   Build a View That Includes Member Pictures

-- 7.3.1   Return to your Worksheet in Snowsight

-- 7.3.2   List the stage to verify that the files are there. Set your context
--         first if you need to.

-- ANSWER AVAILABLE IN ANSWER KEY


-- 7.3.3   As the role SYSADMIN, refresh the stage to update the directory
--         table. Then list the directory table to see the files.

-- ANSWER AVAILABLE IN ANSWER KEY


-- 7.3.4   As TRAINING_ROLE, create a table with two columns (lname and URL)
--         that will hold the member pictures. Name the table member_pics.

-- ANSWER AVAILABLE IN ANSWER KEY


-- 7.3.5   Populate the table with your data. Use the function
--         build_stage_file_URL to populate the URL column.

-- ANSWER AVAILABLE IN ANSWER KEY

--         The value you use for <path to picture> must EXACTLY match the names
--         of the files in the stage, including the case of the letters and the
--         extensions. For example, Lastname.jpg.gz where it matches column name
--         RELATIVE_PATH from the SELECT * FROM DIRECTORY(@member_pics); you ran
--         earlier.

-- 7.3.6   Select from the MEMBER_PICS table, and click one of the URLs to
--         verify you can download the picture
--         If you get an XML error when you attempt to download the picture -
--         check that the file names you used when building the member_pics
--         table exactly match the file names for the associated pictures.

-- 7.3.7   Create a view that joins the MEMBERS table to the MEMBER_PICS table,
--         and include the member ID, first name, last name, and picture URL

-- ANSWER AVAILABLE IN ANSWER KEY


-- 7.3.8   Select from the view and make sure you can download the pictures

-- 7.3.9   View your classmates Pictures
--         Find the UNSTRUCTURED schema in one of your classmates’ databases,
--         and select from their view (if it’s finished) to see what pictures
--         they used.
