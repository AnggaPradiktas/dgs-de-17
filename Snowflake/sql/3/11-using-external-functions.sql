
-- 11.0.0  Using External Functions
--         In this lab you will practice the following:
--         - Create an external function that uses an API Integration that was
--         already created for you by the administrator
--         - Use SQL to execute the external function

-- 11.1.0  Investigate the API Integration

-- 11.1.1  Open a new worksheet or Create Worksheet from SQL File and set your
--         context

USE ROLE training_role;
USE DATABASE BADGER_db;
USE SCHEMA public;
USE WAREHOUSE BADGER_wh;

--         First use the SHOW command to find the API Integration name that was
--         previously setup for the class. Then we will use that name and run
--         the DESCRIBE command to get the details.

-- 11.1.2  Use SHOW API INTEGRATIONS command
--         Using the SHOW command find the name that starts with edu.

SHOW API INTEGRATIONS LIKE 'edu%';


-- 11.1.3  Get more details about the API Integration
--         Take the integration name and use the DESCRIBE command to get details
--         about the API Integration object.

DESCRIBE API INTEGRATION edu_extfcn_api_integration;


-- 11.2.0  Create an External Function
--         Next we will use this data to create an External Function using a
--         remote service that is already in place. In particular, we will use a
--         zip code lookup remote service called zip_lookup.

-- 11.2.1  Create the external function called zip_lookup
--         Using the CREATE EXTERNAL FUNCTION command make an external function
--         called zip_lookup that uses the API Integration you just described.
--         Note it will be returning JSON so we set the returns to VARIANT.

CREATE OR REPLACE EXTERNAL FUNCTION zip_lookup(zipcode varchar)
RETURNS VARIANT
API_INTEGRATION = edu_extfcn_api_integration
AS
'https://90ch70of36.execute-api.us-west-2.amazonaws.com/dev/edu_extfcn/zip_lookup';


-- 11.2.2  Use SHOW EXTERNAL FUNCTIONS to verify it was created
--         Next use the SHOW command to see the External Function you just
--         created.

SHOW EXTERNAL FUNCTIONS LIKE '%ZIP%';


-- 11.3.0  Call the External Function to execute the remote service code
--         Now use the External Function you just created to execute the remote
--         service outside of Snowflake and return results. In particular,
--         submit an SQL query passing a zip code to have the external function
--         do a lookup.

-- 11.3.1  Use a simple SELECT to lookup a zip code
--         Do a very simple SQL select and pass the zip code of 59715 to our
--         remote service code via the zip_lookup external function.

SELECT zip_lookup(59715);

--         What city did this zip code return?

-- 11.3.2  Parse the JSON returned as a Variant
--         Since JSON is being returned from the external function and it uses
--         the VARIANT data type you can parse this data. Here is an example,
--         using the external function as a sub query, to just select the city
--         and state using Bracket Notation. Note: since there is one object in
--         the JSON array it is referenced starting with zero.

SELECT v[0]['city'] AS city,
       v[0]['state'] AS state
FROM (SELECT zip_lookup(59715) AS v);

--         You can also use Dot Notation:

SELECT v[0]:city AS city,
       v[0]:state AS state
FROM (SELECT zip_lookup(59715) AS v);

--         Why is this city and state important for Snowflake?

-- 11.4.0  Create another External Function and call it
--         Let’s create another External function that takes the city name as
--         input and returns zip codes associated with that city name.

-- 11.4.1  Create the new external function called zips_by_city:

CREATE OR REPLACE EXTERNAL FUNCTION zips_by_city(city varchar)
RETURNS VARIANT
API_INTEGRATION = edu_extfcn_api_integration
AS
'https://90ch70of36.execute-api.us-west-2.amazonaws.com/dev/edu_extfcn/zips_by_city'
;


-- 11.4.2  Use a simple SELECT query to lookup the zip codes for a given city
--         name:
--         This time pass the city name you found with the 1st external
--         function, to the new External function you just created, to return
--         all the zip codes associated with that city name.

SELECT zips_by_city('Bozeman');

--         This query returned a JSON array with multiple objects. Here is an
--         example query using Bracket Notation to reference multiple objects.

SELECT v[0]['zip_code'] AS Zip_Code1,
       v[1]['zip_code'] AS Zip_Code2,
       v[2]['zip_code'] AS Zip_Code3,
       v[3]['zip_code'] AS Zip_Code4,
       v[4]['zip_code'] AS Zip_Code5,
       v[5]['zip_code'] AS Zip_Code6,
       v[6]['zip_code'] AS Zip_Code7,
       v[7]['zip_code'] AS Zip_Code8
FROM (SELECT zips_by_city('Bozeman') AS v);

--         How many zip codes does this city have?

-- 11.5.0  Dropping an External Function
--         It is simple to drop and external function, but not obvious when you
--         first try it.

-- 11.5.1  First try what you think is the correct syntax to drop an External
--         Function
--         You will get an error running this drop command:

DROP FUNCTION zips_by_city;


-- 11.5.2  Now try it by adding data type
--         Now try this command which includes the data type passed to the
--         External Function:

DROP FUNCTION zips_by_city(varchar);

--         Do the SHOW command and then drop the other external function you
--         created:

SHOW EXTERNAL FUNCTIONS LIKE '%ZIP%';

DROP FUNCTION zip_lookup(varchar);

