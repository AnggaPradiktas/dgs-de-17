
-- 8.0.0   TOPIC: Extensibility Features

-- 8.1.0   JavaScript User-Defined Functions

-- 8.1.1   Navigate to Worksheets and load the worksheet for this lab that has
--         been provided for you.

-- 8.1.2   Set the worksheet context as follows:
--         ROLE: TRAINING_ROLE
--         WAREHOUSE: BADGER_LOAD_WH
--         DATABASE: BADGER_DB
--         SCHEMA: PUBLIC

ALTER SESSION SET QUERY_TAG='(BADGER) Lab - TOPIC: Extensibility Features';
USE ROLE TRAINING_ROLE;
USE WAREHOUSE BADGER_LOAD_WH;
ALTER WAREHOUSE BADGER_LOAD_WH SET WAREHOUSE_SIZE = 'XSMALL';
USE database BADGER_DB;
USE SCHEMA PUBLIC;

--         Verify that your worksheet context is set to use your user database
--         and the public schema.

-- 8.1.3   Create a table named D2B with one column of type INT.

CREATE OR REPLACE TABLE D2B(I INT);


-- 8.1.4   Populate the table with values.

INSERT INTO D2B VALUES (-60000),(-1),(1),(22),(2000),(100000),(123123123);


-- 8.1.5   Verify the values were inserted.

SELECT * FROM D2B;


-- 8.1.6   Create a JavaScript UDF
--         In this step you will create a UDF (function) that takes two
--         arguments. The first argument accepts an integer value and creates a
--         byte array containing the number of elements equal to the second
--         argument. The function loops through the byte array extracting bytes
--         out of the number field converting them to their binary value.

CREATE OR REPLACE FUNCTION INT_TO_BIN(NUM FLOAT,BYTES FLOAT)
RETURNS BINARY
LANGUAGE JAVASCRIPT
AS $$

  var byteArray = new Uint8Array(BYTES);

  for(var x=0;x<BYTES;x++){
    byteArray[x] = (NUM >>> (x*8))&0xFF;
  }
  return byteArray;

$$;


-- 8.1.7   Query using the UDF
--         In this task you will call the function in a select statement,
--         returning the byte array for different size arrays using the value
--         for the column labeled I.

SELECT
   I
  ,INT_TO_BIN(I,1)
  ,INT_TO_BIN(I,2)
  ,INT_TO_BIN(I,4)
FROM D2B;


-- 8.2.0   SQL User-defined Functions

-- 8.2.1   Set up test data to be accessed by a new SQL UDF

-- 8.2.2   Create a table named PURCHASES as follows:

CREATE OR REPLACE TABLE PURCHASES
  (
   NUMBER_SOLD INTEGER
  ,WHOLESALE_PRICE NUMBER(7,2)
  ,RETAIL_PRICE NUMBER(7,2)
  );


-- 8.2.3   INSERT data into the table

INSERT INTO purchases (number_sold, wholesale_price, retail_price) values
  (3,  10.00,  20.00),
  (5, 100.00, 200.00);


-- 8.2.4   CREATE the SQL UDF
--         The PROFIT function we are creating determines profit by running a
--         select statement and performing calculations. It shows executing a
--         SELECT statement inside of a SQL UDF. Since the UDF calculates and
--         returns a single value, it is called a scalar function.

CREATE OR REPLACE FUNCTION PROFIT()
 RETURNS NUMBER(11, 2)
 AS $$
  SELECT SUM((RETAIL_PRICE - WHOLESALE_PRICE) * NUMBER_SOLD) from PURCHASES
 $$;


-- 8.2.5   QUERY using the SQL UDF

SELECT PROFIT();


-- 8.3.0   Stored Procedures

-- 8.3.1   Create a table to act as source data, to be used by a new stored
--         procedure, as follows:

CREATE or REPLACE TABLE SERVICEDETAIL
(
   RO VARCHAR
  ,RODATE VARCHAR
  ,SERVICE VARCHAR
  ,REV VARCHAR
  ,COST VARCHAR
);


-- 8.3.2   Insert rows of data into the table

INSERT INTO SERVICEDETAIL
VALUES ('183297','2018-01-01','DGR123|OIL543|FLT1241','18.67|43.23|10.87',
        '11.17|22.11|4.45');

INSERT INTO SERVICEDETAIL
 VALUES('183298','2018-01-02','BFR432|BRK132','11.43|41','7.17|18.11');


-- 8.3.3   Create a target table as follows:

CREATE OR REPLACE TABLE RODETAIL (
RO NUMBER,
RODATE DATE,
SERVICE VARCHAR,
REVENUE FLOAT,
COST FLOAT
);


-- 8.3.4   Create the stored procedure
--         The JavaScript code starts after the first double-dollar-sign
--         delimiter, $$, and ends with a $$ delimiter. All code between these
--         delimiters must be correct JavaScript syntax. Remember that
--         JavaScript is case sensitive while SQL is not case sensitive. Note
--         also that you will not receive feedback on any errors in JavaScript
--         syntax (or case-sensitivity) at stored procedure creation time. Any
--         errors of this type will only be reported on first invocation of the
--         procedure.
--         This PARSE_SERVICE_DATA() stored procedure demonstrates executing two
--         SQL statements. The first statement returns a result set. Next the
--         logic iterates through the result set and dynamically creates an
--         INSERT statement that is executed with values from each row returned
--         by the first statement.

CREATE OR REPLACE PROCEDURE PARSE_SERVICE_DATA()
    RETURNS VARCHAR NOT NULL -- "success" or an error message if not successful
    LANGUAGE JAVASCRIPT
    EXECUTE AS OWNER
    AS $$

      // Select string to retrieve data
      var sql_cmd = "SELECT * from SERVICEDETAIL";

      // Use the snowflake object to create a statement object
      var stmt = snowflake.createStatement( {sqlText: sql_cmd} );
      // Use the statement object to execute the query. The return is a ResultSet
      var rs = stmt.execute();

      //Declare target variable for RO number and close date
      var RO, RODate;
      //Declare variables that will get each delimited cell of data before it is split.
      var service, rev, cost;
      //Declare target arrays for the Service Codes and Revenue and Cost for each Service
      var serviceValues = [], revValues = [], costValues = [];

      //Declare variables for creating the query for inserting data into our new table, RODETAIL
      var insertInto1 = "INSERT INTO RODETAIL VALUES (", insertInto2;

      //Iterate through the ResultSet object one row at a time
      // The next() method moves one row down and returns true if a row exists
      while (rs.next()) {
        RO = rs.getColumnValue('RO'); // retrieve data from the row
        RODate = rs.getColumnValue('RODATE');

        // Get an array of the different services performed.
        service = rs.getColumnValue('SERVICE');
        serviceValues = service.split("|");
        rev = rs.getColumnValue('REV');
        revValues = rev.split("|");

        // Get an array of the revenue for the costs of the services performed.
        cost = rs.getColumnValue('COST');
        costValues = cost.split("|");

        // Loop through the services array and insert a record into the table  
        // for each individual service, keeping the RO number and RODate constant
        var arrayLength = serviceValues.length;
        for (var i = 0; i < arrayLength; i++) {
          insertInto2 = RO + ", '"  + RODate + "', '" + serviceValues[i] + "', "
            + revValues[i] + "," + costValues[i] + ")";
          snowflake.execute( {sqlText: insertInto1 + insertInto2} );
        } // end for loop

     }// end while loop

     return "success";     
     $$ -- end Javascript code
; -- end CREATE PROCEDURE statement


-- 8.3.5   Invoke the stored procedure

-- 8.3.6   Call the stored procedure

CALL PARSE_SERVICE_DATA();


-- 8.3.7   View the results of calling the PARSE_SERVICE_DATA() stored procedure
--         on the RODETAIL table.

SELECT * FROM RODETAIL;


-- 8.3.8   Finally, let’s remove the query tag as the work for this lab is
--         complete.

ALTER SESSION UNSET QUERY_TAG;


