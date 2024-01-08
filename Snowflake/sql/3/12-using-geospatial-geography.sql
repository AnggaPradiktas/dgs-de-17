
-- 12.0.0  Using Geospatial Geography
--         In this lab you will learn and practice the following:
--         - How to acquire geospatial data from the Snowflake Marketplace
--         - How to interpret the GEOGRAPHY data type
--         - How to understand the different formats that GEOGRAPHY can be
--         expressed in
--         - How to display Geospatial Coordinates
--         Geospatial query capabilities in Snowflake are built upon a
--         combination of data types and specialized query functions that can be
--         used to parse, construct, and run calculations over geospatial
--         objects. This lab will introduce you to the GEOGRAPHY data type, help
--         you understand geospatial formats supported by Snowflake, and walk
--         you through a simple scenario using sample geospatial data set that
--         involves points-of-interest in New York City from the Snowflake
--         Marketplace.

-- 12.1.0  Acquire geospatial Data from Snowflake Marketplace

-- 12.1.1  Check your context:
--         If you are in a Worksheet then click the Home icon in the upper left
--         hand corner of Snowsight. Check that your role is set to
--         TRAINING_ROLE.

-- 12.1.2  Locate share and get data
--         Now you can acquire sample geospatial data from the Snowflake
--         Marketplace.
--         - Click on Marketplace menu item the left side of the window
--         - Under the Categories drop down and select Geospatial
--         - Next under Providers select Sonra
--         - Then scroll down and find OpenStreetMap New York and click the tile
--         - Once in the listing, click the big blue Get Data button if shown.
--         Another user may have already performed this step and you can skip to
--         the Query Data step below. Note: On the Get Data screen, you may be
--         prompted to complete your user profile if you have not done so
--         before. Click complete your user profile link and enter your name and
--         email address into the profile screen and click the blue Save button.
--         - On the Get Data screen, change the name of the database from the
--         default to OSM_NEWYORK, as this name is shorter and all of the future
--         instructions will assume this name for the database. Then click Get
--         Data to close the window.
--         - Click the big blue Query Data button which will open a worksheet
--         with example queries from provider that you are welcome to run later.
--         You are not going to run any of these queries in this lab so click
--         home icon. Note: If you do run these queries later you need to set
--         the schema to NEW_YORK.
--         Congratulations! You have just created a shared database from a
--         listing on the Snowflake Marketplace.

-- 12.2.0  Understand Geospatial Formats
--         Create a new Worksheet from SQL File, and run different queries to
--         understand how the GEOGRAPHY data type works in Snowflake.

-- 12.2.1  Create Worksheet from SQL File or open a new worksheet and set your
--         context to the new share

USE ROLE training_role;
USE DATABASE osm_newyork;
USE SCHEMA new_york;
USE WAREHOUSE BADGER_wh;

--         You are now ready to run some queries against this free data.

-- 12.2.2  The GEOGRAPHY data type
--         Snowflake’s GEOGRAPHY data type is similar to the GEOGRAPHY data type
--         in other geospatial databases in that it treats all points as
--         longitude and latitude on a spherical earth instead of a flat plane.
--         This is an important distinction from other geospatial types (such as
--         GEOMETRY), but this guide won’t be exploring those distinctions. More
--         information about Snowflake’s specification can be found at
--         https://docs.snowflake.com/en/sql-reference/data-types-
--         geospatial.html documentation site.
--         Look at one of the views in the shared database which has a GEOGRAPHY
--         column by running the following queries.

DESCRIBE VIEW v_osm_ny_amenity_education;

--         Notice the coordinates column is defined of GEOGRAPHY type. This is
--         the column you will focus on in the next lab steps.

-- 12.2.3  View GEOGRAPHY Output Formats
--         Snowflake supports 3 primary geospatial formats and 2 additional
--         variations on those formats. They are:
--         - GeoJSON: a JSON-based standard for representing geospatial data
--         - WKT & EWKT: a Well Known Text string format for representing
--         geospatial data and the Extended variation of that format
--         - WKB & EWKB: a Well Known Binary format for representing geospatial
--         data in binary and the Extended variation of that format
--         These formats are supported for ingestion (files containing those
--         formats can be loaded into a GEOGRAPHY typed column), query result
--         display, and data unloading to new files. You don’t need to worry
--         about how Snowflake stores the data under the covers, but rather how
--         the data is displayed to you or unloaded to files through the value
--         of a session variable called GEOGRAPHY_OUTPUT_FORMAT.
--         Run the query below to make sure the current format is GeoJSON.

ALTER SESSION SET GEOGRAPHY_OUTPUT_FORMAT = 'GEOJSON';

--         The alter session command lets you set a parameter for your current
--         user session, which in this case is the GEOGRAPHY_OUTPUT_FORMAT. The
--         default value for this parameter is GEOJSON, so normally you wouldn’t
--         have to run this command if you want that format, but to be certain
--         the next queries are run with the GEOJSON output.

-- 12.3.0  Visualize the data

-- 12.3.1  Query the Geospatial share data via a view
--         Now run the following query against the v_osm_ny_amenity_education
--         view and look for Universities that start with letter N.

SELECT coordinates,
       name,
       type
FROM v_osm_ny_amenity_education
WHERE amenity = 'university'
AND name LIKE 'N%';

--         In the result set, notice the coordinates column and how it displays
--         a JSON representation of a point or many points. If you look at Type
--         column you will see node or way. Find New York University School of
--         Law and click COORDINATES cell then to right and Click to Copy icon
--         which will copy this JSON text to clip board.
--         Next open a new Web Browser window and bring up the site geojson.io
--         https://geojson.io/ where you will see a map of the World.
--         Take your COORDINATES for New York University School of Law, type
--         node, and replace the </>JSON code in right side of the map. Then
--         click the - minus sign 4 or 5 times to zoom the map out. You should
--         see New York University near Washington Square Park with a Point
--         placed at the School of Law.

-- 12.3.2  Now view data with many points
--         Find New York University with type = way and click COORDINATES cell
--         then to right and Click to Copy icon which will copy this JSON text
--         to clip board.
--         Take your COORDINATES for New York University, type = way, and
--         replace the </>JSON code in right side of the map. You should see New
--         York University is around Washington Square Park with the School of
--         Law inside University boundaries.

-- 12.3.3  Change the output format
--         Now change the GEOGRAPHY_OUTPUT_FORMAT output to the WKT format and
--         re-run our last query.

ALTER SESSION SET geography_output_format = 'WKT';

SELECT coordinates,
       name,
       type
FROM V_OSM_NY_AMENITY_EDUCATION
WHERE amenity = 'university'
AND name LIKE 'N%';

--         The WKT results looks different than GeoJSON, and is arguably more
--         readable.
--         Next open a new Web Browser window and bring up the site
--         OpenStreetMap WKT Playground:
--         https://clydedacruz.github.io/openstreetmap-wkt-playground/
--         Again take your COORDINATES for New York University School of Law,
--         type node, and replace the code at the bottom and click Plot Shape.
--         Then click the - minus sign 10 or 11 times to zoom the map out. You
--         should see New York University near Washington Square Park with a
--         Point placed at the School of Law.

-- 12.3.4  Now view WKT data with many points
--         Find New York University with type = way and click COORDINATES cell
--         then to right and Click to Copy icon which will copy this JSON text
--         to clip board.
--         Take your COORDINATES for New York University, type = way, and
--         replace the code at the bottom and click Plot Shape. You should see
--         New York University is around Washington Square Park with the School
--         of Law inside University boundaries.

-- 12.4.0  Example Business school scenario using some geospatial functions
--         Imagine there is a small college students group at NYU Stern Business
--         school with a class project to complete. The students have come up
--         with and idea and prototype of a better environmentally friendly dog
--         leash. However, they need to validate their prototype by visiting pet
--         stores within 1 kilometer of the school. How can you use your
--         geospatial data to do this?

-- 12.4.1  First locate the starting coordinates
--         Do a very simple modification of the SQL query we ran earlier to find
--         the coordinates of the NYU Stern Business School.

SELECT coordinates,
       name,
       type
FROM v_osm_ny_amenity_education
WHERE amenity = 'university'
AND name LIKE 'NYU St%';

--         Copy the coordinates into OpenStreetMap WKT Playground website to
--         verify it is the school.

-- 12.4.2  Use geospatial function ST_DWITHIN to locate stores
--         Use the ST_DWITHIN function in the where clause to filter out stores
--         that aren’t within the stated distance of 1000 meters. The function
--         takes two points and a distance to determine whether those two points
--         are less than or equal to the stated distance from each other,
--         returning true if they are and false if they are not. In our scenario
--         you will use the school coordinates and compare it to the pet stores
--         to see if they are within the distance.

SELECT *
FROM v_osm_ny_shop  
WHERE ST_DWITHIN(ST_POINT(-73.9965041,40.7289742),COORDINATES,1000)
AND shop LIKE 'pet%';

--         You will see a handful of pet stores within the distance, but it will
--         not display all of them together.

-- 12.4.3  Use ST_COLLECT
--         Use ST_COLLECT to aggregate the rows into a single
--         GEOMETRYCOLLECTION. However, the points for the stores shows up
--         better in GeoJSON so reset you session to that output first.

ALTER SESSION SET GEOGRAPHY_OUTPUT_FORMAT = 'GEOJSON';

SELECT ST_COLLECT(COORDINATES)
FROM v_osm_ny_shop  
WHERE ST_DWITHIN(ST_POINT(-73.9965041,40.7289742),COORDINATES,1000)
AND shop LIKE 'pet%';

--         Now copy the results to the geojson.io website to see the stores
--         surrounding the business school.
--         Job well done… your four legged friends will love you!
--         Final note: It’s worth noting here that the simple scenario is more
--         akin to what a person would do with a map application on their mobile
--         phone, rather than how geospatial data would be used in fictional
--         business setting. This was chosen intentionally to make this lab and
--         these queries more relatable to the student, rather than trying to
--         create a realistic business scenario that is relatable to all
--         industries, since geospatial data is used very differently across
--         industries.
