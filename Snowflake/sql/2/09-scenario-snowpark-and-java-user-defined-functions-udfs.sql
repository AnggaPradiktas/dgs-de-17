
-- 9.0.0   SCENARIO: Snowpark and Java User Defined Functions (UDFs)
--         The SnowBearAir business team is keen to understand how SnowBearAir
--         is perceived by their customers. Customers are an organization’s
--         lifeblood, so understanding if there are any areas of trouble or
--         discontent among their user base is crucial to ensuring SnowBearAir
--         remains at the top of industry polling and customer satisfaction
--         surveys.
--         Luckily, some airline reviews have been made available, freely via
--         CC0, on the web. Your tasks within these labs will be to lay the
--         foundations for working with Snowpark and Java UDFs. Then, using
--         these new skills and capabilities, ingest review data and cleanse and
--         transform it. Along the way you will also create a function to
--         perform sentiment analysis to augment the data, before publishing and
--         presenting it for consumption by your colleagues in the organization
--         - including developers who will use this to drive an executive
--         sentiment analysis dashboard.
--         - Gain hands on experience with Snowpark
--         - Understand common methods and functions for working with Snowpark
--         - Practice developing an entire data pipeline flow - from ingest and
--         transform through to publishing datasets in Snowpark
--         - Gain hands on experience building Java UDFs and familiarity with
--         different options (i.e., anonymous, temporary, etc)
--         - Build knowledge on using local language features in Scala in
--         conjunction with Snowpark

-- 9.1.0   Lab A: Snowpark Sessions and Lazy Evaluation
--         This section will be done in the Jupyter Labs notebook interface:
--         https://labs.snowflakeuniversity.com
--         JUPYTER NOTEBOOKS
--         In this lab your work will be done in the provided Jupyter notebook
--         environment, rather than the Snowflake Web UI. Once you have
--         completed each exercise you can feel free to shut down that notebook,
--         and should certainly do so once completing each section (A-F), before
--         moving to the next. To do so, right-click on the notebook name and
--         select Shut Down Kernel.

-- 9.1.1   Login to https://labs.snowflakeuniversity.com
--         You only need to complete these one-time Notebook file upload steps
--         if you have not done this already for your Jupyter instance earlier
--         in the course.

-- 9.1.2   Click on the upload icon - an up arrow (see screenshot below)
--         A file dialog will appear. Upload the notebook zip file identified
--         earlier. Note that your version number may differ from that in the
--         screenshot below as you are working with a later, enhanced release.
--         Once the upload is complete right click on the zip file in the
--         directory tree in Jupyter and select Extract Archive to unpack the
--         file content. This will create three top level directories,
--         de_connectors, de_snowpark and de_streaming. Each contains multiple
--         files and/or sub-directories.
--         Upload Files
--         In this lab section you will learn the basics of establishing a
--         session within Snowpark, and performing simple DataFrame operations.
--         We will see that there is a specific processing order that Snowpark
--         applies to different operations, and this knowledge will be
--         foundational for the rest of the lab exercises.
--         Snowpark topic labs

-- 9.1.3   Open the notebook de_snowpark/A-Dataframes/01-Sessions.ipynb in the
--         Jupyter Labs environment.
--         Follow the instructions and/or run the notebook cells.

-- 9.1.4   Then move to the notebook
--         de_snowpark/A-Dataframes/02-LazyEvaluation.ipynb and open this in the
--         Jupyter Labs environment.
--         Follow the instructions and/or run the notebook cells.

-- 9.2.0   Lab B: Snowpark Transformations - Filters, Group By and Joins
--         This section will be done in the Jupyter Labs notebook interface:
--         https://labs.snowflakeuniversity.com
--         In this lab section we turn our attention to learning how to refine,
--         shape and aggregate datasets, something we are very familiar with in
--         the context of using SQL (i.e., WHERE, GROUP BY, ORDER BY, etc.),
--         working with methods and functions that Snowpark provides to
--         implement this logic with DataFrames.

-- 9.2.1   Open the notebook de_snowpark/B-Transforms/01-Filters.ipynb in the
--         Jupyter Labs environment.
--         Follow the instructions and/or run the notebook cells.

-- 9.2.2   Then open the notebook de_snowpark/B-Transforms/02-GroupBy.ipynb in
--         the Jupyter Labs environment.
--         Follow the instructions and/or run the notebook cells.

-- 9.2.3   Finally, open the notebook de_snowpark/B-Transforms/03-Joins.ipynb in
--         the Jupyter Labs environment.
--         Follow the instructions and/or run the notebook cells.

-- 9.3.0   Lab C: Loading and Transforming JSON Data in Snowpark
--         This section will be done in the Jupyter Labs notebook interface:
--         https://labs.snowflakeuniversity.com
--         In the third lab section we turn our attention to the practical
--         matter of uploading and reading data into DataFrames. We will work
--         with JSON data, transforming and casting this, and ultimately
--         delivering this end-product dataset through to a table in Snowflake
--         for others to consume.

-- 9.3.1   Open the notebook de_snowpark/C-Loading/01-Loading-json.ipynb in the
--         Jupyter Labs environment.
--         Follow the instructions and/or run the notebook cells.

-- 9.3.2   Then open the notebook de_snowpark/C-Loading/02-Loading-json-
--         xform.ipynb in the Jupyter Labs environment.
--         Follow the instructions and/or run the notebook cells.

-- 9.4.0   Lab D: Java User Defined Functions (UDFs) from Snowpark
--         This section will be done in the Jupyter Labs notebook interface:
--         https://labs.snowflakeuniversity.com
--         Java User Defined functions (UDFs) are the hero of this lab section,
--         and we will focus on developing and using them within the Snowpark
--         context. We will learn about the different types that exist and how
--         we can make use of them in conjunction with local language features
--         in Scala. Along the way we will also develop Java UDFs to display
--         graphical representations of data, and process sentiment analysis
--         across a sample set of example text.

-- 9.4.1   Open the notebook de_snowpark/D-JavaUDF/01-Snowpark-JavaUDF.ipynb in
--         the Jupyter Labs environment.
--         Follow the instructions and/or run the notebook cells.

-- 9.4.2   Then open the notebook de_snowpark/D-JavaUDF/02-Snowpark-JavaUDF-
--         tinychart.ipynb in the Jupyter Labs environment.
--         Follow the instructions and/or run the notebook cells.

-- 9.4.3   Finally, open the notebook de_snowpark/D-JavaUDF/03-Snowpark-JavaUDF-
--         jars.ipynb in the Jupyter Labs environment.
--         Follow the instructions and/or run the notebook cells.

-- 9.5.0   Lab E: Scala Visualization
--         This section will be done in the Jupyter Labs notebook interface:
--         https://labs.snowflakeuniversity.com
--         In the single lab in this section, we will take a look at
--         implementing an open source charting solution, in conjunction with a
--         Java UDF, to show Airline average flight time delayed minutes, for a
--         particular subset of operators and locations.

-- 9.5.1   Open the notebook de_snowpark/E-Visualization/01-delays-bar-
--         chart.ipynb in the Jupyter Labs environment.
--         Follow the instructions and/or run the notebook cells.

-- 9.6.0   Lab F: Reviews
--         This section will be done in the Jupyter Labs notebook interface:
--         https://labs.snowflakeuniversity.com
--         In this final lab section, we will call on all our learning from the
--         previous labs in this section to develop an end to end pipeline
--         solution for SnowBearAir, to help them better understand customer
--         sentiment. This will include working with DataFrames to load, cleanse
--         and conform, and publish relevant data.
--         Snowpark scenario lab

-- 9.6.1   Open the notebook de_snowpark/F-Reviews/01-reviews-load.ipynb in the
--         Jupyter Labs environment.
--         Follow the instructions and/or run the notebook cells.

-- 9.6.2   Then open the notebook de_snowpark/F-Reviews/02-reviews-refine.ipynb
--         in the Jupyter Labs environment.
--         Follow the instructions and/or run the notebook cells.

-- 9.6.3   Finally, open the notebook de_snowpark/F-Reviews/03-reviews-
--         publish.ipynb in the Jupyter Labs environment.
--         Follow the instructions and/or run the notebook cells.
