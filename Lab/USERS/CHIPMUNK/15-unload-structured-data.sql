
-- 15.0.0  Unload Structured Data
--         Expect this lab to take approximately 40 minutes.

-- 15.1.0  Unload a Pipe-Delimited File to an Internal Stage

-- 15.1.1  Open a worksheet and set your context:

USE ROLE TRAINING_ROLE;
CREATE WAREHOUSE IF NOT EXISTS CHIPMUNK_WH;
USE WAREHOUSE CHIPMUNK_WH;
CREATE DATABASE IF NOT EXISTS CHIPMUNK_DB;
USE CHIPMUNK_DB.PUBLIC;


-- 15.1.2  Create a fresh version of the REGION table with 5 records to unload:

CREATE OR REPLACE TABLE region AS 
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION;


-- 15.1.3  Unload the data to the REGION table stage.
--         Remember that a table stage is automatically created for each table.
--         You will use MYPIPEFORMAT for the unload:

COPY INTO @%region
FROM region
FILE_FORMAT = (FORMAT_NAME = TRAINING_DB.TRAININGLAB.MYPIPEFORMAT);


-- 15.1.4  List the stage and verify the data is there:

LIST @%region;


-- 15.1.5  (OPTIONAL) Download the files to your local system.
--         Use the GET command to download all files in the REGION table stage
--         to local directory.
--         The Snowflake Web UI does not support the GET command. If you have
--         the SnowSQL command line client installed, use it to connect to
--         Snowflake and execute the GET command.

GET @%region file:///<path to dir> ; -- this is for Linux or MacOS
GET @%region file://c:<path to dir>; -- this is for Windows

--         After the files are downloaded to your local file system, open them
--         with an editor and see what they contain.

-- 15.1.6  Remove the file from the REGION table’s stage:

REMOVE @%region;


-- 15.2.0  Unload Part of a Table

-- 15.2.1  Create a new table from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS:

CREATE TABLE new_orders AS
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS;


-- 15.2.2  Unload the columns o_orderkey, o_orderstatus, and o_orderdate from
--         your new table, into the table’s stage:
--         Remember that a table stage is automatically created for every table.
--         Use the default file format.

COPY INTO @%new_orders FROM
(SELECT o_orderkey, o_orderstatus, o_orderdate FROM new_orders);


-- 15.2.3  Verify the output is in the stage:

LIST @%new_orders;


-- 15.2.4  (OPTIONAL) Download the files to your local system.
--         Use the GET command to download all files in the new_orders table
--         stage to local directory.
--         The Snowflake Web UI does not support the GET command. If you have
--         the SnowSQL command line client installed, use it to connect to
--         Snowflake and execute the GET command.

GET @%new_orders file:///<path> -- for Linux or MacOS
GET @%new_orders file://c:<path -- For Windows

--         How many files did you get? At what point did COPY INTO decide to
--         split the files?

-- 15.2.5  Remove the files from the stage:

REMOVE @%new_orders;


-- 15.2.6  Repeat the unload with the selected columns, but this time specify
--         SINGLE=TRUE in your COPY INTO command.
--         Also provide a name for the output file as part of the COPY INTO:

COPY INTO @%new_orders/new_orders.csv.gz FROM
(SELECT o_orderkey, o_orderstatus, o_orderdate FROM new_orders)
SINGLE=TRUE;


-- 15.2.7  (OPTIONAL) Download the files to your local system.
--         Use the GET command to download all files in the new_orders table
--         stage to local directory.
--         The Snowflake Web UI does not support the GET command. If you have
--         the SnowSQL command line client installed, use it to connect to
--         Snowflake and execute the GET command.

GET @%new_orders file:///<path> -- for Linux or MacOS
GET @%new_orders file://c:<path -- For Windows


-- 15.2.8  Remove the file from the stage:

REMOVE @%new_orders;


-- 15.3.0  JOIN and Unload a Table

-- 15.3.1  Run a SELECT with a JOIN on the REGION and NATION tables.

SELECT * 
FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."REGION" r 
JOIN "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."NATION" n ON r.r_regionkey = n.n_regionkey;


-- 15.3.2  Create a named internal stage.

CREATE STAGE CHIPMUNK_stage;


-- 15.3.3  Unload the JOINed data into the stage you created.

COPY INTO @CHIPMUNK_stage FROM
(SELECT * FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."REGION" r JOIN "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."NATION" n
ON r.r_regionkey = n.n_regionkey);


-- 15.3.4  Verify the file is in the stage.

LIST @CHIPMUNK_stage;


-- 15.3.5  (OPTIONAL) Download the files to your local system.
--         Use the GET command to download all files in the @mystage stage to
--         local directory.
--         The Snowflake Web UI does not support the GET command. If you have
--         the SnowSQL command line client installed, use it to connect to
--         Snowflake and execute the GET command.

GET @mystage file:///<path> -- for Linux or MacOS
GET @mystage file://c:<path -- For Windows


-- 15.3.6  Remove the file from the stage.

REMOVE @CHIPMUNK_stage;


-- 15.3.7  Remove the stage.

DROP STAGE CHIPMUNK_stage;

