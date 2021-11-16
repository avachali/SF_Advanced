
-- 10.0.0  Work with External Tables
--         This lab will take approximately 35 minutes to complete.

-- 10.1.0  Unload Data to Cloud Storage as a Data Lake
--         To start, you will unload data from the Citibike table onto an
--         external stage.

-- 10.1.1  Set your context.

--Note that we can select the database and schema in one statement
USE ROLE training_role;
CREATE SCHEMA IF NOT EXISTS CHIPMUNK_db.CITIBIKE;
USE schema CHIPMUNK_db.CITIBIKE;
CREATE WAREHOUSE IF NOT EXISTS CHIPMUNK_QUERY_WH;
USE WAREHOUSE CHIPMUNK_QUERY_WH;


-- 10.1.2  Determine the size of the Citibike database.
--         Create a query to get row count and size from Citibike table. Record
--         the results for later.

SELECT TABLE_NAME
     , ROW_COUNT
     , BYTES
     , BYTES / (50*1024*1024) as NUM_CHUNKS
     , NUM_CHUNKS/8 as MAX_NODES
   FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
   WHERE table_name like 'TRIPS'
     AND table_schema like 'SCHEMA1'
     AND table_catalog like 'CITIBIKE';

--         The result likely indicates that the maximum number of nodes required
--         is between 4 and 5. A medium cluster is 4 nodes, and a large cluster
--         is 8 nodes. A large cluster should run the dump faster, but the
--         medium will use fewer credits.

-- 10.2.0  Unload Data to Cloud Storage with Different Warehouse Sizes

-- 10.2.1  Set your context.

USE ROLE training_role;
USE SCHEMA citibike.schema1;
USE WAREHOUSE CHIPMUNK_load_wh;


-- 10.2.2  Set the warehouse size to MEDIUM for the first test.

ALTER WAREHOUSE CHIPMUNK_load_wh SET WAREHOUSE_SIZE=MEDIUM;


-- 10.2.3  Copy data to the external stage.

COPY INTO '@TRAINING_DB.TRAININGLAB.CLASS_STAGE/COURSE/ADVANCED/student/CHIPMUNK/citibike1'
  FROM (SELECT * FROM CITIBIKE.SCHEMA1.TRIPS)
  FILE_FORMAT=(FORMAT_NAME=training_db.traininglab.MYGZIPPIPEFORMAT)
  MAX_FILE_SIZE=49000000
;

ls @TRAINING_DB.TRAININGLAB.CLASS_STAGE/COURSE/ADVANCED/student/CHIPMUNK/citibike1;


-- 10.2.4  Set the warehouse size to LARGE for the second test.

ALTER WAREHOUSE CHIPMUNK_load_wh SUSPEND;

ALTER WAREHOUSE CHIPMUNK_load_wh SET WAREHOUSE_SIZE=LARGE;


-- 10.2.5  Copy the data to the stage.

COPY INTO '@TRAINING_DB.TRAININGLAB.CLASS_STAGE/COURSE/ADVANCED/student/CHIPMUNK/citibike2'
  FROM (SELECT * FROM CITIBIKE.SCHEMA1.TRIPS)
  FILE_FORMAT=(FORMAT_NAME=training_db.traininglab.MYGZIPPIPEFORMAT)
  MAX_FILE_SIZE=49000000
;

ls @TRAINING_DB.TRAININGLAB.CLASS_STAGE/COURSE/ADVANCED/student/CHIPMUNK/citibike2;


-- 10.2.6  View the query profiles to see the performance difference.
--         Looking at the results of the two approaches we do see that the large
--         cluster took less clock time than the medium cluster. We also see
--         files of varying sizes. Both approaches ended up with approximately
--         64 files.

-- 10.2.7  Clean up the stage.

remove @TRAINING_DB.TRAININGLAB.CLASS_STAGE/COURSE/ADVANCED/student/CHIPMUNK/citibike1;

remove @TRAINING_DB.TRAININGLAB.CLASS_STAGE/COURSE/ADVANCED/student/CHIPMUNK/citibike2;


-- 10.2.8  Unload the CITIBIKE data into Parquet files.

COPY INTO '@TRAINING_DB.TRAININGLAB.CLASS_STAGE/COURSE/ADVANCED/student/CHIPMUNK/citibike/trips'
  FROM (SELECT * FROM CITIBIKE.SCHEMA1.TRIPS)
  FILE_FORMAT=(FORMAT_NAME=training_db.traininglab.MYPARQUETFORMAT)
  MAX_FILE_SIZE=49000000;

ls @TRAINING_DB.TRAININGLAB.CLASS_STAGE/COURSE/ADVANCED/student/CHIPMUNK/citibike;


-- 10.2.9  Create an external table using the unloaded Parquet files.

USE DATABASE CHIPMUNK_DB;
CREATE OR REPLACE EXTERNAL TABLE EXT_PARQUET_TRIPS
  location= @TRAINING_DB.TRAININGLAB.CLASS_STAGE/COURSE/ADVANCED/student/CHIPMUNK/citibike/
  FILE_FORMAT=(TYPE=PARQUET);

ALTER EXTERNAL TABLE EXT_PARQUET_TRIPS REFRESH;

SELECT * FROM EXT_PARQUET_TRIPS LIMIT 10;


-- 10.2.10 Unload the data using a join between the tpch customer and nation
--         tables.

COPY INTO '@TRAINING_DB.TRAININGLAB.CLASS_STAGE/COURSE/ADVANCED/student/CHIPMUNK/parquet/tpch/myjoin'
  FROM (SELECT c_name
     , c_nationkey
     , c_address
     , c_acctbal
     , n_nationkey
     , n_name
  FROM snowflake_sample_data.tpch_sf1.customer join snowflake_sample_data.tpch_sf1.nation
    on c_nationkey = n_nationkey)
  FILE_FORMAT=(TYPE='PARQUET');

CREATE OR REPLACE EXTERNAL TABLE EXT_PARQUET_myjoin
  location= @TRAINING_DB.TRAININGLAB.CLASS_STAGE/COURSE/ADVANCED/student/CHIPMUNK/parquet/tpch/
  FILE_FORMAT=(TYPE=PARQUET);

ALTER EXTERNAL TABLE EXT_PARQUET_myjoin REFRESH;

SELECT * FROM EXT_PARQUET_myjoin limit 10;


-- 10.3.0  Execute Queries Against External Tables and Metadata

-- 10.3.1  Set your context.

USE ROLE training_role;
USE WAREHOUSE CHIPMUNK_query_wh;
CREATE DATABASE CHIPMUNK_tpcdi_stg;
USE SCHEMA public;


-- 10.3.2  List the staged files.

LIST @training_db.traininglab.ed_stage/finwire;


-- 10.3.3  Create a file format to query the data.

CREATE OR REPLACE FILE FORMAT CHIPMUNK_TPCDI_STG.PUBLIC.TXT_FIXED_WIDTH 
  TYPE = CSV
  COMPRESSION = 'AUTO' 
  FIELD_DELIMITER = NONE
  RECORD_DELIMITER = '\\n' 
  SKIP_HEADER = 0 
  TRIM_SPACE = FALSE 
  ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE 
  NULL_IF = ('\\N');


-- 10.3.4  Create an external table.

CREATE OR REPLACE EXTERNAL TABLE finwire
  LOCATION = @training_db.traininglab.ed_stage/finwire
  REFRESH_ON_CREATE = FALSE
  FILE_FORMAT = (FORMAT_NAME = 'txt_fixed_width');


-- 10.3.5  Explore TABLE and EXTERNAL TABLE metadata.

SHOW TABLES;
SHOW EXTERNAL TABLES;


-- 10.3.6  Execute a simple query against the external table.

SELECT * FROM finwire LIMIT 10;

--         Note: There are no results because REFRESH_ON_CREATE = FALSE was
--         specified when the table was created.

-- 10.3.7  Manually refresh the external table metadata.

ALTER EXTERNAL TABLE finwire REFRESH;

--         Refreshing the external table synchronizes the metadata with the
--         current list of data files in the specified stage. This action is
--         required for the metadata to register any existing data files in the
--         named external stage.
--         You should see output similar to the following, showing the tables
--         loaded to the external table.
--         Query Results

-- 10.3.8  Rerun the query.

SELECT * FROM finwire LIMIT 10;

--         The result set is a single variant column. Take a look at the query
--         profile.

-- 10.4.0  Work with External Tables

-- 10.4.1  Create a table with columns.

create or replace external table finwire
    (
      PTS VARCHAR(15) AS SUBSTR($1, 8, 15)
    , REC_TYPE VARCHAR(3) AS SUBSTR($1, 23, 3)
    , COMPANY_NAME VARCHAR(60) AS SUBSTR($1, 26, 60)
    , CIK VARCHAR(10) AS SUBSTR($1, 86, 10)
    , STATUS VARCHAR(4) AS IFF(SUBSTR($1, 23, 3) = 'CMP', SUBSTR($1, 96, 4),SUBSTR($1, 47, 4))
    , INDUSTRY_ID VARCHAR(2) AS SUBSTR($1, 100, 2)
    , SP_RATING VARCHAR(4) AS SUBSTR($1, 102, 4)
    , FOUNDING_DATE VARCHAR(8) AS SUBSTR($1, 106, 8)
    , ADDR_LINE1 VARCHAR(80) AS SUBSTR($1, 114, 80)
    , ADDR_LINE2 VARCHAR(80) AS SUBSTR($1, 194, 80)
    , POSTAL_CODE VARCHAR(12) AS SUBSTR($1, 274, 12)
    , CITY VARCHAR(25) AS SUBSTR($1, 286, 25)
    , STATE_PROVINCE VARCHAR(20) AS SUBSTR($1, 311, 20)
    , COUNTRY VARCHAR(24) AS SUBSTR($1, 331, 24)
    , CEO_NAME VARCHAR(46) AS SUBSTR($1, 355, 46)
    , DESCRIPTION VARCHAR(150) AS SUBSTR($1, 401, 150)
    , YEAR VARCHAR(4) AS SUBSTR($1, 8, 4)
    , QUARTER VARCHAR(1) AS SUBSTR($1, 30, 1)
    , QTR_START_DATE VARCHAR(8) AS SUBSTR($1, 31, 8)
    , POSTING_DATE VARCHAR(8) AS SUBSTR($1, 39, 8)
    , REVENUE VARCHAR(17) AS SUBSTR($1, 47, 17)
    , EARNINGS VARCHAR(17) AS SUBSTR($1, 64, 17)
    , EPS VARCHAR(12) AS SUBSTR($1, 81, 12)
    , DILUTED_EPS VARCHAR(12) AS SUBSTR($1, 93, 12)
    , MARGIN VARCHAR(12) AS SUBSTR($1, 105, 12)
    , INVENTORY VARCHAR(17) AS SUBSTR($1, 117, 17)
    , ASSETS VARCHAR(17) AS SUBSTR($1, 134, 17)
    , LIABILITIES VARCHAR(17) AS SUBSTR($1, 151, 17)
    , SH_OUT VARCHAR(13)AS IFF(SUBSTR($1, 23, 3) = 'FIN', SUBSTR($1, 168, 13), SUBSTR($1, 127, 13))
    , DILUTED_SH_OUT VARCHAR(13) AS SUBSTR($1, 181, 13)
    , CO_NAME_OR_CIK VARCHAR(60) AS IFF(SUBSTR($1, 23, 3) = 'FIN', SUBSTR($1, 194, 10), SUBSTR($1, 168, 10))
    , SYMBOL VARCHAR(15) AS SUBSTR($1, 26, 15)
    , ISSUE_TYPE VARCHAR(6) AS SUBSTR($1, 41, 6)
    , NAME VARCHAR(70) AS SUBSTR($1, 51, 70)
    , EX_ID VARCHAR(6) AS SUBSTR($1, 121, 6)
    , FIRST_TRADE_DATE VARCHAR(8) AS SUBSTR($1, 140, 8)
    , FIRST_TRADE_EXCHG VARCHAR(8) AS SUBSTR($1, 148, 8)
    , DIVIDEND VARCHAR(12) AS SUBSTR($1, 156, 12)
    )
location = @training_db.traininglab.ed_stage/finwire
file_format = (format_name = 'txt_fixed_width');


-- 10.4.2  Refresh the external table.

ALTER EXTERNAL TABLE finwire REFRESH;


-- 10.4.3  Query the revised table.

SELECT * 
   FROM finwire 
   WHERE rec_type = 'CMP' limit 10;
SELECT co_name_or_cik
     , year
     , quarter
     , sum(revenue::number) 
  FROM finwire  
  WHERE rec_type='FIN' and year='1967' and quarter='2' group by 1,2,3;

--         Again, examine the query profile for each query and see how efficient
--         the queries are in taking advantage of partition pruning.

-- 10.4.4  Examine the file metadata.

SELECT year
     , quarter
     , co_name_or_cik
     , metadata$filename 
  FROM finwire 
  WHERE rec_type='FIN' LIMIT 10;


-- 10.5.0  Create an External Table with Partitions Based on the File Name.

-- 10.5.1  Create the external table.

create or replace external table finwire
(
    YEAR  VARCHAR(4) AS SUBSTR(METADATA$FILENAME, 16, 4)
  , QUARTER VARCHAR(1) AS SUBSTR(METADATA$FILENAME, 21, 1)
  , thestring varchar(90) AS  SUBSTR(METADATA$FILENAME, 1, 50) 
  , PTS VARCHAR(15) AS SUBSTR($1, 8, 15)
  , REC_TYPE VARCHAR(3) AS SUBSTR($1, 23, 3)
  , COMPANY_NAME VARCHAR(60) AS SUBSTR($1, 26, 60)
  , CIK VARCHAR(10) AS SUBSTR($1, 86, 10)
  , STATUS VARCHAR(4) AS IFF(SUBSTR($1, 23, 3) = 'CMP', SUBSTR($1, 96, 4),SUBSTR($1, 47, 4))
  , INDUSTRY_ID VARCHAR(2) AS SUBSTR($1, 100, 2)
  , SP_RATING VARCHAR(4) AS SUBSTR($1, 102, 4)
  , FOUNDING_DATE VARCHAR(8) AS SUBSTR($1, 106, 8)
  , ADDR_LINE1 VARCHAR(80) AS SUBSTR($1, 114, 80)
  , ADDR_LINE2 VARCHAR(80) AS SUBSTR($1, 194, 80)
  , POSTAL_CODE VARCHAR(12) AS SUBSTR($1, 274, 12)
  , CITY VARCHAR(25) AS SUBSTR($1, 286, 25)
  , STATE_PROVINCE VARCHAR(20) AS SUBSTR($1, 311, 20)
  , COUNTRY VARCHAR(24) AS SUBSTR($1, 331, 24)
  , CEO_NAME VARCHAR(46) AS SUBSTR($1, 355, 46)
  , DESCRIPTION VARCHAR(150) AS SUBSTR($1, 401, 150)
  , QTR_START_DATE VARCHAR(8) AS SUBSTR($1, 31, 8)
  , POSTING_DATE VARCHAR(8) AS SUBSTR($1, 39, 8)
  , REVENUE VARCHAR(17) AS SUBSTR($1, 47, 17)
  , EARNINGS VARCHAR(17) AS SUBSTR($1, 64, 17)
  , EPS VARCHAR(12) AS SUBSTR($1, 81, 12)
  , DILUTED_EPS VARCHAR(12) AS SUBSTR($1, 93, 12)
  , MARGIN VARCHAR(12) AS SUBSTR($1, 105, 12)
  , INVENTORY VARCHAR(17) AS SUBSTR($1, 117, 17)
  , ASSETS VARCHAR(17) AS SUBSTR($1, 134, 17)
  , LIABILITIES VARCHAR(17) AS SUBSTR($1, 151, 17)
  , SH_OUT VARCHAR(13)AS IFF(SUBSTR($1, 23, 3) = 'FIN', SUBSTR($1, 168, 13), SUBSTR($1, 127, 13))
  , DILUTED_SH_OUT VARCHAR(13) AS SUBSTR($1, 181, 13)
  , CO_NAME_OR_CIK VARCHAR(60) AS IFF(SUBSTR($1, 23, 3) = 'FIN', SUBSTR($1, 194, 10), SUBSTR($1, 168, 10))
  , SYMBOL VARCHAR(15) AS SUBSTR($1, 26, 15)
  , ISSUE_TYPE VARCHAR(6) AS SUBSTR($1, 41, 6)
  , NAME VARCHAR(70) AS SUBSTR($1, 51, 70)
  , EX_ID VARCHAR(6) AS SUBSTR($1, 121, 6)
  , FIRST_TRADE_DATE VARCHAR(8) AS SUBSTR($1, 140, 8)
  , FIRST_TRADE_EXCHG VARCHAR(8) AS SUBSTR($1, 148, 8)
  , DIVIDEND VARCHAR(12) AS SUBSTR($1, 156, 12)
)
partition by (year,quarter)
location = @training_db.traininglab.ed_stage/finwire
file_format = (format_name = 'txt_fixed_width');


-- 10.5.2  Refresh the table.

ALTER EXTERNAL TABLE finwire REFRESH;


-- 10.5.3  Execute some queries and examine their profiles

select co_name_or_cik
     , year
     , quarter
     , sum(revenue::number) 
  from finwire 
  where rec_type='FIN' and year='1967' and quarter='3' group by 1,2,3;
  
select co_name_or_cik
     , year
     , quarter
     , sum(revenue::number) 
  from finwire 
  where rec_type='FIN' and year='1989' and quarter='3' group by 1,2,3;

