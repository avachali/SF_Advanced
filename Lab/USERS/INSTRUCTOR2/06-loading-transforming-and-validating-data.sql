
-- 6.0.0   Loading, Transforming and Validating Data
--         This lab will take approximately 45 minutes to complete.
--         You will learn how to load data into Snowflake using external stages
--         and file formats, and transform data upon load. Also you will use
--         Snowflake’s VALIDATION_MODE option on a COPY statement to demonstrate
--         Snowflake’s pre-load error detection mechanism.

-- 6.1.0   Load Structured Data
--         This exercise will load the region.tbl file into a REGION table in
--         your Database. The region.tbl file is pipe (|) delimited. It has no
--         header and contains the following five (5) rows:
--         Note that in the region.tbl file there is a delimiter at the end of
--         every line, which by default is interpreted as an additional column
--         by the COPY INTO statement.
--         The files required for this lab are in an external stage.

-- 6.1.1   Navigate to Worksheets and create a new worksheet. Name it Data
--         Movement.

-- 6.1.2   Set the Worksheet contexts as follows:

USE ROLE TRAINING_ROLE;
CREATE WAREHOUSE IF NOT EXISTS INSTRUCTOR2_LOAD_WH
   WAREHOUSE_SIZE=XSmall
   INITIALLY_SUSPENDED=True
   AUTO_SUSPEND=300;
USE WAREHOUSE INSTRUCTOR2_LOAD_WH;
CREATE DATABASE IF NOT EXISTS INSTRUCTOR2_DB;
USE DATABASE INSTRUCTOR2_DB;
USE SCHEMA PUBLIC;


-- 6.1.3   Create the staging tables by running the following statements

CREATE OR REPLACE TABLE REGION (
       R_REGIONKEY NUMBER(38,0) NOT NULL,
       R_NAME      VARCHAR(25)  NOT NULL,
       R_COMMENT   VARCHAR(152)
);
CREATE OR REPLACE TABLE NATION (
       N_NATIONKEY NUMBER(38,0) NOT NULL,
       N_NAME      VARCHAR(25)  NOT NULL,
       N_REGIONKEY NUMBER(38,0) NOT NULL,
       N_COMMENT   VARCHAR(152)
);
CREATE OR REPLACE TABLE SUPPLIER (
       S_SUPPKEY   NUMBER(38,0) NOT NULL,
       S_NAME      VARCHAR(25)  NOT NULL,
       S_ADDRESS   VARCHAR(40)  NOT NULL,
       S_NATIONKEY NUMBER(38,0) NOT NULL,
       S_PHONE     VARCHAR(15)  NOT NULL,
       S_ACCTBAL   NUMBER(12,2) NOT NULL,
       S_COMMENT   VARCHAR(101)
);
CREATE OR REPLACE TABLE PART (
       P_PARTKEY     NUMBER(38,0) NOT NULL,
       P_NAME        VARCHAR(55)  NOT NULL,
       P_MFGR        VARCHAR(25)  NOT NULL,
       P_BRAND       VARCHAR(10)  NOT NULL,
       P_TYPE        VARCHAR(25)  NOT NULL,
       P_SIZE        NUMBER(38,0) NOT NULL,
       P_CONTAINER   VARCHAR(10)  NOT NULL,
       P_RETAILPRICE NUMBER(12,2) NOT NULL,
       P_COMMENT     VARCHAR(23)
);
CREATE OR REPLACE TABLE PARTSUPP (
       PS_PARTKEY    NUMBER(38,0) NOT NULL,
       PS_SUPPKEY    NUMBER(38,0) NOT NULL,
       PS_AVAILQTY   NUMBER(38,0) NOT NULL,
       PS_SUPPLYCOST NUMBER(12,2) NOT NULL,
       PS_COMMENT    VARCHAR(199)
);
CREATE OR REPLACE TABLE CUSTOMER (   
       C_CUSTKEY    NUMBER(38,0) NOT NULL,   
       C_NAME       VARCHAR(25)  NOT NULL,   
       C_ADDRESS    VARCHAR(40)  NOT NULL,   
       C_NATIONKEY  NUMBER(38,0) NOT NULL,   
       C_PHONE      VARCHAR(15)  NOT NULL,   
       C_ACCTBAL    NUMBER(12,2) NOT NULL,   
       C_MKTSEGMENT VARCHAR(10),   
       C_COMMENT    VARCHAR(117)  
);
CREATE OR REPLACE TABLE ORDERS (
       O_ORDERKEY      NUMBER(38,0) NOT NULL,
       O_CUSTKEY       NUMBER(38,0) NOT NULL,
       O_ORDERSTATUS   VARCHAR(1)   NOT NULL,
       O_TOTALPRICE    NUMBER(12,2) NOT NULL,
       O_ORDERDATE     DATE         NOT NULL,
       O_ORDERPRIORITY VARCHAR(15)  NOT NULL,
       O_CLERK         VARCHAR(15)  NOT NULL,
       O_SHIPPRIORITY  NUMBER(38,0) NOT NULL,
       O_COMMENT       VARCHAR(79)  NOT NULL
);
CREATE OR REPLACE TABLE LINEITEM (
       L_ORDERKEY      NUMBER(38,0) NOT NULL,
       L_PARTKEY       NUMBER(38,0) NOT NULL,
       L_SUPPKEY       NUMBER(38,0) NOT NULL,
       L_LINENUMBER    NUMBER(38,0) NOT NULL,
       L_QUANTITY      NUMBER(12,2) NOT NULL,
       L_EXTENDEDPRICE NUMBER(12,2) NOT NULL,
       L_DISCOUNT      NUMBER(12,2) NOT NULL,
       L_TAX           NUMBER(12,2) NOT NULL,
       L_RETURNFLAG    VARCHAR(1)   NOT NULL,
       L_LINESTATUS    VARCHAR(1)   NOT NULL,
       L_SHIPDATE      DATE         NOT NULL,
       L_COMMITDATE    DATE         NOT NULL,
       L_RECEIPTDATE   DATE         NOT NULL,
       L_SHIPINSTRUCT  VARCHAR(25)  NOT NULL,
       L_SHIPMODE      VARCHAR(10)  NOT NULL,
       L_COMMENT       VARCHAR(44)  NOT NULL
);
CREATE OR REPLACE TABLE COUNTRYGEO (
       CG_NATIONKEY NUMBER(38,0),
       CG_CAPITAL   VARCHAR(100),
       CG_LAT       NUMBER(20,10),
       CG_LON       NUMBER(20,10),
       CG_ALTITUDE  NUMBER(38,0)
);


-- 6.1.4   Find the region.tbl file in the external stage with list and a regex
--         pattern.

LIST @TRAINING_DB.TRAININGLAB.ED_STAGE/load/lab_files/ pattern='.*region.*';


-- 6.1.5   Load the data from the external stage to the REGION Table using the
--         MYPIPEFORMAT file format.

DESCRIBE FILE FORMAT TRAINING_DB.TRAININGLAB.MYPIPEFORMAT;

COPY INTO REGION 
  FROM @TRAINING_DB.TRAININGLAB.ED_STAGE/load/lab_files/
  FILES = ( 'region.tbl' )
  FILE_FORMAT = ( FORMAT_NAME = TRAINING_DB.TRAININGLAB.MYPIPEFORMAT );

--         The file formats required for the lab steps have been created and are
--         all located in the TRAINING_DB.TRAININGLAB schema.

-- 6.1.6   Select and review the data in the REGION Table:

SELECT * FROM REGION;


-- 6.1.7   Preview the REGION table in the WebUI using the sidebar.
--         First, click on your database INSTRUCTOR2_DB in the navigator. Locate and
--         click on the PUBLIC schema. In the list of tables, find the REGION
--         table. Click the series of three ellipses to the right and select
--         Preview Data:
--         Select Preview Data

-- 6.2.0   Loading Data and File Sizes
--         When loading data, file size matters. Snowflake recommends using
--         files sizes of 100MB to 250MB of compressed data for both bulk
--         loading using COPY and for streaming using Snowpipe. Both MACOS and
--         Linux support a file splitting utility.

-- 6.2.1   Set Context:

USE ROLE training_role;
USE SCHEMA INSTRUCTOR2_db.public;
USE WAREHOUSE INSTRUCTOR2_load_wh;


-- 6.2.2   Create a named stage:

CREATE  OR REPLACE TEMPORARY STAGE INSTRUCTOR2_STAGE;


-- 6.2.3   Change your warehouse size to small:

ALTER WAREHOUSE INSTRUCTOR2_load_wh SET WAREHOUSE_SIZE = SMALL;


-- 6.2.4   Download a file from the Citibike table to the stage you created:

COPY INTO @INSTRUCTOR2_STAGE/citibike/singlefile/citibike.tbl
  FROM citibike.schema1.trips
  FILE_FORMAT=(FORMAT_NAME=training_db.traininglab.MYPIPEFORMAT)
  SINGLE=TRUE
  MAX_FILE_SIZE=5368709120;

--         You will receive an error message - this is because the file is too
--         large to be downloaded as a single file.

-- 6.2.5   Confirm the error:

ls @INSTRUCTOR2_STAGE/citibike/singlefile;


-- 6.2.6   Re-run the command to limit the amount of data unloaded:

COPY INTO @INSTRUCTOR2_STAGE/citibike/singlefile/citibike.tbl
  FROM (SELECT * FROM citibike.schema1.trips LIMIT 20000000)
  FILE_FORMAT=(FORMAT_NAME=training_db.traininglab.MYPIPEFORMAT)
  SINGLE=TRUE
  MAX_FILE_SIZE=5368709120;


-- 6.2.7   List the file on the stage:

ls @INSTRUCTOR2_STAGE/citibike/singlefile;


-- 6.2.8   Now resize your warehouse to LARGE and unload the data to the stage
--         without using the SINGLE option:

ALTER WAREHOUSE INSTRUCTOR2_load_wh SET WAREHOUSE_SIZE = LARGE;

COPY INTO @INSTRUCTOR2_STAGE/citibike/multiplefiles/citibike_
  FROM (SELECT * FROM citibike.schema1.trips)
  FILE_FORMAT=(FORMAT_NAME=training_db.traininglab.MYPIPEFORMAT)
  SINGLE=FALSE;

ls @INSTRUCTOR2_STAGE/citibike/multiplefiles;

ALTER WAREHOUSE INSTRUCTOR2_load_wh set warehouse_size = SMALL;


-- 6.3.0   Load Semi-Structured Data
--         This exercise will load tables from text files that are in an
--         external stage. You will load text files from an external stage using
--         the Web UI.

-- 6.3.1   Navigate to Databases in the top ribbon.

-- 6.3.2   Select the TRAINING_DB Database.

-- 6.3.3   Navigate to the Stages area.

-- 6.3.4   Confirm you see the ED_STAGE Stage in the TRAINING_DB.TRAININGLAB
--         Schema.
--         Take note of its location. This external stage points to an AWS S3
--         bucket.

-- 6.3.5   Navigate to Worksheets.

-- 6.3.6   List the files in the TRAINING_DB.TRAININGLAB.ED_STAGE/coredata
--         stage:

ls @TRAINING_DB.TRAININGLAB.ED_STAGE/coredata/TCPH/TCPH_SF100;

--Set a variable to hold the query id of the ls command
SET sf100 = LAST_QUERY_ID();

--         There are many files to load for some of the larger tables. For these
--         larger tables, you will get better performance using more cores (a
--         larger virtual warehouse). Therefore, for this load exercise you will
--         alter the size of the virtual warehouse to an X-Large. In a real-
--         world application, you would create a load cluster sized to the
--         number of files that you plan to load.

-- 6.3.7   Alter the INSTRUCTOR2_load_wh and increase its size:

ALTER WAREHOUSE INSTRUCTOR2_LOAD_WH SET WAREHOUSE_SIZE='X-LARGE';


-- 6.3.8   Query all of the unique directories that follow TCP_SF100 in the
--         stage.

SELECT DISTINCT REGEXP_SUBSTR(  -- https://docs.snowflake.net/manuals/sql-reference/functions/regexp_substr.html
                     "name"     -- column name that contains the string
                     ,'.*TCPH_SF100\/(.*)\/'  --regular expression string, to see how it works you can visit https://regex101.com/r/r6pX3O/1
                     ,1         --start from the beginning of the string
                     ,1         --find the first occurrance match
                     ,'e'       --extract the sub-matches
                     ,1         --return the first sub match
                    ) AS  DIRECTORY_NAMES
   FROM TABLE(RESULT_SCAN($sf100));


-- 6.3.9   Load data into the CUSTOMER table.

COPY INTO INSTRUCTOR2_DB.PUBLIC.CUSTOMER 
  FROM @TRAINING_DB.TRAININGLAB.ED_STAGE
  PATTERN='.*/CUSTOMER/.*tbl'
  FILE_FORMAT = (FORMAT_NAME = TRAINING_DB.TRAININGLAB.MYPIPEFORMAT)
  ON_ERROR = 'CONTINUE';


-- 6.3.10  Load the remaining tables.
--         Using the above COPY INTO command as a template, run additional COPY
--         INTO statements to load each table in your INSTRUCTOR2_DB.PUBLIC schema.
--         Remaining tables:

-- 6.3.11  Count the number of rows from the newly populated tables:

SELECT TABLE_NAME, ROW_COUNT 
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = CURRENT_SCHEMA();


-- 6.3.12  BONUS Try re-writing the above query to also get the number of files
--         in each directory.

-- 6.4.0   Load Semi-Structured Parquet Data
--         This exercise will load a Parquet data file using different methods.

-- 6.4.1   Empty the REGION Table in the PUBLIC schema of your INSTRUCTOR2_DB:

TRUNCATE TABLE REGION;


-- 6.4.2   Confirm that the region.parquet file is in the External Stage:

LIST @TRAINING_DB.TRAININGLAB.ED_STAGE/load/lab_files 
    PATTERN = '.*region.*';

LIST @TRAINING_DB.TRAININGLAB.ED_STAGE/load/lab_files 
    PATTERN = '.*region.*parquet$';


-- 6.4.3   Create the file format for the Parquet file in the current schema:

CREATE OR REPLACE FILE FORMAT MYPARQUETFORMAT
    TYPE = PARQUET
    COMPRESSION = NONE;

SELECT *
FROM @TRAINING_DB.TRAININGLAB.ED_STAGE/load/lab_files/region.parquet
(FILE_FORMAT => MYPARQUETFORMAT);


-- 6.4.4   Query the data in the region.parquet file from the external stage:

SELECT 
      $1,
      $1:_COL_0::number,
      $1:_COL_1::varchar,
      $1:_COL_2::varchar
 FROM @TRAINING_DB.TRAININGLAB.ED_STAGE/load/lab_files/region.parquet
 (FILE_FORMAT => MYPARQUETFORMAT);


-- 6.4.5   Reload the REGION Table from the region.parquet file:

COPY INTO REGION
FROM (
      SELECT $1:_COL_0::number,
             $1:_COL_1::varchar,
             $1:_COL_2::varchar
      FROM @TRAINING_DB.TRAININGLAB.ED_STAGE/load/lab_files/
     )
FILES = ('region.parquet')
FILE_FORMAT = (FORMAT_NAME = MYPARQUETFORMAT);


-- 6.4.6   View the data:

SELECT * FROM REGION;


-- 6.5.0   Load Semi-Structured JSON Data
--         This exercise will load a JSON data file.

-- 6.5.1   Confirm that the countrygeo.json file is in the External Stage:

LIST @TRAINING_DB.TRAININGLAB.ED_STAGE PATTERN = '.*countrygeo.*';


-- 6.5.2   Query the file directly from the stage:

SELECT *
FROM @TRAINING_DB.TRAININGLAB.ED_STAGE/load/lab_files/countrygeo.json
(FILE_FORMAT => 'TRAINING_DB.TRAININGLAB.MYJSONFORMAT');


-- 6.5.3   Load the COUNTRYGEO Table from the countrygeo.json file:

CREATE OR REPLACE TABLE COUNTRYGEO (CG_V variant);

COPY INTO COUNTRYGEO (CG_V)
FROM (SELECT $1
      FROM @TRAINING_DB.TRAININGLAB.ED_STAGE/load/lab_files/countrygeo.json )
      FILE_FORMAT = ( FORMAT_NAME = TRAINING_DB.TRAININGLAB.MYJSONFORMAT )
      ON_ERROR = 'continue';


-- 6.5.4   View the data:

SELECT * FROM COUNTRYGEO;


-- 6.6.0   Load Fixed Format Data
--         Loading fixed format data takes advantage of Snowflake’s ability to
--         transform data upon load. The approach is to load the data into a
--         VARCHAR or STRING column and use Snowflake functions to transform the
--         data and load it.

-- 6.6.1   Set Context

USE ROLE TRAINING_ROLE;
USE WAREHOUSE INSTRUCTOR2_LOAD_WH;
USE DATABASE INSTRUCTOR2_DB;
USE SCHEMA PUBLIC;


-- 6.6.2   Create the target NATION table from TCPH data.

CREATE TABLE nation_tbl LIKE training_db.traininglab.nation;


-- 6.6.3   Validate that the source file exists on the stage.

LS @training_db.traininglab.ed_stage/coredata/TCPH/FIXFORMAT;


-- 6.6.4   Create a File Format object.

CREATE FILE FORMAT INSTRUCTOR2_FIXED TYPE = 'CSV' 
                   COMPRESSION = 'AUTO' 
                   FIELD_DELIMITER = 'NONE' 
                   RECORD_DELIMITER = '\n' 
                   FIELD_OPTIONALLY_ENCLOSED_BY = 'NONE' 
                   ESCAPE = 'NONE' ;


-- 6.6.5   Use the copy statement to load the data from Stage

COPY INTO nation_tbl
  FROM (SELECT CAST(SUBSTR($1,1,2) AS NUMBER)
               ,SUBSTR($1,3,12)
               ,CAST(SUBSTR($1,19,1) AS NUMBER)
               ,SUBSTR($1,20,114)
        FROM '@training_db.traininglab.ed_stage/coredata/TCPH/FIXFORMAT'
  )
          FILE_FORMAT=(FORMAT_NAME=INSTRUCTOR2_FIXED);


-- 6.6.6   Query the loaded data.

SELECT * FROM nation_tbl;

--         Sample data:

-- 6.7.0   Detect File Format Problems with VALIDATION_MODE
--         Use Snowflake’s VALIDATION_MODE option on a COPY statement to
--         demonstrate Snowflake’s pre-load error detection mechanism.

-- 6.7.1   Set the following context:

USE ROLE TRAINING_ROLE;
CREATE WAREHOUSE IF NOT EXISTS INSTRUCTOR2_WH;
USE WAREHOUSE INSTRUCTOR2_WH;
CREATE DATABASE IF NOT EXISTS INSTRUCTOR2_DB;
USE INSTRUCTOR2_DB.PUBLIC;


-- 6.7.2   Create (or replace from previous labs) the REGION table to get ready
--         to load an empty table:

CREATE OR REPLACE TABLE REGION (
       R_REGIONKEY NUMBER(38,0) NOT NULL,
       R_NAME      VARCHAR(25)  NOT NULL,
       R_COMMENT   VARCHAR(152)
);


-- 6.7.3   Run a COPY command in validation mode against region_bad_1.tbl, and
--         identify the issue that will cause the load to fail:

COPY INTO region
FROM @training_db.traininglab.ed_stage/load/lab_files/
  FILES = ( 'region_bad_1.tbl' )
  FILE_FORMAT = ( FORMAT_NAME = TRAINING_DB.TRAININGLAB.MYPIPEFORMAT )
  VALIDATION_MODE = RETURN_ALL_ERRORS;

--         To see what happened, expand the ERROR column in the query results.
--         To see the contents of that row, scroll all the way to the right and
--         click the linked text in the REJECTED_RECORD column. You can see in
--         the details that in the first column, which should contain a number,
--         the character is x.

-- 6.7.4   Run a COPY command in validation mode against region_bad_2.tbl and
--         identify the issue that will cause the load to fail:

COPY INTO region FROM @training_db.traininglab.ed_stage/load/lab_files/
  FILES = ('region_bad_2.tbl')
  FILE_FORMAT = (FORMAT_NAME = TRAINING_DB.TRAININGLAB.MYPIPEFORMAT)
  VALIDATION_MODE = RETURN_ALL_ERRORS;

--         Why did it fail? And what does the data look like?

-- 6.8.0   Load Data with ON_ERROR set to CONTINUE
--         This exercise will use Snowflake’s optional ON_ERROR parameter of the
--         COPY command to define the behavior Snowflake should exhibit if an
--         error is encountered when loading a file.

-- 6.8.1   Run a COPY command with the ON_ERROR parameter set to CONTINUE:

COPY INTO REGION FROM @training_db.traininglab.ed_stage/load/lab_files/
  FILES = ( 'region_bad_1.tbl' )
  FILE_FORMAT = ( FORMAT_NAME = TRAINING_DB.TRAININGLAB.MYPIPEFORMAT )
  ON_ERROR = CONTINUE;

--         In the query results pane, you will see that the status is
--         PARTIALLY_LOADED.

-- 6.8.2   Query the data that was loaded, and confirm that all rows were loaded
--         except the row that would not load according to the validation mode.

SELECT * FROM region;

--         You should see that the row with REGIONKEY 1 is missing.

-- 6.8.3   Truncate the REGION table:

TRUNCATE TABLE region;


-- 6.8.4   Run a COPY command with the ON_ERROR parameter set to CONTINUE
--         against region_bad_2.tbl:

COPY INTO region FROM @training_db.traininglab.ed_stage/load/lab_files/
  FILES = ('region_bad_2.tbl')
  FILE_FORMAT = (FORMAT_NAME = TRAINING_DB.TRAININGLAB.MYPIPEFORMAT)
  ON_ERROR = CONTINUE;


-- 6.8.5   View the data that was loaded
--         Confirm that all rows were loaded except the row the VALIDATION_MODE
--         against this file stated would not load (row 2).

SELECT * FROM region;

--         This time, REGIONKEY 2 is missing:

-- 6.9.0   Reload the Region Table with Clean Data

-- 6.9.1   Truncate the REGION table:

TRUNCATE TABLE region;


-- 6.9.2   Validate that you have data in the table stage for your region table:

LIST @training_db.traininglab.ed_stage/load/lab_files/
  PATTERN='.*region.*';


-- 6.9.3   Load clean data to the REGION table:

COPY INTO region FROM @training_db.traininglab.ed_stage/load/lab_files/
  FILES = ('region.tbl.gz')
  FILE_FORMAT = ( FORMAT_NAME = TRAINING_DB.TRAININGLAB.MYGZIPPIPEFORMAT);


-- 6.9.4   View the data that was loaded and confirm that all 5 rows were
--         loaded.

SELECT * FROM region;


-- 6.9.5   Navigate to [Databases] in the top ribbon.

-- 6.9.6   Select INSTRUCTOR2_DB Database and confirm that the REGION table has data
--         loaded.
