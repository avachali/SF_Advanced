
-- 12.0.0  Materialized View Use Cases
--         This lab will take approximately 15 minutes to complete.

-- 12.1.0  Cluster a Table Using a Timestamp Column

-- 12.1.1  Set your context.

USE ROLE TRAINING_ROLE;
USE database CHIPMUNK_db;
USE WAREHOUSE CHIPMUNK_QUERY_WH;
ALTER SESSION SET USE_CACHED_RESULT=TRUE;


-- 12.1.2  Create a table using cloning.

CREATE TABLE CHIPMUNK_db.PUBLIC.weblog CLONE TRAINING_DB.TRAININGLAB.WEBLOG;


-- 12.1.3  Check the clustering quality of the CREATE_MS and METRIC9 columns.

SELECT SYSTEM$CLUSTERING_INFORMATION( 'WEBLOG' , '(CREATE_MS)');

SELECT SYSTEM$CLUSTERING_INFORMATION( 'WEBLOG' , '(METRIC9)');

--         Which column is more effectively clustered?

-- 12.1.4  Run a query with a search filter using the column CREATE_MS.

SELECT COUNT(*) CNT
     , AVG(TIME_ON_LOAD_MS) AVG_TIME_ON_LOAD
FROM WEBLOG
WHERE CREATE_MS BETWEEN 1000000000 AND 1000001000;


-- 12.1.5  View the query profile to check micro-partition pruning.
--         In this case, the micro-partition pruning is very good.
--         TableScan[2] Node Details

-- 12.1.6  Check the clustering quality of the column PAGE_ID.
--         Based on the column name - would you expect it to be well-clustered,
--         or poorly clustered?

SELECT SYSTEM$CLUSTERING_INFORMATION( 'WEBLOG' , '(PAGE_ID)' );

--         Were you right?

-- 12.1.7  Run a query that filters in the PAGE_ID.
--         Since PAGE_ID is not well-clustered, you would expect the micro-
--         partition pruning to be low.

SELECT COUNT(*) CNT
     , AVG(TIME_ON_LOAD_MS) AVG_TIME_ON_LOAD
FROM WEBLOG
WHERE PAGE_ID=100000; 


-- 12.1.8  Check the micro-partition pruning in the query profile.
--         TableScan[2] Node Details
--         Note that, as expected, the micro-partition pruning is very low.
--         Record the execution time, and the micro-partition pruning.

-- 12.2.0  Cluster a Table to Improve Query Performance
--         You would like both queries - the one filtered by PAGE_ID and the one
--         filtered by CREATE_MS - to run fast. But running both queries with
--         equally good performance requires using a second copy of the data
--         that’s organized differently. You can do this easily with
--         materialized views.

-- 12.2.1  Create a materialized view clustered by PAGE_ID.
--         Creating the materialized view with a clustering key causes Snowflake
--         to reorganize the data during the initial creation of the
--         materialized view. Here you will increase the virtual warehouse size
--         so the re-clustering will go faster - but the operation will still
--         take up to 10 minutes. This would be a good time to stretch your legs
--         or refill your coffee.

ALTER WAREHOUSE CHIPMUNK_query_wh set warehouse_size=XLARGE;

CREATE OR REPLACE MATERIALIZED VIEW MV_TIME_ON_LOAD
    (CREATE_MS,
    PAGE_ID,
    TIME_ON_LOAD_MS)
    CLUSTER BY (PAGE_ID)
AS
SELECT
    CREATE_MS,
    PAGE_ID,
    TIME_ON_LOAD_MS
FROM WEBLOG;


-- 12.2.2  Check clustering efficiency on the PAGE_ID column of the materialized
--         view.

SELECT SYSTEM$CLUSTERING_INFORMATION ( 'MV_TIME_ON_LOAD' , '(PAGE_ID)' );

--         After the clustering, the average_depth should be around 2 or 3. This
--         is quite an improvement.

-- 12.2.3  Run the query filtered on PAGE_ID against the materialize view.
--         For a proper performance comparison, set the warehouse size back to
--         what it was the first time the query ran.

ALTER WAREHOUSE CHIPMUNK_query_wh set warehouse_size=LARGE;
 
SELECT COUNT(*), AVG(TIME_ON_LOAD_MS) AVG_TIME_ON_LOAD
FROM MV_TIME_ON_LOAD
WHERE PAGE_ID=100000;

--         This example illustrates a substantial improvement in terms of query
--         performance.

-- 12.2.4  Check micro-partition pruning in the query profile.
--         With the materialized view, only one micro-partition was scanned.
--         There was a significant increase in performance as a result.

-- 12.2.5  SHOW materialized views on the WEBLOG table.

SHOW MATERIALIZED VIEWS ON weblog;


-- 12.3.0  Explore Automatic Transparent Rewrite on Materialized Views
--         The Snowflake query optimizer can exploit materialized views to
--         automatically rewrite/reroute queries made against the source table,
--         to the materialized view.

-- 12.3.1  Use EXPLAIN to see if a command will use a source table or a
--         materialized view.
--         Use explain to check if a query against the original source table
--         will use a materialized view for query performance

EXPLAIN
SELECT COUNT(*) CNT
     , AVG(TIME_ON_LOAD_MS) AVG_TIME_ON_LOAD
FROM WEBLOG
WHERE PAGE_ID=100000; 

--         Note that even though the query was against the WEBLOG table, the
--         EXPLAIN plan shows that the materialized view will be scanned.

-- 12.3.2  Run the query.

ALTER SESSION SET USE_CACHED_RESULT = FALSE;

SELECT COUNT(*) CNT
     , AVG(TIME_ON_LOAD_MS) AVG_TIME_ON_LOAD
FROM WEBLOG
WHERE PAGE_ID=100000; 


-- 12.3.3  Check the query profile.
--         Even though the query was against the WEBLOG table, the materialized
--         view was scanned instead.

-- 12.4.0  Materialized Views on External Tables

-- 12.4.1  Create a file format for an external table.

create or replace file format txt_fixed_width 
  TYPE = CSV
  COMPRESSION = 'AUTO' 
  FIELD_DELIMITER = NONE
  RECORD_DELIMITER = '\\n' 
  SKIP_HEADER = 0 
  TRIM_SPACE = FALSE 
  ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE 
  NULL_IF = ('\\N');


-- 12.4.2  Create an external table with partitions based on the filename.

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


-- 12.4.3  Refresh the external table.

ALTER EXTERNAL TABLE finwire REFRESH;


-- 12.4.4  Execute some queries and examine their profiles.

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


-- 12.4.5  Create a materialized view that filters on REC_TYPE = CMP.

CREATE OR REPLACE MATERIALIZED VIEW FINWIRE_CMP AS 
 SELECT TO_TIMESTAMP_NTZ(PTS,'YYYYMMDD-HH24MISS') AS PTS
      , REC_TYPE
      , COMPANY_NAME
      , CIK, STATUS
      , INDUSTRY_ID
      , SP_RATING
      , TRY_TO_DATE(FOUNDING_DATE) AS FOUNDING_DATE
      , ADDR_LINE1
      , ADDR_LINE2
      , POSTAL_CODE
      , CITY
      , STATE_PROVINCE
      , COUNTRY
      , CEO_NAME
      , DESCRIPTION
    FROM FINWIRE WHERE REC_TYPE = 'CMP';


-- 12.4.6  Create a materialized view that filters on REC_TYPE = FIN.

CREATE OR REPLACE MATERIALIZED VIEW FINWIRE_FIN AS 
  SELECT TO_TIMESTAMP_NTZ(PTS,'YYYYMMDD-HH24MISS') AS PTS, 
  REC_TYPE, TO_NUMBER(YEAR,4,0) AS YEAR, 
  TO_NUMBER(QUARTER,1,0) AS QUARTER,
  TO_DATE(QTR_START_DATE, 'YYYYMMDD') AS QTR_START_DATE, 
  TO_DATE(POSTING_DATE, 'YYYYMMDD') AS POSTING_DATE, 
  TO_NUMBER(REVENUE,15,2) AS REVENUE,
  TO_NUMBER(EARNINGS,15,2) AS EARNINGS, 
  TO_NUMBER(EPS,10,2) AS EPS, 
  TO_NUMBER(DILUTED_EPS,10,2) AS DILUTED_EPS, 
  TO_NUMBER(MARGIN,10,2) AS MARGIN,
  TO_NUMBER(INVENTORY,15,2) AS INVENTORY, 
  TO_NUMBER(ASSETS,15,2) AS ASSETS, 
  TO_NUMBER(LIABILITIES,15,2) AS LIABILITIES,
  TO_NUMBER(SH_OUT,13,0) AS SH_OUT, 
  TO_NUMBER(DILUTED_SH_OUT,13,0) AS DILUTED_SH_OUT, 
  CO_NAME_OR_CIK
FROM FINWIRE WHERE REC_TYPE = 'FIN';


-- 12.4.7  Create a materialized view that filters on REC_TYPE = SEC
--         .

CREATE OR REPLACE MATERIALIZED VIEW FINWIRE_SEC AS SELECT
TO_TIMESTAMP_NTZ(PTS,'YYYYMMDD-HH24MISS') AS PTS, REC_TYPE, SYMBOL, ISSUE_TYPE, STATUS, NAME, EX_ID,
TO_NUMBER(SH_OUT,13,0) AS SH_OUT, TO_DATE(FIRST_TRADE_DATE,'YYYYMMDD') AS FIRST_TRADE_DATE,
TO_DATE(FIRST_TRADE_EXCHG,'YYYYMMDD') AS FIRST_TRADE_EXCHG, TO_NUMBER(DIVIDEND,10,2) AS DIVIDEND, CO_NAME_OR_CIK
FROM FINWIRE WHERE REC_TYPE = 'SEC';


-- 12.4.8  SHOW the materialized views.

SHOW MATERIALIZED VIEWS;


-- 12.4.9  Run an query on the FINWIRE_FIN materialized view.

select co_name_or_cik, year, quarter, sum(revenue) 
  from finwire_fin 
  where rec_type='FIN' and year=1967 and quarter=2 group by 1,2,3;


-- 12.4.10 View the query profile.
