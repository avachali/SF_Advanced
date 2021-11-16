
-- 8.0.0   High-Performance Functions
--         This lab will take approximately 25 minutes to complete.
--         The purpose of this exercise is to test Snowflake’s high-performing
--         functions and data sampling.

-- 8.1.0   Explore the Sample Function

-- 8.1.1   Open a new worksheet and set the context as follows:

USE ROLE training_role;
USE WAREHOUSE CHIPMUNK_QUERY_WH;
USE DATABASE snowflake_sample_data;
USE SCHEMA TPCH_SF10;


-- 8.1.2   Select a sample from the lineitem table (each row has a 20% chance of
--         being included):

SELECT * FROM lineitem SAMPLE (20);

--         By default, the SAMPLE function uses the row method - which means
--         every row is examined to determine if it is in the sample set, or
--         not. The row method is very accurate, but is also slower than the
--         block method (which evaluates a block of rows at a time).

-- 8.1.3   Select a sample from the lineitem table using the block method:

SELECT * FROM lineitem SAMPLE BLOCK (20);

--         Compare the speed of the row and block methods.

-- 8.1.4   Sample with a seed option to produce deterministic behavior:
--         Run with the seed several times to verify that the samples are the
--         same.

SELECT * FROM lineitem SAMPLE (10) SEED (4444);

SELECT * FROM lineitem SAMPLE (10) SEED (4444);

SELECT * FROM lineitem SAMPLE (10) SEED (4444);


-- 8.1.5   JOIN samples from two different tables:

SELECT * FROM lineitem SAMPLE (5) JOIN orders SAMPLE (10)
      ON (l_orderkey = o_orderkey);


-- 8.1.6   Use sampling to create training (90%) and test (remaining 10%)
--         datasets for data science modeling:

CREATE TEMPORARY TABLE CHIPMUNK_db.public.lineitem_training 
  AS SELECT * FROM  lineitem SAMPLE (90);

CREATE TEMPORARY TABLE CHIPMUNK_db.public.lineitem_testing
  AS SELECT * FROM lineitem
      WHERE l_orderkey NOT IN (SELECT l_orderkey
  FROM CHIPMUNK_db.public.lineitem_training);


-- 8.2.0   Use the Hyperloglog Approximate Count Function
--         Snowflake uses HyperLogLog to estimate the approximate number of
--         distinct values in a data set. HyperLogLog is a state-of-the-art
--         cardinality estimation algorithm, capable of estimating distinct
--         cardinalities of trillions of rows with an average relative error of
--         a few percent.

-- 8.2.1   Resize your virtual warehouse for this test:

ALTER WAREHOUSE CHIPMUNK_query_wh
  SET WAREHOUSE_SIZE = 'XLARGE';

ALTER SESSION SET USE_CACHED_RESULT = FALSE;
-- Disable reuse cached query results

ALTER WAREHOUSE CHIPMUNK_query_wh SUSPEND;
-- Ignore error if the the warehouse is already in the suspended state

ALTER WAREHOUSE CHIPMUNK_query_wh RESUME;


-- 8.2.2   Determine an approximate count of distinct values with the HLL
--         function:

SELECT HLL(L_ORDERKEY) FROM lineitem;


-- 8.2.3   View the query profile to see how long it took.
--         From looking at the query profile, the bulk of the execution time at
--         first run is dominated by the table scan of the large data set, which
--         obscures the speed of the HLL function. Note that 0% of the data was
--         scanned from the data cache.
--         HLL Query Profile

-- 8.2.4   Execute the same query again and record the execution time:

SELECT HLL(L_ORDERKEY) FROM lineitem;

--         Since the the session has been altered not to reuse the query result
--         cache, this query will not use the query result cache. However, this
--         will use the data cache that was built up from the first run on the
--         function. This eliminates the time required to scan the data, and
--         gives a truer estimate of the speed of the HLL function.
--         Confirm that the query profile shows a very high Percentage scanned
--         from cache (it should be over 98%).
--         HLL Query Profile

-- 8.2.5   Execute the regular count distinct version of the query:

SELECT COUNT(DISTINCT l_orderkey) FROM lineitem;


-- 8.2.6   Review the query profile to verify execution time, and cache used.
--         The query profile should again show that a high percentage of the
--         data was scanned from cache. Since both functions used primarily data
--         cache, it eliminates the time associated with the table scan.
--         Compare the execution time of the two queries and note the HLL
--         approximate count version is much faster than the regular count
--         version.
--         HLL Query Profile

-- 8.3.0   Use the Percentile Estimation Function

-- 8.3.1   Run a query using the SQL MEDIAN function:
--         If the select statement runs more than 5 minutes, cancel it.

USE SCHEMA snowflake_sample_data.tpcds_sf10tcl;

SELECT MEDIAN(ss_sales_price), ss_store_sk
FROM store_sales
GROUP BY ss_store_sk;

--         Review the time it took to complete as well as the figure.

-- 8.3.2   Run the approximate percentile query to find the approximate 50th
--         percentile.

SELECT approx_percentile(ss_sales_price, 0.5), ss_store_sk
FROM store_sales
GROUP BY ss_store_sk;


-- 8.3.3   Review the time it took to complete.
--         Notice that the approximate percentile function was faster, and it
--         returned a number almost identical to that of MEDIAN, thus making it
--         more efficient.

-- 8.4.0   Use Collations
--         Text strings in Snowflake are stored using the UTF-8 character set
--         and, by default, strings are compared according to the Unicode codes
--         that represent the characters in the string. Comparing strings based
--         on their UTF-8 character representations might not provide the
--         desired result.
--         In this task you will use collations to modify how string values are
--         sorted and compared.

-- 8.4.1   Set your context:

CREATE DATABASE IF NOT EXISTS CHIPMUNK_db;
USE DATABASE CHIPMUNK_db;
CREATE SCHEMA IF NOT EXISTS collation;
USE SCHEMA collation;
CREATE OR REPLACE WAREHOUSE CHIPMUNK_QUERY_WH;
USE WAREHOUSE CHIPMUNK_QUERY_WH;
ALTER WAREHOUSE CHIPMUNK_QUERY_WH SET WAREHOUSE_SIZE = 'XSMALL';


-- 8.4.2   Create a source table named collation_tbl:

CREATE OR REPLACE TABLE collation_tbl(
    DEF_COLLATION STRING
  , CASE_INSENSITIVE STRING COLLATE 'EN-CI'
);

--         The first column uses the default collation (UTF-8), and the second
--         column uses English, case-insensitive collation rules.

-- 8.4.3   Insert some values into collation_tbl:

INSERT INTO collation_tbl(def_collation, case_insensitive)
  VALUES ('abc','AbC'),
         ('bcd','bcD'),
         ('ggg','GGG'),
         ('AAA','AAA'),
         ('aaa','aaa'),
         ('zzz','AAA'),
         ('ZZZ','zzz');


-- 8.4.4   Select data without a sort:

SELECT * FROM collation_tbl;

--         The data is returned in the order it was ingested.

-- 8.4.5   Select data sorted by the def_collation column:

SELECT * FROM collation_tbl ORDER BY def_collation;

--         The values in the def_collation column will have all strings that
--         start with upper-case letters ordered first.

-- 8.4.6   Select data sorted by case_insensitive column:

SELECT * FROM collation_tbl ORDER BY case_insensitive;

--         The values in the case_insensitive column are ordered without regard
--         to the case of the first letter.

-- 8.4.7   Change the sort order of the def_collation column using English case-
--         insensitive collation:

SELECT * FROM collation_tbl ORDER BY COLLATE(def_collation,'en-ci');


-- 8.4.8   Change the sort order of the def_collation column using English
--         collation:

SELECT * FROM collation_tbl ORDER BY COLLATE(def_collation,'en');

--         Compare how the English collation is sorted in contrast to the
--         English, case-insensitive collation.

-- 8.5.0   Query Hierarchical Data
--         A common use case in SQL is creating a hierarchical structure out of
--         a relational table. In the presentation you saw an example with
--         employees, managers, and their reporting structure. This lab presents
--         that example. Another use case is in banking where a bank wants to
--         create a hierarchical structure with portfolio managers and their
--         customer’s investment portfolios.

-- 8.5.1   Set your context:

USE ROLE training_role;
CREATE SCHEMA CHIPMUNK_db.hierarchy;
USE SCHEMA CHIPMUNK_db.hierarchy;
USE WAREHOUSE CHIPMUNK_query_wh;


-- 8.5.2   Create a table to hold employee data:

CREATE OR REPLACE TABLE employees (
      title STRING
    , employee_Id INTEGER
    , manager_Id INTEGER
);


-- 8.5.3   Load data into the employees table:

INSERT INTO employees
VALUES ('CEO', 1, NULL)
      ,('CFO', 2, 1)
      ,('IT MANAGER', 3, 1)
      ,('IT ANALYST', 4, 3);

SELECT * FROM employees;


-- 8.5.4   Query the hierarchical data using recursive WITH:

WITH hierarchy AS (
  SELECT
   *, 1 level, '/' || title chain
  FROM employees
  WHERE employee_id = 1
 UNION ALL
  SELECT
    e.*, h.level + 1,
    chain || '/' || e.title
  FROM
   employees e JOIN hierarchy h
   ON (e.manager_Id = h.employee_Id)
)

SELECT * FROM hierarchy;


-- 8.5.5   Query the hierarchical data using CONNECT BY:

SELECT
  *, level, sys_connect_by_path(title, '/') chain
FROM employees
CONNECT BY prior employee_Id = manager_Id
START WITH employee_Id = 1;

