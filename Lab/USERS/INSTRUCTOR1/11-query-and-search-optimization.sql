
-- 11.0.0  Query and Search Optimization
--         This lab will take approximately 40 minutes to complete.

-- 11.1.0  Explore Query Performance

-- 11.1.1  Set your context and disable the query result cache.

USE role training_role;
USE warehouse INSTRUCTOR1_QUERY_WH;
USE database training_db;
USE schema TPCH_SF1000;

ALTER SESSION SET USE_CACHED_RESULT = false;


-- 11.1.2  Run a query that filters on columns that are well-clustered:

SELECT
  c_custkey,
  c_name,
  sum(l_extendedprice * (1 - l_discount)) as revenue, c_acctbal,
  n_name,
  c_address,
  c_phone,
  c_comment
FROM customer 
  inner join orders
    on c_custkey = o_custkey 
  inner join lineitem
    on l_orderkey = o_orderkey 
  inner join nation
    on c_nationkey = n_nationkey 
WHERE
  o_orderdate >= to_date('1993-10-01')
    AND o_orderdate < dateadd(month, 3, to_date('1993-10-01')) 
    AND l_returnflag = 'R'
GROUP BY
  c_custkey, 
  c_name, 
  c_acctbal, 
  c_phone, 
  n_name, 
  c_address, 
  c_comment
ORDER BY
  3 desc
LIMIT 20;


-- 11.1.3  View the query profile and note performance metrics.
--         Take note of:
--         How effective was micro-partition pruning for this query? This query
--         was filtered on the O_ORDERDATE column. In general, date columns tend
--         to be fairly well clustered.

-- 11.1.4  Check the clustering quality of the o_orderdate column.

SELECT SYSTEM$CLUSTERING_INFORMATION( 'orders' , '(o_orderdate)' );


-- 11.1.5  Click on the result row and examine statistics.
--         Review the result and notice that the table is fairly well clustered
--         around the o_orderdate dimension.

/*
{
  "cluster_by_keys" : "LINEAR(o_orderdate)",
  "total_partition_count" : 3180,
  "total_constant_partition_count" : 1030,
  "average_overlaps" : 1.195,
  "average_depth" : 1.678,
  "partition_depth_histogram" : {
    "00000" : 0,
    "00001" : 1024,
    "00002" : 2156,
    "00003" : 0,
    "00004" : 0,
    "00005" : 0,
    "00006" : 0,
    "00007" : 0,
    "00008" : 0,
    "00009" : 0,
    "00010" : 0,
    "00011" : 0,
    "00012" : 0,
    "00013" : 0,
    "00014" : 0,
    "00015" : 0,
    "00016" : 0
  }
}
*/


-- 11.1.6  Return to the query profile for the query you ran.

-- 11.1.7  Click on the TableScan [6] operator (at the bottom of the query
--         profile).
--         How many micro-partitions were pruned? Is this table scan filtered on
--         a column that is well-clustered?
--         The SQL pruner did not skip any micro-partitions when reading the
--         table CUSTOMER because there was no WHERE clause, and the JOIN
--         condition was on a column that was not well clustered.
--         Table Scan Node

-- 11.1.8  Check the clustering quality of the c_custkey column.

SELECT SYSTEM$CLUSTERING_INFORMATION( 'customer' , '(c_custkey)' );


-- 11.1.9  Open the row and examine the result.

-- 11.2.0  Explore GROUP BY and ORDER BY Operation Performance

-- 11.2.1  Set the warehouse size.

ALTER warehouse INSTRUCTOR1_QUERY_WH SET WAREHOUSE_SIZE=SMALL;


-- 11.2.2  Run a query which has a GROUP BY on a column with few distinct
--         values.

SELECT l_returnflag,
l_linestatus,
sum(l_quantity) as sum_qty,
sum(l_extendedprice) as sum_base_price,
sum(l_extendedprice * (1-l_discount)) as sum_disc_price,
sum(l_extendedprice * (1-l_discount) *
(1+l_tax)) as sum_charge,
avg(l_quantity) as avg_qty,
avg(l_extendedprice) as avg_price,
avg(l_discount) as avg_disc,
count(*) as count_order
FROM lineitem
WHERE l_shipdate <= dateadd(day, -90, to_date('1998-12-01'))
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;

--         Note that this produces only four groups.

-- 11.2.3  View the query profile and click on the operator Aggregate [1].
--         Note that the amount of data shuffled during the parallel aggregation
--         operation (Bytes sent over the network) is minimal.
--         Aggregate Node

-- 11.2.4  Click ouside the operator to show the statistics panel.
--         Note that there is no spilling to local or remote storage.

-- 11.2.5  Click on the operator Sort [4].
--         This is the operator for the ORDER BY. Note that the amount of data
--         shuffled during the global sort operation is also minimal.
--         Sort Node

-- 11.2.6  Run a query with a GROUP BY on a column with many distinct values.

SELECT l_shipdate, count( * ) FROM lineitem
GROUP BY 1
ORDER BY 1;

--         Note that this creates over 2,000 groups.

-- 11.3.0  Querying with LIMIT
--         Applying a LIMIT clause to a query does not affect the amount of data
--         that is read; it merely limits the results set output.

-- 11.3.1  Execute the following query with a LIMIT clause.

SELECT
S.SS_SOLD_DATE_SK,
R.SR_RETURNED_DATE_SK,
S.SS_STORE_SK,
S.SS_ITEM_SK,
S.SS_CUSTOMER_SK,
S.SS_TICKET_NUMBER,
S.SS_QUANTITY,
S.SS_SALES_PRICE,
S.SS_CUSTOMER_SK,
S.SS_STORE_SK,
S.SS_QUANTITY,
S.SS_SALES_PRICE,
R.SR_RETURN_AMT
FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.STORE_SALES  S
INNER JOIN SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.STORE_RETURNS  R on R.SR_ITEM_SK=S.SS_ITEM_SK
WHERE  S.SS_ITEM_SK =4164
LIMIT 100;


-- 11.3.2  Review the query profile:
--         Limit Node
--         Notice that the LIMIT operator is processed at the very end of the
--         query, and has no impact on table access or JOIN filtering. But the
--         LIMIT clause does help to reduce the query result output, which helps
--         to speed up the overall query performance.

-- 11.4.0  Join Optimizations in Snowflake
--         JOIN is one of the most resource-intensive operations. The Snowflake
--         optimizer provides built-in dynamic partition pruning to help reduce
--         data access during join processing. If you use a JOIN filter column
--         that is well-clustered, the query optimization can push down micro-
--         partition pruning.

-- 11.4.1  Set your context.

USE SCHEMA snowflake_sample_data.tpcds_sf10tcl;


-- 11.4.2  Run the following query.

SELECT count(ss_customer_sk) 
FROM store_sales JOIN date_dim d
ON ss_sold_date_sk = d_date_sk
WHERE d_year = 2000
GROUP BY ss_customer_sk;


-- 11.4.3  Open the query profile, and click on the operator TableScan [4].
--         Table Scan Node

-- 11.4.4  Take note of the performance metrics for this table scan.
--         The micro-partition pruning is fairly effective; the SQL pruner
--         skipped a large number of micro-partitions. This corresponds to the
--         filter: D.D_DATE_SK = STORE_SALES.SS_SOLD_DATE_SK

-- 11.4.5  Check the clustering quality of the filter column.

SELECT SYSTEM$CLUSTERING_INFORMATION( 'snowflake_sample_data.tpcds_sf10tcl.store_sales', '(ss_sold_date_sk)');


-- 11.4.6  Review the result.
--         All of these contribute to better micro-partition pruning.

-- 11.5.0  Using the Search Optimization Service
--         This lab takes you through the steps needed to identify a table that
--         can benefit from a search optimization, and register the search
--         optimization service on that table. You will perform a selective
--         lookup query on the table both before and after applying the search
--         optimization, and note the difference in performance.

-- 11.5.1  Set your context.

CREATE WAREHOUSE IF NOT EXISTS INSTRUCTOR1_QUERY_WH;
USE WAREHOUSE INSTRUCTOR1_QUERY_WH;

CREATE DATABASE IF NOT EXISTS INSTRUCTOR1_DB;
USE DATABASE INSTRUCTOR1_DB;

CREATE SCHEMA IF NOT EXISTS SEARCH;
USE SCHEMA SEARCH;

ALTER SESSION SET USE_CACHED_RESULT = FALSE;


-- 11.5.2  Create a table that can be enabled for the Search Optimization
--         service.
--         Clone the web_sales table from training_db.traininglab. This allows
--         you to put a search optimization on the cloned table, and compare
--         performance of a point query between the cloned table and the
--         original (wihtout the search optimization).

CREATE OR REPLACE TABLE WEB_SALES_SO CLONE TRAINING_DB.TRAININGLAB.WEB_SALES;


-- 11.5.3  Register the cloned table to the search optimization service.
--         Search optimization is a table-level property and applies to all
--         columns with supported data types (date, number, string, etc.).

ALTER TABLE WEB_SALES_SO ADD SEARCH OPTIMIZATION;

--         Search optimization has a maintenance service that runs in the
--         background to create and maintain the search access paths. The
--         service will check roughly once an hour for new work (table) that
--         needs to have a search structure built.
--         Before you run a query to verify that search optimization is working,
--         wait until this shows that the table has been fully optimized.

-- 11.5.4  Verify that the search structure is complete.
--         Run the SHOW TABLES command to verify that search optimization has
--         been added and to determine how much of the table has been optimized

SHOW TABLES LIKE '%WEB_SALES%';
SET QID = LAST_QUERY_ID();
DESCRIBE RESULT $QID;

SELECT  
  "name", "search_optimization", 
  "search_optimization_progress", 
  "bytes" as "bytes in table", 
  "search_optimization_bytes"
FROM TABLE(RESULT_SCAN($QID));

--         In the output from this command:
--         Verify that SEARCH_OPTIMIZATION is ON, which indicates that search
--         optimization has been added.
--         Check the value of SEARCH_OPTIMIZATION_PROGRESS. This specifies the
--         percentage of the table that has been optimized so far.
--         For example:

/*
  name        rows         bytes          search_optimization   search_optimization_progress
  ----------- ------------ -------------- --------------------- ------------------------------
  WEB_SALES   7199963324   132571177472   ON                    100
*/


-- 11.6.0  Test Search Optimization Performance
--         Search optimization works best to improve the performance of a query
--         when the following conditions are true:
--         The query requires a fast response (in seconds)
--         Queries on the table frequently include an equality filter on columns
--         that may not be well clustered
--         The query does not include any expressions, functions, or
--         conjunctions like OR
--         At least one of the columns that is accessed through the query filter
--         has at least 100k distinct values

-- 11.6.1  Show order number, order date, and items sold for one customer on the
--         original (not optimized) table:

SELECT
  WS_ORDER_NUMBER, WS_SOLD_DATE_SK, WS_ITEM_SK
FROM TRAINING_DB.TRAININGLAB.WEB_SALES
  WHERE 
WS_BILL_CUSTOMER_SK = 956673;


-- 11.6.2  Show order number, order date and items sold for one customer on the
--         search-optimized table.

SELECT
  WS_ORDER_NUMBER, WS_SOLD_DATE_SK, WS_ITEM_SK
FROM WEB_SALES_SO
  WHERE 
WS_BILL_CUSTOMER_SK = 956673;


-- 11.6.3  Review the Query Profile and note the data access path is the Search
--         Optimization Access operator.

-- 11.6.4  Estimate the number of distinct values (cardinality) in the
--         WS_BILL_CUSTOMER_SK column.
--         The point query executes fast in the previous step. This is because
--         the cardinality of the filter column is sufficiently high:

SELECT
  APPROX_COUNT_DISTINCT(WS_BILL_CUSTOMER_SK)
FROM WEB_SALES_SO;

--         On the contrary, the column ws_web_site_sk has low cardinality:

SELECT
  APPROX_COUNT_DISTINCT(WS_WEB_SITE_SK)
FROM WEB_SALES_SO;

--         The query below using ws_web_site_sk as the search column is not a
--         point query, so performance will not be helped by the search
--         optimization.

SELECT
  WS_ORDER_NUMBER,WS_SOLD_DATE_SK, WS_ITEM_SK
FROM WEB_SALES_SO
  WHERE
WS_WEB_SITE_SK IN (1, 8, 23, 61, 73);


-- 11.7.0  Explore Costs Associated with Search Optimization
--         These costs depend upon multiple factors, including the number of
--         distinct values (NDVs) in the table. Typically, the size of the
--         search optimization structure is approximately 25% of the original
--         table’s size.
--         In the extreme case where all columns have data types that use the
--         search access path, and all data values in each column are unique,
--         the required storage can be as much as the original table’s size.

-- 11.7.1  Find which columns have high cardinality (large number of distinct
--         values)
--         Use the HLL function to approximate the count of distinct values in
--         each column:;

SELECT
  HLL(WS_SOLD_DATE_SK),
  HLL(WS_SOLD_TIME_SK),
  HLL(WS_SHIP_DATE_SK),
  HLL(WS_ITEM_SK),
  HLL(WS_BILL_CUSTOMER_SK),
  HLL(WS_BILL_CDEMO_SK),
  HLL(WS_BILL_HDEMO_SK),
  HLL(WS_BILL_ADDR_SK),
  HLL(WS_SHIP_CUSTOMER_SK),
  HLL(WS_SHIP_CDEMO_SK),
  HLL(WS_SHIP_HDEMO_SK),
  HLL(WS_SHIP_ADDR_SK),
  HLL(WS_WEB_PAGE_SK),
  HLL(WS_WEB_SITE_SK),
  HLL(WS_SHIP_MODE_SK),
  HLL(WS_WAREHOUSE_SK),
  HLL(WS_PROMO_SK),
  HLL(WS_ORDER_NUMBER),
  HLL(WS_QUANTITY),
  HLL(WS_WHOLESALE_COST),
  HLL(WS_LIST_PRICE),
  HLL(WS_SALES_PRICE),
  HLL(WS_EXT_DISCOUNT_AMT),
  HLL(WS_EXT_SALES_PRICE),
  HLL(WS_EXT_WHOLESALE_COST)
FROM WEB_SALES_SO;


-- 11.7.2  Note the cost of Search Optimization for building the structure on
--         your big table
--         It is possible to use either the WebUI or SQL:

SELECT
  DATABASE_NAME,
  TABLE_ID,
  TABLE_NAME,
  START_TIME,
  END_TIME,
  CREDITS_USED
FROM SNOWFLAKE.ACCOUNT_USAGE.SEARCH_OPTIMIZATION_HISTORY
WHERE
  SCHEMA_NAME = CURRENT_SCHEMA()
  AND DATABASE_NAME = CURRENT_DATABASE();


-- 11.7.3  Use the system function to estimate the costs of adding a search
--         optimization to the table:

SELECT SYSTEM$ESTIMATE_SEARCH_OPTIMIZATION_COSTS('INSTRUCTOR1_DB.SEARCH.WEB_SALES_SO');

