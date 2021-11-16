
-- 13.0.0  Three Vectors of Scaling a Virtual Warehouse
--         This lab will take approximately 20 minutes to complete.

-- 13.1.0  Create Warehouses for the Lab

-- 13.1.1  Create a LOAD virtual warehouse:

USE ROLE TRAINING_ROLE;

CREATE OR REPLACE WAREHOUSE CHIPMUNK_LOAD_WH 
  WAREHOUSE_SIZE = 'LARGE'
  WAREHOUSE_TYPE = 'STANDARD'
  AUTO_SUSPEND = 65
  AUTO_RESUME = true
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 2
  INITIALLY_SUSPENDED = true
  SCALING_POLICY = 'STANDARD'
  COMMENT = 'Training WH for completing hands-on lab queries';


-- 13.1.2  Create a QUERY virtual warehouse.

CREATE OR REPLACE WAREHOUSE CHIPMUNK_QUERY_WH 
  WAREHOUSE_SIZE = 'LARGE'
  WAREHOUSE_TYPE = 'STANDARD'
  AUTO_SUSPEND = 60
  AUTO_RESUME = true
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 2
  INITIALLY_SUSPENDED = true
  SCALING_POLICY = 'STANDARD'
  COMMENT = 'Training WH for completing hands-on lab queries';


-- 13.1.3  SHOW the warehouses.

SHOW WAREHOUSES LIKE 'CHIPMUNK%';


-- 13.2.0  Explore AUTO_SUSPEND and AUTO_RESUME

-- 13.2.1  Set the auto_suspend time on your LOAD warehouse to one minute.

ALTER WAREHOUSE CHIPMUNK_LOAD_WH SET AUTO_SUSPEND = 60;


-- 13.2.2  Verify your warehouse using the top ribbon.
--         In the top ribbon, select the Warehouses tab. Locate your warehouse
--         and confirm that the auto_suspend time is set to 1 minutes. Then
--         return to your worksheet.

-- 13.2.3  Suspend your LOAD warehouse.

ALTER WAREHOUSE CHIPMUNK_LOAD_WH SUSPEND;

--         If you get the message, Invalid State. Warehouse CHIPMUNK_LOAD_WH
--         cannot be suspended, that means that the warehouse is already
--         suspended. You can ignore this message whenever it appears.

-- 13.2.4  Locate your warehouse in the top ribbon and confirm that it is now
--         suspended.
--         When you have verified its state, return to the worksheet.

-- 13.2.5  Submit a query on a suspended warehouse.
--         Auto_resume is enabled by default when you create a virtual
--         warehouse. When you submit a command that requires a warehouse, it
--         will automatically resume.

USE WAREHOUSE CHIPMUNK_QUERY_WH;

--Turn off the query result cache so the warehouse must be used for the query.
ALTER SESSION SET USE_CACHED_RESULT = false;

SELECT * FROM TRAINING_DB.TRAININGLAB.REGION;

--         Because the warehouse is configured to auto resume, it resumed
--         automatically when it was needed to process a query.

-- 13.2.6  Locate your warehouse in the top ribbon and confirm that it is now
--         running.
--         When you have verified its state, return to the worksheet.

-- 13.3.0  Size Warehouses Up and Down
--         Resizing can be completed at any time, even when the virtual
--         warehouse is running.

-- 13.3.1  Resize one of your warehouses.

ALTER WAREHOUSE CHIPMUNK_QUERY_WH SET WAREHOUSE_SIZE = 'X-LARGE';

SHOW WAREHOUSES LIKE 'CHIPMUNK_QUERY_WH';

ALTER WAREHOUSE CHIPMUNK_QUERY_WH SET WAREHOUSE_SIZE = 'MEDIUM';

SHOW WAREHOUSES LIKE 'CHIPMUNK_QUERY_WH';

--         Note the change in each warehouse size after running the ALTER
--         commands.

-- 13.3.2  Locate your warehouse in the top ribbon and confirm that its size is
--         now MEDIUM.
--         Return to the worksheet when you are done.

-- 13.3.3  Set your virtual warehouse size to LARGE.

USE ROLE training_role;
USE WAREHOUSE CHIPMUNK_query_wh;
USE DATABASE snowflake_sample_data;
USE SCHEMA TPCDS_SF10TCL;

ALTER WAREHOUSE CHIPMUNK_QUERY_WH SET WAREHOUSE_SIZE=LARGE;

ALTER SESSION SET USE_CACHED_RESULT=FALSE;


-- 13.3.4  Run query 3 of the TPCDS_SF10TCL schema.

SELECT dt.d_year 
  ,item.i_brand_id brand_id 
  ,item.i_brand brand
  ,sum(ss_net_profit) sum_agg
FROM  date_dim dt 
  ,store_sales
  ,item
WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
  and store_sales.ss_item_sk = item.i_item_sk
  and item.i_manufact_id = 939
  and dt.d_moy=12
GROUP BY dt.d_year
  ,item.i_brand
  ,item.i_brand_id
ORDER BY dt.d_year
  ,sum_agg desc
  ,brand_id
LIMIT 100;


-- 13.3.5  Examine the query profile overview and record the execution time.
--         Query Profile (Large virtual warehouse)

-- 13.3.6  Set your virtual warehouse size to XLARGE.

ALTER WAREHOUSE CHIPMUNK_QUERY_WH SUSPEND;

ALTER WAREHOUSE CHIPMUNK_QUERY_WH SET WAREHOUSE_SIZE=XLARGE;


-- 13.3.7  Re-run query 3 of the TPCDS_SF10TCL schema.

SELECT dt.d_year 
  ,item.i_brand_id brand_id 
  ,item.i_brand brand
  ,sum(ss_net_profit) sum_agg
FROM  date_dim dt 
  ,store_sales
  ,item
WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
  and store_sales.ss_item_sk = item.i_item_sk
  and item.i_manufact_id = 939
  and dt.d_moy=12
GROUP BY dt.d_year
  ,item.i_brand
  ,item.i_brand_id
ORDER BY dt.d_year
  ,sum_agg desc
  ,brand_id
LIMIT 100;


-- 13.3.8  Examine the query profile overview and record the execution time.
--         Query Profile (X-Large virtual warehouse)
--         Observation: Doubling the virtual warehouse size cut the elapsed time
--         in half. The query ran twice as fast, for the same cost (as billed
--         per second).
--         Twice the speed for the same cost

-- 13.3.9  Set the warehouse size back to LARGE, and turn on the query result
--         cache.

ALTER WAREHOUSE CHIPMUNK_QUERY_WH SET WAREHOUSE_SIZE=LARGE;
ALTER SESSION SET USE_CACHED_RESULT=TRUE;


-- 13.4.0  Size Warehouses Out and Back
--         A Snowflake multi-cluster warehouse consists of one or more clusters
--         of servers that execute queries. For a given warehouse, a user can
--         set both the minimum and maximum number of compute clusters to
--         allocate to that warehouse.

-- 13.4.1  Create an auto-scaling virtual warehouse.

CREATE OR REPLACE WAREHOUSE CHIPMUNK_SCALE_OUT_WH 
WAREHOUSE_SIZE = 'SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = true
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 3
  INITIALLY_SUSPENDED = true
  SCALING_POLICY = 'STANDARD'
  COMMENT = 'Training WH for completing concurrency tests';
  
SHOW WAREHOUSES LIKE 'CHIPMUNK_SCALE_OUT_WH';

--         Note: By setting the MAX_CLUSTER_COUNT greater than the
--         MIN_CLUSTER_COUNT, you are configuring the Warehouse in auto-scale
--         mode. This allows Snowflake to scale the warehouse as needed to
--         handing fluctuating workloads.
