
-- 17.0.0  Data Sharing
--         This lab will take approximately 30 minutes to complete.
--         Secure Data Sharing enables account-to-account sharing of data
--         through Snowflake database tables, secure views, and secure UDFs.
--         In this exercise you will learn how to setup two Snowflake accounts
--         for data sharing. The first account will be the data [provider-
--         account] . The second account will be the data [consumer-account].
--         You will then perform the steps to enable sharing selected objects in
--         a database in the [provider-account] with the [consumer-account]. No
--         actual data is copied or transferred between accounts. All data
--         sharing is accomplished through Snowflake’s unique services layer and
--         metadata store.

-- 17.1.0  Setup Browsers for Data Sharing

-- 17.1.1  Open two browser windows side-by-side.

-- 17.1.2  In browser 1, enter the Snowflake [provider-account] URL, login,
--         navigate to worksheets and load the script into a new worksheet.
--         In this worksheet, you will only execute commands that are surrounded
--         by PROVIDER comments:

-- PROVIDER --


-- 17.1.3  Rename the worksheet to something that contains the word provider.

-- 17.1.4  In browser 2, enter the Snowflake [consumer-account] URL, login,
--         navigate to worksheets and load the script into a new worksheet.
--         In this worksheet, you will only execute commands that are surrounded
--         by CONSUMER comments:

-- CONSUMER --


-- 17.1.5  Rename the worksheet to something that contains the word consumer.
--         Data Sharing Browser Setup

-- 17.2.0  Basic Data Sharing
--         In this exercise you will create a basic data share and share data.
--         Perform the following steps in the [provider-account] in browser 1.

-- 17.2.1  Set the context

-- PROVIDER --
USE ROLE TRAINING_ROLE;
CREATE WAREHOUSE IF NOT EXISTS CHIPMUNK_QUERY_WH;
USE WAREHOUSE CHIPMUNK_QUERY_WH;
CREATE DATABASE CHIPMUNK_SHARE_DB;
USE DATABASE CHIPMUNK_SHARE_DB;
-- PROVIDER --


-- 17.2.2  Create schema and tables

-- PROVIDER --
CREATE SCHEMA DS_TPCH_SF1;
USE SCHEMA DS_TPCH_SF1;

CREATE TABLE CUSTOMER AS 
SELECT C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT 
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER;

CREATE TABLE ORDERS AS 
SELECT O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, 
       O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT 
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS;
-- PROVIDER --


-- 17.2.3  Create empty share
--         An empty share is a shell that you can later use to share actual
--         objects.

-- PROVIDER --
CREATE SHARE CHIPMUNK_SHARE;
-- PROVIDER --


-- 17.2.4  Grant object privileges to the share

-- PROVIDER --
GRANT USAGE ON DATABASE CHIPMUNK_SHARE_DB TO SHARE CHIPMUNK_SHARE;

GRANT USAGE ON SCHEMA CHIPMUNK_SHARE_DB.DS_TPCH_SF1 TO SHARE CHIPMUNK_SHARE;

GRANT SELECT ON TABLE CHIPMUNK_SHARE_DB.DS_TPCH_SF1.CUSTOMER TO SHARE CHIPMUNK_SHARE;

GRANT SELECT ON TABLE CHIPMUNK_SHARE_DB.DS_TPCH_SF1.ORDERS TO SHARE CHIPMUNK_SHARE;
-- PROVIDER --


-- 17.2.5  Add account to the share

-- PROVIDER --
ALTER SHARE CHIPMUNK_SHARE SET ACCOUNTS=[consumer-account];
-- PROVIDER --


-- 17.2.6  Validate the share configuration

-- PROVIDER --
SHOW GRANTS TO SHARE CHIPMUNK_SHARE;
-- PROVIDER --


-- 17.3.0  Create database on data consumer account from tables shared on the
--         data provider server
--         Perform all the steps in this section on [consumer-account] in
--         browser 2.

-- 17.3.1  View the in inbound shares in the web ui.

-- 17.3.2  Click the Shares icon.
--         Shares Icon

-- 17.3.3  You should see something like:
--         Inbound Shares

-- 17.3.4  View Outbound Shares on the Data Provider

-- 17.3.5  On the data provider server, click on the Shares button and then hit
--         the Outbound button. You should see the share that you setup.
--         Outbound Shares

-- 17.3.6  Create a Warehouse

-- CONSUMER --
USE ROLE TRAINING_ROLE;

CREATE OR REPLACE WAREHOUSE CHIPMUNK_QUERY_WH 
   WAREHOUSE_SIZE = 'LARGE' 
   AUTO_SUSPEND = 300 
   AUTO_RESUME = TRUE 
   MIN_CLUSTER_COUNT = 1 
   MAX_CLUSTER_COUNT = 1 
   SCALING_POLICY = 'STANDARD' 
   COMMENT = 'Training WH for completing hands on lab queries';
-- CONSUMER --


-- 17.3.7  Use SQL to show available shares

-- CONSUMER --
SHOW SHARES LIKE 'CHIPMUNK_SHARE';

DESCRIBE SHARE [provider-account].CHIPMUNK_SHARE;
-- CONSUMER --


-- 17.3.8  Examine contents of share on data consumer

-- CONSUMER --
CREATE DATABASE CHIPMUNK_DS_CONSUMER
FROM SHARE [provider-account].CHIPMUNK_SHARE;
USE DATABASE CHIPMUNK_DS_CONSUMER;

SHOW SCHEMAS;
USE SCHEMA DS_TPCH_SF1;

SHOW TABLES;
SELECT * from CUSTOMER LIMIT 10;
-- CONSUMER --


-- 17.3.9  Use Public Role, and test ability to query share

-- CONSUMER --
GRANT USAGE ON WAREHOUSE CHIPMUNK_query_wh TO ROLE public;

USE ROLE PUBLIC;
USE DATABASE CHIPMUNK_DS_CONSUMER;
--This command will fail because PUBLIC does not have access to the database

USE ROLE TRAINING_ROLE;
GRANT USAGE ON DATABASE CHIPMUNK_DS_CONSUMER TO ROLE public;
--You should have received an error message: you cannot add privileges to a share

GRANT IMPORTED PRIVILEGES ON DATABASE CHIPMUNK_DS_CONSUMER TO ROLE PUBLIC;

USE ROLE PUBLIC;
USE DATABASE CHIPMUNK_DS_CONSUMER;

SHOW SCHEMAS;
USE SCHEMA DS_TPCH_SF1;

SELECT * from CUSTOMER LIMIT 10;
-- CONSUMER --

--         Note: You will run into a few error messages as you run each
--         statement above. Keep going and the subsequent queries will make the
--         necessary changes.

-- 17.4.0  Create A Secure View And Add It To The Share
--         Perform all the steps in this section on [provider-account] in
--         browser 1.

-- 17.4.1  Create a schema

-- PROVIDER --
USE DATABASE CHIPMUNK_share_db;
CREATE SCHEMA private;
-- PROVIDER --


-- 17.4.2  Create a mapping table
--         A mapping table is only required if you wish to share the data in the
--         base table with multiple consumer accounts and share specific rows in
--         the table with specific accounts.

-- PROVIDER --
CREATE OR REPLACE TABLE PRIVATE.SHARING_ACCESS(
  C_CUSTKEY STRING,
  SNOWFLAKE_ACCOUNT STRING);
-- PROVIDER --


-- 17.4.3  Populate mapping table

-- PROVIDER --
INSERT INTO PRIVATE.SHARING_ACCESS (C_CUSTKEY, SNOWFLAKE_ACCOUNT)
SELECT C_CUSTKEY,
       CASE WHEN C_CUSTKEY BETWEEN 1 AND 20 THEN '[consumer-account]'
            ELSE 'UNKNOWN'
            END AS SNOWFLAKE_ACCOUNT
FROM DS_TPCH_SF1.CUSTOMER
WHERE C_CUSTKEY BETWEEN 1 AND 50;
-- PROVIDER --


-- 17.4.4  Create a secure view
--         Remember a secure view hides the SQL used to create the view and runs
--         the optimizer after the secured data is filtered out meaning that the
--         query will not return an access error message. Unauthorized values
--         will appear as not in the table

-- PROVIDER --
CREATE OR REPLACE SECURE VIEW DS_TPCH_SF1.CUST_SENSITIVE_DATA_VW AS
SELECT SD.C_CUSTKEY, 
    SD.C_NAME, 
    SD.C_ADDRESS, 
    SD.C_NATIONKEY, 
    SD.C_PHONE
FROM DS_TPCH_SF1.CUSTOMER SD
INNER JOIN PRIVATE.SHARING_ACCESS SA 
ON SD.C_CUSTKEY = SA.C_CUSTKEY 
AND UPPER(SA.SNOWFLAKE_ACCOUNT) = UPPER(CURRENT_ACCOUNT());
-- PROVIDER --


-- 17.4.5  Validate the table and secure view

-- PROVIDER --
SELECT *
FROM DS_TPCH_SF1.CUST_SENSITIVE_DATA_VW;
-- should return 0 rows because the provider account is not mapped
-- PROVIDER --


-- 17.4.6  Validate the secure view by simulating data consumer

-- PROVIDER --
ALTER SESSION SET SIMULATED_DATA_SHARING_CONSUMER='[consumer-account]';
SELECT *
FROM DS_TPCH_SF1.CUST_SENSITIVE_DATA_VW;
-- This should return 20 rows because simulated consumer account is mapped!
ALTER SESSION UNSET SIMULATED_DATA_SHARING_CONSUMER;
-- PROVIDER --


-- 17.4.7  Add the secure view to the share

-- PROVIDER --
GRANT SELECT ON CHIPMUNK_SHARE_DB.DS_TPCH_SF1.CUST_SENSITIVE_DATA_VW TO SHARE CHIPMUNK_SHARE;


-- 17.4.8  Confirm grants on the share

-- PROVIDER --
SHOW GRANTS TO SHARE CHIPMUNK_SHARE;
-- PROVIDER --


-- 17.5.0  Data Consumer - Use A Shared Database
--         Perform all the steps in this section on [consumer-account] in
--         browser 2.

-- 17.5.1  Set context and create warehouse if it does not exist

-- CONSUMER --
USE ROLE TRAINING_ROLE;
CREATE OR REPLACE WAREHOUSE CHIPMUNK_LOAD_WH 
   WAREHOUSE_SIZE = 'SMALL' 
   AUTO_SUSPEND = 300 
   AUTO_RESUME = TRUE 
   MIN_CLUSTER_COUNT = 1 
   MAX_CLUSTER_COUNT = 1 
   SCALING_POLICY = 'STANDARD' 
   COMMENT = 'Training WH for completing hands on lab queries';
-- CONSUMER --


-- 17.5.2  View available shares

-- CONSUMER --
SHOW SHARES LIKE 'CHIPMUNK_SHARE';
DESCRIBE SHARE [provider-account].CHIPMUNK_SHARE;
-- CONSUMER --


-- 17.5.3  Create a database from the share and grant privileges

-- CONSUMER --
CREATE DATABASE CHIPMUNK_DS_CONSUMER
FROM SHARE [provider-account].CHIPMUNK_SHARE;
--this will generate an error, because the database already exists, from the 
--first time the consumer ingested the share.  The provider did not create a
--new share, but added objects to an existing share.
GRANT IMPORTED PRIVILEGES ON DATABASE CHIPMUNK_DS_CONSUMER TO ROLE TRAINING_ROLE;
-- CONSUMER --


-- 17.5.4  Validate shared objects

-- CONSUMER --
SHOW DATABASES LIKE 'CHIPMUNK_DS_CONSUMER';
SHOW SCHEMAS IN DATABASE CHIPMUNK_DS_CONSUMER;
SHOW TABLES IN SCHEMA CHIPMUNK_DS_CONSUMER.DS_TPCH_SF1;
SHOW VIEWS IN SCHEMA CHIPMUNK_DS_CONSUMER.DS_TPCH_SF1;


-- 17.5.5  Validate shared objects by running queries

-- CONSUMER --
SELECT COUNT(*)
FROM CHIPMUNK_DS_CONSUMER.DS_TPCH_SF1.CUST_SENSITIVE_DATA_VW;
-- expected row count =  20
SELECT COUNT(*)
FROM CHIPMUNK_DS_CONSUMER.DS_TPCH_SF1.CUSTOMER;
-- expected row count = 150000
SELECT COUNT(*)
FROM CHIPMUNK_DS_CONSUMER.DS_TPCH_SF1.ORDERS;
-- expected row count = 1500000
-- CONSUMER --


-- 17.6.0  Remove objects
--         Perform the next steps in the [provider-account] in browser 1.

-- 17.6.1  Remove the share

-- PROVIDER --
USE CHIPMUNK_SHARE_DB;
DESCRIBE SHARE CHIPMUNK_SHARE;
-- Revoke access to the ORDERS table:
REVOKE SELECT ON TABLE CHIPMUNK_SHARE_DB.DS_TPCH_SF1.ORDERS FROM SHARE CHIPMUNK_SHARE;
-- PROVIDER --


-- 17.6.2  Confirm that the table was removed

-- PROVIDER --
DESCRIBE SHARE CHIPMUNK_SHARE;
-- PROVIDER --


-- 17.6.3  Confirm that the ORDERS table was revoked
--         Perform the next steps in the [consumer-account] in browser 2.

-- CONSUMER --
DESC SHARE [provider-account].CHIPMUNK_SHARE;
SELECT COUNT (*) FROM CHIPMUNK_DS_CONSUMER.DS_TPCH_SF1.ORDERS;
-- should fail with SQL compilation error
-- CONSUMER --


-- 17.6.4  Verify table data

-- CONSUMER --
SELECT MIN(C_CUSTKEY)
  FROM CHIPMUNK_DS_CONSUMER.DS_TPCH_SF1.CUSTOMER;
-- expected result = 1
-- CONSUMER --


-- 17.6.5  Delete a row in the CUSTOMER table shared with the consumer account.
--         Perform this step on the [provider-account] in browser 1.

-- PROVIDER --
DELETE
  FROM CHIPMUNK_SHARE_DB.DS_TPCH_SF1.CUSTOMER
  WHERE C_CUSTKEY = 1;
-- expected result = 1 Rows deleted
-- PROVIDER --


-- 17.6.6  Verify the row was deleted
--         Perform this step on the [consumer-account] in browser 2.

-- CONSUMER --
SELECT MIN(C_CUSTKEY)
  FROM CHIPMUNK_DS_CONSUMER.DS_TPCH_SF1.CUSTOMER;
-- expected result = 2. This is different from before because the row
-- with C_CUSTKEY=1 was removed by the provider.
-- CONSUMER --


-- 17.7.0  The Power Of Secure User-defined Functions For Protecting Shared Data
--         Secure Views And Their Limitations
--         Today, most data sharing in Snowflake uses secure views. Secure views
--         are a great way for a data owner to grant other Snowflake users
--         secure access to select subsets of their data.
--         Secure views are effective for enforcing cell-level security in
--         multi-tenant situations. This includes software-as-a-service (SaaS)
--         providers granting access to each of their customers, while allowing
--         each customer to see only their specific rows of data from each
--         table. However, there is nothing preventing another user from running
--         a SELECT * query against the secure view and then exporting all the
--         data that’s visible to them.
--         In many situations, allowing a data consumer to see and export the
--         raw data is completely acceptable. However, in other situations, such
--         as when monetizing data, the most valuable analyses are often run
--         against low-level and raw data, and allowing a data consumer to
--         export the raw data is not desirable. Furthermore, when PII and PHI
--         are involved, privacy policies and government regulations often do
--         not permit providing data access to other parties.
--         Perform the next steps on the [provider-account] in browser 1.

-- 17.7.1  The Power Of Secure UDFs
--         Secure UDFs are small pieces of SQL or JavaScript code that securely
--         operate against raw data, but provide only a constrained set of
--         outputs in response to specific inputs. For example, imagine a
--         retailer that wants to allow its suppliers to see which items from
--         other suppliers are commonly sold together with theirs. This is known
--         as market basket analysis.
--         Using the TCP-DS sample data set that’s available to all users from
--         the Shares tab within Snowflake, we can run the following SQL
--         commands to create a test data set and perform a market basket
--         analysis:

-- PROVIDER --
CREATE DATABASE IF NOT EXISTS CHIPMUNK_UDF_DEMO;
CREATE SCHEMA IF NOT EXISTS CHIPMUNK_UDF_DEMO.PUBLIC;

CREATE OR REPLACE TABLE CHIPMUNK_udf_demo.public.sales AS
(SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.STORE_SALES
 SAMPLE BLOCK (1));

select 6139 as input_item
     , ss_item_sk as basket_Item
     , count(distinct ss_ticket_number) baskets
  from CHIPMUNK_udf_demo.public.sales  
  where ss_ticket_number in 
         (select ss_ticket_number 
             from CHIPMUNK_udf_demo.public.sales 
             where ss_item_sk = 6139)
  group by ss_item_sk
  order by 3 desc, 2;
-- PROVIDER --


-- 17.7.2  Create a Secure UDF
--         This example returns the items that sold together with item #6139.
--         This example outputs only aggregated data, which is the number of
--         times various other products are sold together, in the same
--         transaction, with item #6139. This SQL statement needs to operate
--         across all of the raw data to find the right subset of transactions.
--         To enable this type of analysis while preventing the user who is
--         performing the analysis from seeing the raw data, we wrap this SQL
--         statement in a secure UDF and add an input parameter to specify the
--         item number we are selecting for market basket analysis, as follows:

-- PROVIDER --
CREATE OR REPLACE SECURE FUNCTION CHIPMUNK_UDF_DEMO.PUBLIC.GET_MARKET_BASKET(INPUT_ITEM_SK NUMBER(38))
RETURNS TABLE (INPUT_ITEM NUMBER(38,0), BASKET_ITEM_SK NUMBER(38,0),NUM_BASKETS NUMBER(38,0))
AS
 'SELECT INPUT_ITEM_SK
       , SS_ITEM_SK BASKET_ITEM
       , COUNT(DISTINCT SS_TICKET_NUMBER) BASKETS
    FROM CHIPMUNK_UDF_DEMO.PUBLIC.SALES
    WHERE SS_TICKET_NUMBER IN (SELECT SS_TICKET_NUMBER FROM CHIPMUNK_UDF_DEMO.PUBLIC.SALES WHERE SS_ITEM_SK = INPUT_ITEM_SK)
GROUP BY SS_ITEM_SK
ORDER BY 3 DESC, 2';

SELECT * FROM TABLE(CHIPMUNK_UDF_DEMO.PUBLIC.GET_MARKET_BASKET(6139));
-- PROVIDER --

--         We can then call this function and specify any item number as an
--         input, and we will get the same results we received when running the
--         SQL statement directly. Now, we can grant a specified user access to
--         this function while preventing the user from accessing the underlying
--         transactional data.

-- 17.7.3  How To Share Secure UDFs
--         To share a secure UDF, we can then grant usage rights on the secure
--         UDF to a Snowflake share. This gives other specified Snowflake
--         accounts the ability to run the secure UDF, but does not grant any
--         access rights to the data in the underlying tables.

-- PROVIDER --
USE DATABASE CHIPMUNK_udf_demo;
CREATE SHARE IF NOT EXISTS CHIPMUNK_UDF_DEMO_SHARE;
GRANT USAGE ON DATABASE CHIPMUNK_UDF_DEMO TO SHARE CHIPMUNK_UDF_DEMO_SHARE;
GRANT USAGE ON SCHEMA CHIPMUNK_UDF_DEMO.PUBLIC to share CHIPMUNK_UDF_DEMO_SHARE;
GRANT USAGE ON FUNCTION CHIPMUNK_UDF_DEMO.PUBLIC.get_market_basket(number) to share CHIPMUNK_UDF_DEMO_SHARE;
ALTER SHARE CHIPMUNK_UDF_DEMO_SHARE ADD ACCOUNTS=[consumer-account];
-- PROVIDER --


-- 17.7.4  Create database from share and run query on the data consumer.
--         Perform this step on the [consumer-account] in browser 2.
--         We can run the secure UDF from the share using the second account’s
--         virtual warehouse. However, from the second account, we cannot select
--         any data from the underlying tables, determine anything about the
--         names or structures of the underlying tables, or see the code behind
--         the secure UDF.

-- CONSUMER --
USE ROLE TRAINING_ROLE;
CREATE DATABASE CHIPMUNK_UDF_TEST FROM SHARE [provider-account].CHIPMUNK_UDF_DEMO_SHARE;
GRANT IMPORTED PRIVILEGES ON DATABASE CHIPMUNK_UDF_TEST TO ROLE PUBLIC;
USE DATABASE CHIPMUNK_UDF_TEST;
SELECT * FROM TABLE(CHIPMUNK_UDF_TEST.PUBLIC.GET_MARKET_BASKET(6139));
-- CONSUMER --


-- 17.7.5  Examine Share from the data provider.
--         Perform this step on the [provider-account] in browser 1.

-- PROVIDER --
DESCRIBE SHARE CHIPMUNK_UDF_DEMO_SHARE;
-- PROVIDER --

--         The secure UDF is essentially using the data access rights of its
--         creator, but allowing itself to be run by another Snowflake account
--         that has access rights to run it. With Snowflake Data Sharing, the
--         compute processing for secure UDFs runs in the context of, and is
--         paid for by, the data consumer using the consumer’s virtual
--         warehouse, against the function provider’s single encrypted copy of
--         the underlying data.
--         This ability to share a secure UDF enables a myriad of secure data
--         sharing and data monetization use cases, including the ability to
--         share raw and aggregated data and powerful analytical functions,
--         while also protecting the secure UDF’s code. It also prevents other
--         parties from directly viewing or exporting the underlying encrypted
--         data.
