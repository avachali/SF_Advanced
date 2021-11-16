
-- 19.0.0  Database Replication and Disaster Recovery
--         This lab will take approximately 40 minutes to complete.
--         In this exercise you will learn how to set up two Snowflake accounts
--         for replication, create a primary database, and perform the initial
--         replication of this primary database to a secondary database on
--         another account.
--         Database Replication Diagram
--         You will also perform the steps necessary to fail over to the
--         secondary account for disaster recovery.
--         Database Failover Diagram
--         Before you can configure database replication, two or more accounts
--         must be linked to an organization. The instructor will provide the
--         Primary and Secondary account URLs for this exercise.

-- 19.1.0  Set up Browsers for Database Replication

-- 19.1.1  Open two browser windows side-by-side.
--         You could also open two tabs, but the exercise is easier to complete
--         if you can see both browser windows at the same time.

-- 19.1.2  In browser 1, enter the Primary account URL (provided by your
--         instructor) and log in with your assigned credentials.

-- 19.1.3  Navigate to worksheets and load the script for this lab.

-- 19.1.4  Rename the worksheet to PRIMARY.
--         In the PRIMARY worksheet, you will only execute statements that are
--         surrounded by the PRIMARY comments, as shown below.

-- PRIMARY --
-- SQL statement 1;
-- SQL statement 2;
-- PRIMARY --


-- 19.1.5  Determine the region and account locator name for your primary
--         account.

-- PRIMARY --
SELECT CURRENT_REGION() AS "primary.region", 
       CURRENT_ACCOUNT() AS "primary.account_locator";
-- PRIMARY --


-- 19.1.6  In browser 2, enter the Secondary account URL and log in with your
--         assigned credentials.

-- 19.1.7  Navigate to worksheets and load the script for this lab.

-- 19.1.8  Rename the worksheet to SECONDARY.
--         In the SECONDARY worksheet, you will only execute statements that are
--         surrounded by the SECONDARY comments, as shown below.

-- SECONDARY --
-- SQL command 1;
-- SQL command 2;
-- SECONDARY --


-- 19.1.9  Determine the region and account locator names for your secondary
--         account.

-- SECONDARY --
SELECT CURRENT_REGION() AS "secondary.region", 
    CURRENT_ACCOUNT() AS "secondary.account_locator";
-- SECONDARY --


-- 19.1.10 Examine the results.
--         Browser 1 - Primary Account & Browser 2 - Secondary Account
--         The results in Browser 1 show the primary.region and
--         primary.account_locator. The results in Browser 2 show the
--         secondary.region and secondary.account_locator.
--         For this lab, the primary and secondary accounts are in the same
--         region. However, you can have the secondary account on a different
--         cloud provider, or in a different region, from the primary account.

-- 19.2.0  Set Account Locator and Region Names
--         The SQL commands in this lab use an account identifier in the format,
--         snowflake_region.account_locator. However, be aware account
--         identifiers in the format snowflake_region.account_locator are also
--         supported. For information about account identifiers, see Account
--         Identifiers.

-- 19.2.1  Find and replace account locator and region names.
--         This lab contains placeholders for the primary and secondary account
--         locators and regions, because those values will be different for
--         every class. Before continuing, you need to replace those
--         placeholders with the correct names.
--         To do this, use the find/replace feature in the UI. The keyboard
--         shortcut for this feature is:
--         On MacOS: CMD+OPT+F On Windows: SHIFT+CTRL+F
--         Using the find/replace feature, find ALL instances of the following
--         placeholders, and replace them with the values returned by the
--         commands you ran on each account
--         Replace [PRIMARY-REGION] with the region for the primary account.
--         Replace [PRIMARY-ACCOUNT-LOCATOR] with the account locator for the
--         primary account.
--         Replace [SECONDARY-REGION] with the region for the secondary account.
--         Replace [SECONDARY-ACCOUNT-LOCATOR] with the account name for the
--         secondary account.
--         Perform the find/replace this in the script in BOTH the secondary and
--         primary accounts.

-- 19.3.0  Set Up the Primary Database

-- 19.3.1  On the PRIMARY account, create a database and objects to replicate.

-- PRIMARY --
USE ROLE TRAINING_ROLE;

CREATE WAREHOUSE IF NOT EXISTS CHIPMUNK_REPL_WH  
   WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 300;
CREATE DATABASE IF NOT EXISTS CHIPMUNK_REPL_DB;
CREATE SCHEMA IF NOT EXISTS REPL_SCHEMA;
USE CHIPMUNK_REPL_DB.REPL_SCHEMA;

-- create a table with 1000 rows 
CREATE OR REPLACE TABLE MARKETING_A
    ( CUST_NUMBER INT, CUST_NAME CHAR(50), CUST_ADDRESS VARCHAR(100), 
      CUST_PURCHASE_DATE DATE ) CLUSTER BY (CUST_PURCHASE_DATE)
AS (  SELECT UNIFORM(1,999,RANDOM(10002)), 
             UUID_STRING(), 
             UUID_STRING(), 
             CURRENT_DATE 
      FROM TABLE(GENERATOR(ROWCOUNT => 1000))
);

-- create a procedure to insert 100 rows into the table
CREATE OR REPLACE PROCEDURE INSERT_MARKETING_ROWS()
RETURNS VARCHAR NOT NULL
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
var result = "";
try {
    var sql_command = 
        "INSERT INTO MARKETING_A SELECT UNIFORM(1,999,RANDOM(10002)), UUID_STRING(), UUID_STRING(), CURRENT_DATE FROM TABLE(GENERATOR(ROWCOUNT => 100))"
    stmt = snowflake.createStatement(
        {sqlText: sql_command});
    rs = stmt.execute();
    }
catch (err) {
    result =  "Failed: Code: " + err.code + "\n  State: " + err.state;
    result += "\n  Message: " + err.message;
    result += "\nStack Trace:\n" + err.stackTraceTxt; 
    }
return result;
$$
;
-- PRIMARY --


-- 19.3.2  View the primary account identifiers.

-- PRIMARY --
USE ROLE ACCOUNTADMIN;

SHOW  REPLICATION ACCOUNTS LIKE '[PRIMARY-ACCOUNT-LOCATOR]'; 
-- PRIMARY --

--         Examine the results. Note the ACCOUNT_LOCATOR values and confirm that
--         they match the Primary account URL provided by your instructor.
--         Show Replication Account Results
--         Note each value of the SNOWFLAKE_REGION, ACCOUNT_LOCATOR,
--         ORGANIZATION_NAME and ACCOUNT_NAME columns to determine the Primary
--         account identifiers are in one of the following two formats:
--         org_name.account_name or snowflake_region.account_locator.

-- 19.3.3  View the secondary account identifiers.

-- SECONDARY --
USE ROLE ACCOUNTADMIN;

SHOW  REPLICATION ACCOUNTS LIKE '[SECONDARY-ACCOUNT-LOCATOR]'; 
-- SECONDARY --

--         Examine the results. Note the ACCOUNT_LOCATOR values and confirm that
--         they match the Secondary account URL provided by your instructor.
--         Note each value of the SNOWFLAKE_REGION, ACCOUNT_LOCATOR,
--         ORGANIZATION_NAME and ACCOUNT_NAME columns to determine the Secondary
--         account identifiers are in one of the following two formats:
--         org_name.account_name or snowflake_region.account_locator.

-- 19.3.4  Promote CHIPMUNK_REPL_DB to serve as a primary database.

-- PRIMARY --
ALTER DATABASE CHIPMUNK_REPL_DB ENABLE REPLICATION 
    TO ACCOUNTS [SECONDARY-REGION].[SECONDARY-ACCOUNT-LOCATOR];
-- PRIMARY --


-- 19.3.5  Examine the results of the SHOW REPLICATION DATABASES statement.

-- PRIMARY --
SHOW REPLICATION DATABASES; 
-- PRIMARY --

--         Verify that IS_PRIMARY is TRUE and that the
--         REPLICATION_ALLOWED_TO_ACCOUNTS column contains both the primary and
--         secondary account identifiers in one of the the following two
--         formats: org_name.account_name or snowflake_region.account_locator.

-- 19.3.6  Enable the ability to fail over from the primary to the secondary
--         account.

-- PRIMARY --
ALTER DATABASE CHIPMUNK_REPL_DB ENABLE FAILOVER 
    TO ACCOUNTS [SECONDARY-REGION].[SECONDARY-ACCOUNT-LOCATOR];
-- PRIMARY --


-- 19.3.7  Examine the results of the SHOW REPLICATION DATABASES statement.

-- PRIMARY --
SHOW REPLICATION DATABASES; 
-- PRIMARY --

--         Verify that the FAILOVER_ALLOWED_TO_ACCOUNTS column contains both the
--         primary and secondary account identifiers in one of the following two
--         formats: org_name.account_name or snowflake_region.account_locator.

-- 19.4.0  Creation and Replication To the Secondary Database
--         In this exercise, you will perform the steps to create the secondary
--         database, and replicate the database from the primary account to the
--         secondary account.
--         Initial Database Replica

-- 19.4.1  Create a replica database on the secondary account.
--         This step creates an empty database with the same structure as the
--         database on the primary account. No data will be transferred during
--         this step.

-- SECONDARY --
CREATE DATABASE CHIPMUNK_REPL_DB AS REPLICA 
    OF [PRIMARY-REGION].[PRIMARY-ACCOUNT-LOCATOR].CHIPMUNK_REPL_DB;

SHOW REPLICATION DATABASES;
-- SECONDARY --

--         Examine the results. Check to see that is_primary is FALSE for the
--         secondary account.
--         Show Replication Database Results

-- 19.4.2  Transfer ownership of the replica objects to TRAINING_ROLE.

-- SECONDARY --
GRANT OWNERSHIP ON DATABASE CHIPMUNK_REPL_DB TO ROLE TRAINING_ROLE;
GRANT OWNERSHIP ON SCHEMA CHIPMUNK_REPL_DB.PUBLIC TO ROLE TRAINING_ROLE;
-- SECONDARY --


-- 19.4.3  Start the initial replication.
--         Replication is a pull operation, so the process is initiated on the
--         secondary account.

-- SECONDARY --
USE ROLE TRAINING_ROLE;
ALTER DATABASE CHIPMUNK_REPL_DB REFRESH;
-- SECONDARY --


-- 19.4.4  Query the secondary database to verify the replication has completed.

-- SECONDARY --
-- verify data and objects 
CREATE WAREHOUSE IF NOT EXISTS CHIPMUNK_REPL_WH 
    WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 300;
USE DATABASE CHIPMUNK_REPL_DB;
USE SCHEMA REPL_SCHEMA;

SELECT COUNT(CUST_NUMBER) FROM MARKETING_A;

SHOW TABLES;
SHOW PROCEDURES;
-- SECONDARY --

--         The COUNT(cust_number) value should be 1000.

-- 19.4.5  Verify that the replica is read-only.

-- SECONDARY --
USE WAREHOUSE CHIPMUNK_REPL_WH;
CALL INSERT_MARKETING_ROWS();
-- SECONDARY --

--         When the primary database is replicated, a snapshot of its database
--         objects and data is transferred to the secondary database.

-- 19.5.0  Monitor Replication
--         In this exercise, you will perform the steps to determine the current
--         status of the initial database replication or a subsequent secondary
--         database refresh.

-- 19.5.1  Set your context.

-- SECONDARY --
USE ROLE TRAINING_ROLE;
USE WAREHOUSE CHIPMUNK_REPL_WH;
USE DATABASE CHIPMUNK_REPL_DB;
USE SCHEMA REPL_SCHEMA;
-- SECONDARY --


-- 19.5.2  Monitor the database refresh progress.

-- SECONDARY --
-- show the steps for the latest refresh on the database, in seconds
SELECT 
    PHASE_NAME, 
    RESULT, 
    START_TIME,
    END_TIME, 
    DATEDIFF(SECOND, START_TIME, END_TIME) AS DURATION, 
    DETAILS
FROM TABLE(INFORMATION_SCHEMA.DATABASE_REFRESH_PROGRESS('CHIPMUNK_REPL_DB')); 

-- show the steps for the latest refresh on the database, in minutes
SELECT value:phaseName::string as Phase,
    value:resultName::string as Result,
    to_timestamp_ltz(value:startTimeUTC::numeric,3) as startTime,
    to_timestamp_ltz(value:endTimeUTC::numeric,3) as endTime,
    datediff(mins, startTime, endTime) as Minutes
FROM TABLE (flatten(input=>parse_json(
    SYSTEM$database_refresh_progress('CHIPMUNK_REPL_DB'))));
-- SECONDARY --


-- 19.5.3  Monitor the database refresh history.

-- SECONDARY --
SELECT *
FROM TABLE(INFORMATION_SCHEMA.DATABASE_REFRESH_HISTORY('CHIPMUNK_REPL_DB')); 
-- SECONDARY --

--         You can also monitor database refresh progress by job id, by
--         providing the value of the JOB_UUID column from the
--         database_refresh_history to investigate to a specific refresh in the
--         last 14 days.

-- SECONDARY --
SELECT 
    PHASE_NAME,
    RESULT,
    START_TIME,
    END_TIME,
    DATEDIFF(SECOND, START_TIME, END_TIME) AS DURATION
FROM TABLE(INFORMATION_SCHEMA.DATABASE_REFRESH_PROGRESS_BY_JOB
    ('<JOB_UUID_VALUE_FROM_REFRESH_HISTORY_QUERY>')); 
-- SECONDARY --


-- 19.6.0  Schedule Automatic Refreshes of the Replica
--         In the previous exercise, you learned how to manually perform the
--         initial refresh and validated the replication between the primary and
--         secondary databases. As a best practice, Snowflake recommends
--         scheduling your secondary database refreshes. In this exercise you
--         will perform the steps for starting a database refresh automatically
--         on a specified schedule.

-- 19.6.1  Create a database on the secondary account where the task will be
--         created.

-- SECONDARY --
USE ROLE TRAINING_ROLE;

CREATE DATABASE IF NOT EXISTS CHIPMUNK_DB;
CREATE SCHEMA IF NOT EXISTS TASKS;
USE DATABASE CHIPMUNK_DB;
USE SCHEMA TASKS;

-- create a task to refresh on a regular basis
CREATE OR REPLACE TASK CHIPMUNK_REPL_DB_REFRESH_TASK  
    WAREHOUSE = CHIPMUNK_REPL_WH 
    SCHEDULE = '1 MINUTE'
AS ALTER DATABASE CHIPMUNK_REPL_DB REFRESH;
-- SECONDARY --


-- 19.6.2  Start the task
--         After creating a task, you must RESUME the task before it will run.

-- SECONDARY --
SHOW TASKS;

ALTER TASK CHIPMUNK_REPL_DB_REFRESH_TASK RESUME;

SHOW TASKS;
-- SECONDARY --


-- 19.6.3  Monitor the task history and the database refresh history.

-- SECONDARY --
-- monitor task history
USE WAREHOUSE CHIPMUNK_REPL_WH;
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) 
  WHERE DATABASE_NAME LIKE UPPER('CHIPMUNK_DB');

-- monitor database refresh history
-- LOOK AT ALL REFRESH OPERATIONS FOR THIS DB IN LAST 14 DAYS
SELECT *
   FROM TABLE(INFORMATION_SCHEMA.DATABASE_REFRESH_HISTORY('CHIPMUNK_REPL_DB')); 
-- SECONDARY --


-- 19.6.4  Examine the results. This example here shows results after about 5
--         minutes.
--         Monitor Database Refresh History Results

-- 19.7.0  Verify that a Refresh Picks Up Changes

-- 19.7.1  Insert new rows into the primary database.

-- PRIMARY --
USE ROLE TRAINING_ROLE;
USE DATABASE CHIPMUNK_REPL_DB;
USE SCHEMA REPL_SCHEMA;

CALL INSERT_MARKETING_ROWS();
SELECT COUNT(CUST_NUMBER) FROM MARKETING_A;
CALL INSERT_MARKETING_ROWS();
SELECT COUNT(CUST_NUMBER) FROM MARKETING_A;
-- PRIMARY --

--         COUNT(cust_number) now has the value of 1200.

-- 19.7.2  Check that the secondary database was updated.
--         The task to refresh the secondary database runs every minute. Wait a
--         minute or so, then use the command below to check the refresh history
--         until you see the new refresh operation start, and then complete.

-- SECONDARY --
-- check the database refresh history
SELECT *
  FROM TABLE(INFORMATION_SCHEMA.DATABASE_REFRESH_HISTORY('CHIPMUNK_REPL_DB')); 

-- count the rows in the table
USE WAREHOUSE CHIPMUNK_REPL_WH;
USE DATABASE CHIPMUNK_REPL_DB;
USE SCHEMA REPL_SCHEMA;

SELECT COUNT(CUST_NUMBER) FROM MARKETING_A;
-- SECONDARY --

--         The COUNT(cust_number) will have a value of 1200 after the refresh
--         completes.

-- 19.7.3  Suspend the Task

-- SECONDARY --
USE DATABASE CHIPMUNK_DB;
USE SCHEMA TASKS;

SHOW TASKS;

ALTER TASK CHIPMUNK_REPL_DB_REFRESH_TASK SUSPEND;

SHOW TASKS;
-- SECONDARY --


-- 19.8.0  Change Replication Direction
--         In this exercise, you will perform the steps to promote the secondary
--         database to act as the primary. When promoted, the secondary database
--         becomes writeable. At the same time, the previous primary database
--         becomes a read-only replica database.
--         Changing Replication Direction

-- 19.8.1  Promote the secondary database.

-- SECONDARY --
-- view replication databases
SHOW REPLICATION DATABASES;

-- fail over to the secondary database
ALTER DATABASE CHIPMUNK_REPL_DB PRIMARY;
-- SECONDARY --


-- 19.8.2  Verify the database on the secondary account is now the primary.

-- SECONDARY --
SHOW REPLICATION DATABASES;
-- SECONDARY --

--         Check to see that is_primary is TRUE for the secondary account.
--         Show replication databases results

-- 19.8.3  Verify that the replica database is now writeable.

-- SECONDARY --
-- verify database can be written to
USE ROLE TRAINING_ROLE;
USE WAREHOUSE CHIPMUNK_REPL_WH;
USE DATABASE CHIPMUNK_REPL_DB;
USE SCHEMA REPL_SCHEMA;

CALL INSERT_MARKETING_ROWS();
SELECT COUNT(CUST_NUMBER) FROM MARKETING_A;
CALL INSERT_MARKETING_ROWS();
SELECT COUNT(CUST_NUMBER) FROM MARKETING_A;

-- Drop a column from the table
ALTER TABLE MARKETING_A DROP COLUMN CUST_ADDRESS;

DESC TABLE MARKETING_A;

-- recreate the stored procedure without the column that you just dropped
CREATE OR REPLACE PROCEDURE INSERT_MARKETING_ROWS()
RETURNS VARCHAR NOT NULL
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
var result = "";
try {
    var sql_command = 
        "INSERT INTO MARKETING_A SELECT UNIFORM(1,999,RANDOM(10002)),UUID_STRING(), CURRENT_DATE FROM TABLE(GENERATOR(ROWCOUNT => 100))"
    stmt = snowflake.createStatement(
        {sqlText: sql_command});
    rs = stmt.execute();
    }
catch (err) {
    result =  "Failed: Code: " + err.code + "\n  State: " + err.state;
    result += "\n  Message: " + err.message;
    result += "\nStack Trace:\n" + err.stackTraceTxt; 
    }
return result;
$$
;

CALL INSERT_MARKETING_ROWS();

SELECT COUNT(CUST_NUMBER) FROM MARKETING_A;

-- SECONDARY --

--         The COUNT(cust_number) value is now 1500.

-- 19.8.4  Refresh the new secondary database.

-- PRIMARY --
USE ROLE TRAINING_ROLE;
USE DATABASE CHIPMUNK_REPL_DB;
USE SCHEMA REPL_SCHEMA;

SELECT COUNT(CUST_NUMBER) FROM MARKETING_A;

ALTER DATABASE CHIPMUNK_REPL_DB REFRESH;

-- Wait a minute or so to give the refresh time to complete

DESC TABLE MARKETING_A;

SELECT COUNT(CUST_NUMBER) FROM MARKETING_A;
-- PRIMARY --

--         You should see the record count increase from 1200 before the
--         REFRESH, to 1500 after the REFRESH.

-- 19.9.0  Clean Up

-- 19.9.1  Run the following statements on the primary account.

-- PRIMARY --
USE ROLE TRAINING_ROLE;
DROP DATABASE CHIPMUNK_REPL_DB;
DROP WAREHOUSE CHIPMUNK_REPL_WH;
-- PRIMARY --


-- 19.9.2  Run the following statements on the secondary account.

-- SECONDARY --
USE ROLE TRAINING_ROLE;
DROP DATABASE CHIPMUNK_REPL_DB;
DROP SCHEMA CHIPMUNK_db.TASKS; 
DROP WAREHOUSE CHIPMUNK_REPL_WH;
-- SECONDARY --

