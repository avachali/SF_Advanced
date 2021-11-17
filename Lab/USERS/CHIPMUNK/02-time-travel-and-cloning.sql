
-- 2.0.0   Time Travel and Cloning
--         This lab will take approximately 15 minutes to complete.

-- 2.1.0   Drop and Undrop Objects

-- 2.1.1   Use the following commands to set up the objects for this lab.

USE ROLE TRAINING_ROLE;
CREATE DATABASE IF NOT EXISTS CHIPMUNK_CLONE_DB;
CREATE OR REPLACE SCHEMA CHIPMUNK_lab;
USE SCHEMA CHIPMUNK_lab;
CREATE TABLE nation CLONE training_db.traininglab.nation;

-- 2.1.2   Use the UNDROP command on a table.

SHOW TABLES IN SCHEMA CHIPMUNK_lab;

DROP TABLE CHIPMUNK_lab.NATION;

SHOW TABLES IN SCHEMA CHIPMUNK_lab;

UNDROP TABLE NATION;

SHOW TABLES IN SCHEMA CHIPMUNK_lab;


-- 2.1.3   Use the UNDROP command on a schema.

SHOW SCHEMAS IN database CHIPMUNK_CLONE_DB;

DROP SCHEMA CHIPMUNK_lab;
SHOW SCHEMAS IN database CHIPMUNK_CLONE_DB;

UNDROP SCHEMA CHIPMUNK_lab;
SHOW SCHEMAS IN database CHIPMUNK_CLONE_DB;


-- 2.1.4   Use the UNDROP command on a database.

SHOW DATABASES STARTS WITH 'CHIPMUNK';

DROP database CHIPMUNK_CLONE_DB;
SHOW DATABASES STARTS WITH 'CHIPMUNK';

UNDROP database CHIPMUNK_CLONE_DB;
SHOW DATABASES STARTS WITH 'CHIPMUNK';


-- 2.2.0   Explore Time Travel
--         In an earlier lab, you used Time Travel with the CLONE command. In
--         this task, you will explore its use in a query statement.

-- 2.2.1   Set your context.

USE DATABASE CHIPMUNK_CLONE_DB;
USE SCHEMA CHIPMUNK_lab;
USE WAREHOUSE CHIPMUNK_wh;
ALTER WAREHOUSE CHIPMUNK_wh set warehouse_size=small;



-- 2.2.2   Make a change to a table, and save the query ID.

SELECT N_NATIONKEY, N_NAME, N_REGIONKEY, N_COMMENT 
  FROM NATION;
  
UPDATE NATION 
  SET N_NAME ='ERROR'
  WHERE N_NATIONKEY=1;

SET un = last_query_id();


-- 2.2.3   Select from the table before and after you made the change.

SELECT 
  (SELECT N_NAME FROM NATION BEFORE(STATEMENT => $un)
  WHERE N_NATIONKEY=1) ORIGINAL,
  (SELECT N_NAME FROM NATION WHERE N_NATIONKEY=1) "CURRENT";


-- 2.2.4   Recover the table to its previous state using a clone.
--         Often you use the CLONE command as part of agile development. It also
--         has a role in data recovery or undoing mistakes.
--         Restore the NATION table to the state it was in prior to the update
--         you made in the previous exercise:

SHOW TABLES STARTS WITH 'NATION';

CREATE TABLE NATION_RESTORED CLONE NATION BEFORE(STATEMENT => $un);

SELECT * FROM nation_restored;

SELECT * FROM nation;

ALTER TABLE NATION SWAP WITH NATION_RESTORED;

SELECT (SELECT N_NAME FROM nation_restored
                WHERE  N_NATIONKEY=1) ERROR,
      (SELECT N_NAME FROM nation
                WHERE  N_NATIONKEY=1) RESTORED;

