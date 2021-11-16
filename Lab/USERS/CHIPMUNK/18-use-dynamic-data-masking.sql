
-- 18.0.0  Use Dynamic Data Masking
--         This lab will take approximately 20 minutes to complete.
--         A membership program needs to develop an analytic application using
--         data which contains Personally Identifiable Information (PII). The
--         production environment will need to provide the necessary restricted
--         access. In order to develop the application, the development team
--         requires a development environment to build, code and test the
--         application against the PII data.
--         In this lab, you will create a development environment that securely
--         provides the required production data to the development team using
--         Snowflake Dynamic Data Masking.

-- 18.1.0  Identify PII Data
--         The table TRAINING_DB.TRAININGLAB.MEMBERS, which you will use for
--         this lab, contains PII data.

-- 18.1.1  Query the table.

USE ROLE training_role; 
USE WAREHOUSE CHIPMUNK_wh;
USE DATABASE training_db;
USE SCHEMA traininglab;

DESCRIBE TABLE members;

SELECT firstname, lastname, age, email FROM members LIMIT 10; 

--         By examining the columns names and their values it is clear that the
--         MEMBERS table contains multiple columns of PII data.

-- 18.1.2  Create a new database using CLONE.
--         To prepare the development environment, create a new schema and clone
--         the MEMBERS table to it.

CREATE OR REPLACE SCHEMA CHIPMUNK_db.mask_lab;
USE SCHEMA CHIPMUNK_db.mask_lab;

CREATE OR REPLACE TABLE members CLONE training_db.traininglab.members;

--         This created a MEMBERS table in the new schema, but changed the
--         ownership of the table to TRAINING_ROLE (the role that created it).

-- 18.2.0  Create Masking Policies

-- 18.2.1  Create a masking policy for customer names.
--         By default, only SYSADMIN and ACCOUNTADMIN can work with masking
--         policies.

USE ROLE SYSADMIN;
CREATE OR REPLACE MASKING POLICY CHIPMUNK_db.mask_lab.name_mask AS
(val VARCHAR) RETURNS VARCHAR ->
  CASE
    WHEN current_role() IN ('SYSADMIN') THEN regexp_replace(val,'.','*',2)
    WHEN current_role() IN ('TRAINING_ROLE') THEN val
    ELSE '*** REDACTED ***'
    END;


-- 18.2.2  Create a masking policy for email addresses.

CREATE OR REPLACE MASKING POLICY CHIPMUNK_db.mask_lab.email_mask AS(val STRING) RETURNS STRING ->
  CASE
    WHEN current_role() IN ('TRAINING_ROLE') THEN val
    WHEN current_role() IN ('SYSADMIN') THEN regexp_replace(val,'.+\@','*****@')
    ELSE '*** REDACTED ***'
  END;


-- 18.2.3  Create a masking policy for age.

CREATE OR REPLACE MASKING POLICY CHIPMUNK_db.mask_lab.age_mask AS(val INTEGER) RETURNS INTEGER ->
  CASE
    WHEN current_role() IN ('TRAINING_ROLE') THEN val
      ELSE null
  END;


-- 18.3.0  Use Data Masking Policies

-- 18.3.1  Set masking policies on the firstname, lastname, email and age
--         columns of the MEMBERS table.

ALTER TABLE members MODIFY COLUMN firstname SET MASKING POLICY name_mask;
ALTER TABLE members MODIFY COLUMN lastname SET MASKING POLICY name_mask;
ALTER TABLE members MODIFY COLUMN email SET MASKING POLICY email_mask;
ALTER TABLE members MODIFY COLUMN age SET MASKING POLICY age_mask;


-- 18.3.2  As TRAINING_ROLE, view masking policy metadata using the SHOW and
--         DESCRIBE commands.

USE ROLE TRAINING_ROLE;

SHOW MASKING POLICIES;


-- 18.3.3  DESCRIBE each masking policy.

DESC MASKING POLICY name_mask;
DESC MASKING POLICY age_mask;
DESC MASKING POLICY email_mask;


-- 18.3.4  View grants on masking policies.

SHOW GRANTS ON MASKING POLICY name_mask;
SHOW GRANTS ON MASKING POLICY age_mask;
SHOW GRANTS ON MASKING POLICY email_mask;


-- 18.4.0  Test Masking Policies

-- 18.4.1  Run table queries as the role TRAINING_ROLE.

SELECT firstname, lastname, age, email FROM members LIMIT 10;

--         TRAINING_ROLE should be able to see the unmasked data for all
--         columns.

-- 18.4.2  Run table queries as the role SYSADMIN.

USE ROLE SYSADMIN;

SELECT firstname, lastname, age, email FROM members LIMIT 10;

--         SYSADMIN should see the first letter of the first and last names, the
--         domain for the email addresses, and NULL for age.

-- 18.4.3  Grant object privileges to the PUBLIC role.

GRANT USAGE ON WAREHOUSE CHIPMUNK_wh TO ROLE PUBLIC;
GRANT USAGE ON DATABASE CHIPMUNK_db TO ROLE PUBLIC;
GRANT USAGE ON SCHEMA CHIPMUNK_db.mask_lab TO ROLE PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA CHIPMUNK_db.mask_lab TO ROLE PUBLIC;


-- 18.4.4  Run table queries as the PUBLIC role.

USE ROLE PUBLIC;
USE WAREHOUSE CHIPMUNK_wh;

SELECT firstname, lastname, age, email FROM members LIMIT 10;

--         The PUBLIC role should see NULL for the age column, and REDACTED or
--         NULL for everything else.
