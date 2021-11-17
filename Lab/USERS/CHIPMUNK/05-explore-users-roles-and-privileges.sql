
-- 5.0.0   Explore Users, Roles, and Privileges
--         This lab will take approximately 25 minutes to complete.
--         This exercise will demonstrate the separation of functional (user)
--         roles and object roles. In this lab you will create four functional
--         roles and four object roles, to create the hierarchy shown here:
--         Role Diagram

-- 5.1.0   Create and Configure Roles

-- 5.1.1   Start by creating a database and schema you will use for this lab.

USE ROLE SYSADMIN;

CREATE DATABASE if not exists CHIPMUNK_prod;
CREATE or replace SCHEMA CHIPMUNK_prod.sales;


-- 5.1.2   Create the functional roles.

USE ROLE SECURITYADMIN;

CREATE ROLE CHIPMUNK_dba;
CREATE ROLE CHIPMUNK_dev;
CREATE ROLE CHIPMUNK_elt;
CREATE ROLE CHIPMUNK_analyst;


-- 5.1.3   Grant each of these roles to SYSADMIN, to create the first part of
--         the hierarchy.

GRANT ROLE CHIPMUNK_dba TO ROLE SYSADMIN; 
GRANT ROLE CHIPMUNK_dev TO ROLE SYSADMIN; 
GRANT ROLE CHIPMUNK_elt TO ROLE SYSADMIN; 
GRANT ROLE CHIPMUNK_analyst TO ROLE SYSADMIN; 


-- 5.1.4   Assign your user to each of these roles.
--         In an actual deployment, each role would probably be granted to
--         multiple users, and each role would have a different set of users.

GRANT ROLE CHIPMUNK_dba
           ,CHIPMUNK_dev
           ,CHIPMUNK_elt
           ,CHIPMUNK_analyst TO USER CHIPMUNK; 


-- 5.1.5   Create the object roles, which will be assigned privileges to access
--         objects.

CREATE ROLE CHIPMUNK_read;
CREATE ROLE CHIPMUNK_ins_del;
CREATE ROLE CHIPMUNK_create_objects; 
CREATE ROLE CHIPMUNK_manage_db;


-- 5.1.6   Link the object roles together so privileges flow up through those
--         roles.

GRANT ROLE CHIPMUNK_read TO ROLE CHIPMUNK_ins_del;
GRANT ROLE CHIPMUNK_ins_del TO ROLE CHIPMUNK_create_objects;
GRANT ROLE CHIPMUNK_create_objects TO ROLE CHIPMUNK_manage_db;


-- 5.1.7   Grant the object roles to the appropriate functional roles.
--         Note that each object role is granted to a functional role (to define
--         what that functional role can do), and to another object role (to
--         build the hierarchy between object roles).

GRANT ROLE CHIPMUNK_read TO ROLE CHIPMUNK_analyst;
GRANT ROLE CHIPMUNK_ins_del TO ROLE CHIPMUNK_elt;
GRANT ROLE CHIPMUNK_create_objects TO ROLE CHIPMUNK_dev;
GRANT ROLE CHIPMUNK_manage_db TO ROLE CHIPMUNK_dba;


-- 5.1.8   Grant USAGE Privileges to Object Roles

USE ROLE SYSADMIN;

GRANT USAGE ON DATABASE CHIPMUNK_prod to ROLE CHIPMUNK_read;

GRANT USAGE ON ALL SCHEMAS IN DATABASE CHIPMUNK_prod to ROLE CHIPMUNK_read;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE CHIPMUNK_prod to ROLE CHIPMUNK_read;

--         Because the READ role’s privileges are rolled up the hierarchy, all
--         object roles now have USAGE privileges on the database CHIPMUNK_PROD,
--         the schema SALES, and all future schemas in that database.

-- 5.1.9   Grant object privileges to the object roles.
--         Keep in mind that privileges granted to one role will roll up to the
--         parent roles - so you only have to grant privileges at the lowest
--         level you want them to be available. Also, there are not yet any
--         objects inside the schema SALES, so you only have to grant privileges
--         on future objects.

GRANT SELECT ON FUTURE TABLES IN DATABASE CHIPMUNK_prod TO ROLE CHIPMUNK_read;
GRANT SELECT ON FUTURE VIEWS IN DATABASE CHIPMUNK_prod TO ROLE CHIPMUNK_read;


-- 5.1.10  Grant privileges to the INS_DEL role.

GRANT INSERT, UPDATE, DELETE ON FUTURE TABLES IN DATABASE CHIPMUNK_prod
  TO ROLE CHIPMUNK_ins_del;


-- 5.1.11  Grant privileges to the CREATE_OBJECTS role.
--         Since a schema already exists, you will need to grant privileges on
--         the current schema (SALES), and any future schemas you want the role
--         to have access to.

GRANT CREATE TABLE
     ,CREATE VIEW
     ,CREATE FILE FORMAT
     ,CREATE STAGE
     ,CREATE PIPE
     ,CREATE SEQUENCE
     ,CREATE FUNCTION
     ,CREATE PROCEDURE
  ON ALL SCHEMAS IN DATABASE CHIPMUNK_prod TO ROLE CHIPMUNK_create_objects;

GRANT CREATE TABLE
     ,CREATE VIEW
     ,CREATE FILE FORMAT
     ,CREATE STAGE
     ,CREATE PIPE
     ,CREATE SEQUENCE
     ,CREATE FUNCTION
     ,CREATE PROCEDURE
  ON FUTURE SCHEMAS IN DATABASE CHIPMUNK_prod TO ROLE CHIPMUNK_create_objects;


-- 5.1.12  Grant privileges to the MANAGE_DB role.

GRANT MODIFY
     ,MONITOR
     ,CREATE SCHEMA
ON DATABASE CHIPMUNK_prod 
TO ROLE CHIPMUNK_manage_db;


-- 5.2.0   Verify the Current Hierarchy

-- 5.2.1   Verify that you are assigned to the functional roles, and that the
--         functional roles have been granted to the role SYSADMIN.

USE ROLE SECURITYADMIN;

SHOW GRANTS OF ROLE CHIPMUNK_dba;
SHOW GRANTS OF ROLE CHIPMUNK_analyst;
SHOW GRANTS OF ROLE CHIPMUNK_elt;
SHOW GRANTS OF ROLE CHIPMUNK_dev;


-- 5.2.2   Verify that you are NOT assigned to the object roles.
--         In your design, users are only assigned to the functional roles. Each
--         object role should be assigned to one or more functional roles.

SHOW GRANTS OF ROLE CHIPMUNK_read;
SHOW GRANTS OF ROLE CHIPMUNK_ins_del;
SHOW GRANTS OF ROLE CHIPMUNK_create_objects;
SHOW GRANTS OF ROLE CHIPMUNK_manage_db;


-- 5.2.3   Verify the grants on the CHIPMUNK_PROD database.

SHOW GRANTS ON DATABASE CHIPMUNK_prod;


-- 5.3.0   Create and Grant Privileges on Warehouses

-- 5.3.1   Create the warehouses.

USE ROLE SYSADMIN;

CREATE WAREHOUSE IF NOT EXISTS CHIPMUNK_elt_wh
  WAREHOUSE_SIZE=XSmall
  INITIALLY_SUSPENDED=TRUE
  AUTO_SUSPEND=60;

CREATE WAREHOUSE IF NOT EXISTS CHIPMUNK_small_query_wh
   WAREHOUSE_SIZE=Small
   INITIALLY_SUSPENDED=TRUE
   AUTO_SUSPEND=600;

CREATE WAREHOUSE IF NOT EXISTS CHIPMUNK_large_query_wh
   WAREHOUSE_SIZE=Large
   INITIALLY_SUSPENDED=TRUE
   AUTO_SUSPEND=1200;

CREATE WAREHOUSE IF NOT EXISTS CHIPMUNK_mc_wh
   WAREHOUSE_SIZE=Medium
   INITIALLY_SUSPENDED=TRUE
   MIN_CLUSTER_COUNT=1
   MAX_CLUSTER_COUNT=8
   AUTO_SUSPEND=300;


-- 5.3.2   Grant privileges to the appropriate roles.

GRANT USAGE ON WAREHOUSE CHIPMUNK_elt_wh TO ROLE CHIPMUNK_ins_del;
GRANT USAGE ON WAREHOUSE CHIPMUNK_small_query_wh TO ROLE CHIPMUNK_read;
GRANT USAGE ON WAREHOUSE CHIPMUNK_large_query_wh TO ROLE CHIPMUNK_read;
GRANT USAGE ON WAREHOUSE CHIPMUNK_mc_wh TO ROLE CHIPMUNK_read;


-- 5.4.0   Explore and Test Ownership and Privileges
--         Ownership is a privilege that is automatically granted to the role
--         that creates an object. Ownership can be transferred to another role,
--         but only by the current owner or the ACCOUNTADMIN role.

-- 5.4.1   Check ownership of the SALES schema.

SHOW SCHEMAS IN DATABASE CHIPMUNK_prod;

--         You should see that the PUBLIC and SALES schemas are owned by
--         SYSADMIN. INFORMATION_SCHEMA does not have an owner, which is
--         expected.

-- 5.4.2   Transfer ownership of the SALES schema to the CHIPMUNK_MANAGE_DB role.
--         The company wants centralized management, and since CHIPMUNK_manage_db
--         can create schemas, it should own all schemas.

USE ROLE SYSADMIN;
GRANT OWNERSHIP ON SCHEMA CHIPMUNK_prod.sales TO ROLE CHIPMUNK_manage_db COPY CURRENT GRANTS; 

--         The COPY CURRENT GRANTS clause means that any privileges on the
--         schema will be preserved.

-- 5.4.3   Check ownership of the SALES schema.

SHOW SCHEMAS LIKE 'sales';


-- 5.4.4   Make SALES a MANAGED ACCESS schema.

ALTER SCHEMA CHIPMUNK_prod.sales ENABLE MANAGED ACCESS;

--         When a role creates an object, that role owns it. Normally, the owner
--         of an object can do anything with it, including grant privileges on
--         that object to other roles. The MANAGED ACCESS clause means that only
--         the schema owner can grant privileges on the objects inside the
--         schema, regardless of who owns them. This centralizes the control of
--         object privileges.

-- 5.4.5   Test the DBA role.
--         Once you have set up your role-based access control, you should test
--         it to verify that it works as expected.
--         This is the most powerful custom functional role in your hierarchy.
--         It should be able to create schemas and objects, and perform any
--         operation on those objects. It should also be able to grant
--         privileges on any objects in the schemas it owns.

USE ROLE CHIPMUNK_dba;
USE DATABASE CHIPMUNK_prod;
USE WAREHOUSE CHIPMUNK_small_query_wh;

CREATE SCHEMA finance;
USE SCHEMA finance;

CREATE TABLE dba_fin_tbl (c1 INT);
INSERT INTO dba_fin_tbl VALUES (1), (2), (3), (4), (5), (6);
SELECT * FROM dba_fin_tbl;
DELETE FROM dba_fin_tbl WHERE c1=6;
SELECT * FROM dba_fin_tbl;

USE SCHEMA sales;
CREATE TABLE partners (acct INT, company_name VARCHAR, sales_2020 NUMBER(10,2));
INSERT INTO partners VALUES (1004, 'Expedia', 1234548.12),
                            (1332, 'Kayak', 987256.95),
                            (1447, 'Priceline', 245334.78); 



-- 5.4.6   Test the DEV role.
--         The DEV role was granted privileges to create objects. It also
--         inherited SELECT, INSERT, UPDATE, DELETE, and TRUNCATE from the roles
--         below it. It was also given usage rights on future schemas. You want
--         to verify that this role can use a warehouse and create and
--         manipulate objects. You also want to verify that this role cannot
--         create schemas, but it can create objects in schemas that the DBA
--         created.
--         Run the following statements to determine if the DEV role was
--         configured correctly, based on what it was designed to do.

USE ROLE CHIPMUNK_dev;
USE CHIPMUNK_prod.sales;
USE WAREHOUSE CHIPMUNK_small_query_wh;
CREATE TABLE dev_sales_tbl (c1 INT, c2 VARCHAR(1));
INSERT INTO dev_sales_tbl VALUES (1, 'A'), (2, 'B'), (3, 'C'), (4,'D'), (5, 'E');
SELECT * FROM dev_sales_tbl;

CREATE VIEW dev_sales_view AS 
  SELECT * FROM dev_sales_tbl WHERE c1=3;

SELECT * FROM dev_sales_view;

SELECT * FROM partners;

USE SCHEMA finance;

TRUNCATE TABLE dba_fin_tbl;

INSERT INTO dba_fin_tbl VALUES (2), (4), (6), (8), (10);

SELECT f.c1, s.c2 FROM dba_fin_tbl f 
   JOIN sales.dev_sales_tbl s
   ON f.c1 = s.c1;

CREATE SCHEMA dev_schema;

--         The CREATE SCHEMA and TRUNCATE TABLE commands should fail, but all
--         the others should work.

-- 5.4.7   Test the ELT role.
--         The ELT role cannot create objects, but it can SELECT, INSERT,
--         UPDATE, and DELETE. Test the capabilities of the ELT role. Some of
--         these commands will work, and others will fail.
--         Run the following statements to determine if the ELT role was
--         configured correctly, based on what it was designed to do.

USE ROLE CHIPMUNK_elt;
USE DATABASE CHIPMUNK_prod;
USE WAREHOUSE CHIPMUNK_elt_wh;

SELECT * FROM sales.dev_sales_tbl;
INSERT INTO sales.dev_sales_tbl VALUES (6, 'F'), (7, 'G'), (8, 'H'), (9, 'I');
SELECT * FROM sales.dev_sales_tbl;
DELETE FROM sales.dev_sales_tbl WHERE c2='H';
SELECT * FROM sales.dev_sales_tbl;

SELECT * FROM finance.dba_fin_tbl;

TRUNCATE finance.dba_fin_tbl;

DELETE FROM sales.partners WHERE sales_2020 < 100000;

--         The ELT role was configured to SELECT, INSERT, UPDATE, and DELETE,
--         but not TRUNCATE the table.

-- 5.4.8   Test the ANALYST role.
--         The ANALYST role can use a warehouse and select data, but has not
--         other privileges. Verify that this is the case. Again, some of the
--         command below will succeed and others will fail. Is this role set up
--         correctly?

USE ROLE CHIPMUNK_analyst;
USE DATABASE CHIPMUNK_prod;
USE WAREHOUSE CHIPMUNK_elt_wh;
USE WAREHOUSE CHIPMUNK_large_query_wh;

CREATE SCHEMA test;

CREATE TABLE sales.analyst_tbl (c1 INT);

USE SCHEMA finance;

INSERT INTO dba_fin_tbl VALUES (11), (12), (13);

SELECT * FROM dba_fin_tbl;

SELECT f.c1, s.c2 FROM dba_fin_tbl f 
   JOIN sales.dev_sales_tbl s
   ON f.c1 = s.c1;

SELECT * FROM sales.partners 
   WHERE company_name = 'Expedia';


-- 5.4.9   Grant the ability to create users to the DBAs.
--         The company has decided that the DBA should be more powerful. DBAs
--         should be able to create users and assign them to roles, and should
--         also be able to operate and monitor warehouses.

USE ROLE SECURITYADMIN;

GRANT CREATE USER ON ACCOUNT TO ROLE CHIPMUNK_manage_db;

--         Note that we grant these privileges on the object role that rolls up
--         to the functional (DBA) role, not to the functional role itself.

-- 5.4.10  Also give DBAs the ability to assign users to roles.

GRANT MANAGE GRANTS ON ACCOUNT
   TO ROLE CHIPMUNK_manage_db;


-- 5.4.11  Finally, allow the DBA to operate and monitor warehouses.

USE ROLE SYSADMIN;

GRANT OPERATE, MONITOR ON WAREHOUSE CHIPMUNK_elt_wh TO ROLE CHIPMUNK_manage_db;
GRANT OPERATE, MONITOR ON WAREHOUSE CHIPMUNK_mc_wh TO ROLE CHIPMUNK_manage_db;
GRANT OPERATE, MONITOR ON WAREHOUSE CHIPMUNK_small_query_wh TO ROLE CHIPMUNK_manage_db;
GRANT OPERATE, MONITOR ON WAREHOUSE CHIPMUNK_large_query_wh TO ROLE CHIPMUNK_manage_db;


-- 5.4.12  Test the new privileges.
--         The DBA should be able to create users, assign then to roles, and
--         start or stop (operate) the warehouses. The DBA is not able to modify
--         the warehouses.

USE ROLE CHIPMUNK_dba;

CREATE USER CHIPMUNK_test_user;
GRANT ROLE CHIPMUNK_dba TO USER CHIPMUNK_test_user;

ALTER WAREHOUSE CHIPMUNK_elt_wh RESUME;
ALTER WAREHOUSE CHIPMUNK_elt_wh SUSPEND;

ALTER WAREHOUSE CHIPMUNK_small_query_wh
   SET WAREHOUSE_SIZE=XSmall;

--         Since the DBA cannot modify a warehouse an error is expected.

-- 5.5.0   Explore and Testing Revoking Privileges
--         Revoking privileges may be needed to prevent a role, for example,
--         from selecting from a privileged table. The company does not want the
--         analysts selecting from the partners table.

-- 5.5.1   Confirm that you can currently select from the table using the
--         analyst role.

USE ROLE CHIPMUNK_analyst;
USE WAREHOUSE CHIPMUNK_small_query_wh;

SELECT * FROM CHIPMUNK_prod.sales.partners;


-- 5.5.2   Revoke privileges on the table.

USE ROLE CHIPMUNK_dba;

REVOKE SELECT ON TABLE CHIPMUNK_prod.sales.partners
   FROM ROLE CHIPMUNK_read;


-- 5.5.3   Confirm that you no longer can select from the table using the
--         analyst role.

USE ROLE CHIPMUNK_analyst;
USE CHIPMUNK_prod.sales;
USE WAREHOUSE CHIPMUNK_small_query_wh;

SELECT * FROM partners;

--         Since the role cannot select from a table an error is expected.

-- 5.5.4   Verify that the analyst can still select from other tables in the
--         SALES schema.

SELECT * FROM dev_sales_tbl;


-- 5.5.5   See if any other roles higher up in the hierarchy can select from the
--         partners table.

USE ROLE CHIPMUNK_elt;
SELECT * FROM partners;

USE ROLE CHIPMUNK_dev;
SELECT * FROM partners;

USE ROLE CHIPMUNK_dba;
SELECT * FROM partners;

--         What happened, and why?

-- 5.5.6   To try and find out, revoke privileges from another table and see
--         which roles can still read it.

USE ROLE CHIPMUNK_dba;
REVOKE SELECT ON TABLE CHIPMUNK_prod.sales.dev_sales_tbl
   FROM ROLE CHIPMUNK_read;

USE ROLE CHIPMUNK_analyst;
SELECT * FROM CHIPMUNK_prod.sales.dev_sales_tbl;

USE ROLE CHIPMUNK_ELT;
SELECT * FROM CHIPMUNK_prod.sales.dev_sales_tbl;

USE ROLE CHIPMUNK_DEV;
SELECT * FROM CHIPMUNK_prod.sales.dev_sales_tbl;

USE ROLE CHIPMUNK_DBA;
SELECT * FROM CHIPMUNK_prod.sales.dev_sales_tbl;

--         What were the results, and what does this tell you?

-- 5.5.7   Run this command to help you figure it out:

SHOW TABLES;

--         In each case, the owner of the table could still read it, even after
--         SELECT privileges were revoked from the READ role. You cannot revoke
--         privileges from an object owner.

-- 5.6.0   Clean Up
--         Run this series of commands all at once, to clean up the roles and
--         objects that were created for this exercise.

-- 5.6.1   Remove the roles, and the user that the DBA created.

USE ROLE SECURITYADMIN;

DROP ROLE CHIPMUNK_dba;
DROP ROLE CHIPMUNK_dev;
DROP ROLE CHIPMUNK_elt;
DROP ROLE CHIPMUNK_analyst;

DROP ROLE CHIPMUNK_read;
DROP ROLE CHIPMUNK_ins_del;
DROP ROLE CHIPMUNK_create_objects;
DROP ROLE CHIPMUNK_manage_db;

DROP USER CHIPMUNK_test_user;


-- 5.6.2   Remove the objects.

USE ROLE SYSADMIN;

DROP DATABASE CHIPMUNK_prod;

--         Note that if you drop a database, all the schemas and objects within
--         that database are automatically dropped as well. Warehouses are
--         independent of databases, so those do need to be dropped separately.

DROP WAREHOUSE CHIPMUNK_small_query_wh;
DROP WAREHOUSE CHIPMUNK_large_query_wh;
DROP WAREHOUSE CHIPMUNK_elt_wh;
DROP WAREHOUSE CHIPMUNK_mc_wh;

