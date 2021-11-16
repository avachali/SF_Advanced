
-- 1.0.0   Take a Quick Test Drive
--         This lab will take approximately 25 minutes to complete.
--         You must complete this lab to be able to complete all of the
--         remaining labs in this course. Do not skip this lab.
--         The instructor will provide you with a URL to connect to the training
--         account, as well as a user name and password. The remainder of the
--         labs in this workbook assume that you can log in as needed.

-- 1.1.0   Explore the User Interface
--         The purpose of this task is simply to make sure you are comfortable
--         navigating around the user interface, and can perform basic tasks
--         like rename a worksheet, turn on code highlighting, and switch your
--         workksheet context.

-- 1.1.1   Make sure you are in the Worksheets section of the interface (this is
--         where you will be by default when you log in).

-- 1.1.2   Double-click the tab at the top of your worksheet, and rename it Lab
--         1.

-- 1.1.3   Review the list of databases in the object browser (on the left-hand
--         side of the worksheet).

-- 1.1.4   Change to the PUBLIC role:

USE ROLE PUBLIC;

--         Notice that the list in the object browser changes. The role you are
--         currently using will determine what objects you can access.

-- 1.1.5   Change back to the TRAINING_ROLE role:

USE ROLE TRAINING_ROLE;


-- 1.1.6   Turn on Code Highlighting. To do this, look for the three dots just
--         to the right of the worksheet context (in the upper right-hand corner
--         of the worksheet). Click on the three dots and select Turn on Code
--         Highlight.
--         Enable Code Highlight
--         Now when your cursor is on a line of code, the entire statement will
--         be highlighted. This shows you what will be executed if you run the
--         statement.

-- 1.1.7   Enter the following SQL statement and then click Run at the top of
--         the worksheet.

SHOW PARAMETERS FOR SESSION;


-- 1.1.8   Run the same command again, using the keyboard shortcut. This will be
--         CTRL+RETURN on Windows machines, or CMD+RETURN on Macs.

SHOW PARAMETERS FOR SESSION;


-- 1.1.9   Quickly explore what is available to you in the top ribbon. When you
--         are done, return to the Worksheets area.

-- 1.2.0   Create Objects for Course Labs

-- 1.2.1   Run the following commands to create some basic objects that will be
--         used during this course:

CREATE DATABASE INSTRUCTOR2_db;
CREATE SCHEMA INSTRUCTOR2_db.myschema;
CREATE WAREHOUSE INSTRUCTOR2_wh INITIALLY_SUSPENDED=TRUE AUTO_SUSPEND=300;
CREATE WAREHOUSE INSTRUCTOR2_wh_small WAREHOUSE_SIZE=SMALL INITIALLY_SUSPENDED=TRUE AUTO_SUSPEND=300;
CREATE WAREHOUSE INSTRUCTOR2_wh_large WAREHOUSE_SIZE=LARGE INITIALLY_SUSPENDED=TRUE AUTO_SUSPEND=300;

--         When you create virtual warehouses through the command line, they
--         will be set to automatically resume when needed, unless you set
--         AUTO_RESUME=FALSE.

-- 1.2.2   Run the following commands to set your context (the database, schema,
--         role, and virtual warehouse that will be used in this worksheet).
--         Some of these objects may already be set in your context, but these
--         are the four default elements you can set for every worksheet
--         context.

USE ROLE TRAINING_ROLE;
USE WAREHOUSE INSTRUCTOR2_wh_small;
USE DATABASE INSTRUCTOR2_db;
USE SCHEMA myschema;


-- 1.2.3   Set your default context, so that every time you open a new worksheet
--         these values will automatically be set for you:

USE ROLE SECURITYADMIN;
ALTER USER INSTRUCTOR2
    SET
    DEFAULT_ROLE=TRAINING_ROLE
    DEFAULT_NAMESPACE=INSTRUCTOR2_DB.PUBLIC
    DEFAULT_WAREHOUSE=INSTRUCTOR2_wh_small;


-- 1.2.4   Log out of the web UI, and back in. This forces your new user
--         settings to be used.

-- 1.2.5   Open a new worksheet and verify that your default context is
--         automatically set when you open a new worksheet. The context for any
--         existing worksheets will not be changed.

-- 1.3.0   Run Queries on Sample Data

-- 1.3.1   In the left-side object browser, navigate to SNOWFLAKE_SAMPLE_DATA,
--         then TPCH_SF1.

-- 1.3.2   Right-click the schema name (TPCH_SF1) and select **Set as Context**.
--         This is another way that you can change your worksheet context.

-- 1.3.3   Verify that the new database and schema are now set in your context.

-- 1.3.4   Click TPCH_SF1 to expand the schema, and then click the ORDERS table.
--         A pane describing the orders table appears at the bottom of the
--         navigation pane.

-- 1.3.5   Click Preview Data to preview the data in the ORDERS table.
--         Above the results is a slider with Data and Details. Data should be
--         selected by default.

-- 1.3.6   Select Details to view the detailed information on the column
--         definitions.

-- 1.3.7   In your worksheet, run the following commands to explore the data:

SHOW TABLES;

SELECT COUNT(*) FROM orders;

SELECT * FROM supplier LIMIT 10;

SELECT MAX(o_totalprice) FROM orders;

SELECT o_orderpriority, SUM(o_totalprice)
FROM orders
GROUP BY o_orderpriority
ORDER BY SUM(o_totalprice);

SELECT o_orderpriority, SUM(o_totalprice)
FROM orders
GROUP BY o_orderpriority
ORDER BY o_orderpriority;


-- 1.3.8   View the Query Profile by clicking Query ID just above the results
--         pane. Follow the link to get to the Query Profile. Review the
--         information in the Details tab, then select the Profile tab to show
--         more information about the query.
--         We will view the query profile many times throughout this course, so
--         make sure you know how to get there.

-- 1.3.9   Click the Worksheets icon in the top ribbon to return to the
--         Worksheets area.
