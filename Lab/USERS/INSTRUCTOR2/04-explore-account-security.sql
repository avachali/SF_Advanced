
-- 4.0.0   Explore Account Security
--         This lab will take approximately 10 minutes to complete.

-- 4.1.0   Network Security
--         In this task, you will create a network policy and apply it to a user
--         to see the effect.

-- 4.1.1   Open a worksheet and use the role SECURITYADMIN.

-- 4.1.2   Enter the following to create a network policy that allows access
--         from only a single IP address:

CREATE NETWORK POLICY INSTRUCTOR2_policy ALLOWED_IP_LIST=('12.13.14.15');


-- 4.1.3   Describe the policy to verify that it was set correctly.

DESCRIBE NETWORK POLICY INSTRUCTOR2_policy;


-- 4.1.4   Now, create a new user:

CREATE USER INSTRUCTOR2_testuser PASSWORD = 'Password@1' MUST_CHANGE_PASSWORD = FALSE;


-- 4.1.5   Log out of the Snowflake account, and log back in as the user
--         INSTRUCTOR2_testuser you just created. This is just to verify that the
--         new user can log in. After you have done that, log back out and log
--         in as yourself INSTRUCTOR2.

-- 4.1.6   Now apply the network policy you created earlier to your test user:

ALTER USER INSTRUCTOR2_testuser SET NETWORK_POLICY = INSTRUCTOR2_policy;


-- 4.1.7   Log out of the Snowflake account, and try to log in as your test user
--         INSTRUCTOR2_testuser. You will not be able to. Then log in as yourself
--         INSTRUCTOR2. Since the network policy has only been applied to the test
--         user, it does not impact your ability to access the Snowflake
--         account.

-- 4.1.8   Drop the policy and user you created:

DROP USER INSTRUCTOR2_testuser;
DROP NETWORK POLICY INSTRUCTOR2_policy;


-- 4.2.0   Account-Level Parameters
--         In this lab, you will explore parameters that can be set at various
--         levels, and how parameters are affected by changes at a higher level.

-- 4.2.1   In your worksheet, set your context to use the training role, your
--         database, and the public schema:

USE ROLE training_role;
CREATE DATABASE IF NOT EXISTS INSTRUCTOR2_db;
USE INSTRUCTOR2_db.public;


-- 4.2.2   Run the following command to see all the parameters that are
--         available at the account level:

SHOW PARAMETERS FOR ACCOUNT;

--         Review the available parameters and their default values.

-- 4.2.3   Check the default time travel retention time that is set at the
--         account level:

SHOW PARAMETERS LIKE 'DATA_RETENTION_TIME_IN_DAYS' IN ACCOUNT ;


-- 4.2.4   Create two new tables - one with the default retention time, and
--         another with a different retention time:

CREATE OR REPLACE TABLE tt_default (col1 INT);

CREATE OR REPLACE TABLE tt_set30 (col2 INT)
DATA_RETENTION_TIME_IN_DAYS=30;


-- 4.2.5   Verify the retention time for the two tables:

SHOW PARAMETERS LIKE 'DATA_RETENTION%' FOR TABLE tt_default;
SHOW PARAMETERS LIKE 'DATA_RETENTION%' FOR TABLE tt_set30;


-- 4.2.6   Change the retention time on your schema:

ALTER SCHEMA INSTRUCTOR2_db.public SET DATA_RETENTION_TIME_IN_DAYS = 10;


-- 4.2.7   Verify the retention time for the two tables:

SHOW PARAMETERS LIKE 'DATA_RETENTION%' FOR TABLE tt_default;
SHOW PARAMETERS LIKE 'DATA_RETENTION%' FOR TABLE tt_set30;

--         You will see that the retention time on tt_set30, which was set to a
--         specific value, did not change. The retention time on tt_default
--         takes its value from the closest enclosing object, which is the
--         schema.

-- 4.2.8   Change the retention time for the database:

ALTER DATABASE INSTRUCTOR2_db SET DATA_RETENTION_TIME_IN_DAYS = 2;


-- 4.2.9   Verify the retention time for the schema, and the two tables:

SHOW PARAMETERS LIKE 'DATA_RETENTION%' FOR SCHEMA public;
SHOW PARAMETERS LIKE 'DATA_RETENTION%' FOR TABLE tt_default;
SHOW PARAMETERS LIKE 'DATA_RETENTION%' FOR TABLE tt_set30;

--         You should see that all of the retention times are unaffected by this
--         change - because tt_set30 has a specific value set and the schema,
--         which also has a specific value set, passes its setting to
--         tt_default. This is because the schema is tt_default’s closest
--         enclosing object that has the value set.

-- 4.2.10  Reset the retention time for the schema back to the default:

ALTER SCHEMA INSTRUCTOR2_db.public UNSET DATA_RETENTION_TIME_IN_DAYS;

--         Using UNSET ensures the schema will inherit its data retention time
--         from its enclosing object. If you had set the data retention time to
--         1, it would use that value regardless of any changes made at the
--         database or account level.

-- 4.2.11  Verify the retention time for the schema, and the two tables:

SHOW PARAMETERS LIKE 'DATA_RETENTION%' FOR SCHEMA public;
SHOW PARAMETERS LIKE 'DATA_RETENTION%' FOR TABLE tt_default;
SHOW PARAMETERS LIKE 'DATA_RETENTION%' FOR TABLE tt_set30;

--         Since the schema and table tt_default do not have specific values
--         set, they will now take on the value set by the closest enclosing
--         object that has a set value (the database).

-- 4.2.12  Drop your test tables, and reset the retention time for the database
--         back to the default value:

DROP TABLE tt_default;
DROP TABLE tt_set30;
ALTER DATABASE INSTRUCTOR2_db UNSET DATA_RETENTION_TIME_IN_DAYS;

