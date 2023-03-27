/*-----------------------------------------------------------------------------
Hands-On Lab: Data Engineering with Snowpark
Script:       01_setup_snowflake.sql
Last Updated: 1/1/2023
-----------------------------------------------------------------------------*/

-- ----------------------------------------------------------------------------
-- Step #1: Accept Anaconda Terms & Conditions
-- ----------------------------------------------------------------------------

-- See Getting Started section in Third-Party Packages (https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-packages.html#getting-started)

-- ----------------------------------------------------------------------------
-- Step #2: Create the account level objects
-- ----------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

-- Roles
SET MY_USER = CURRENT_USER();
CREATE OR REPLACE ROLE HOL_ROLE_DE;
GRANT ROLE HOL_ROLE_DE TO ROLE SYSADMIN;
GRANT ROLE HOL_ROLE_DE TO USER SVCSNOWPARK;

GRANT EXECUTE TASK ON ACCOUNT TO ROLE HOL_ROLE_DE;
GRANT MONITOR EXECUTION ON ACCOUNT TO ROLE HOL_ROLE_DE;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE HOL_ROLE_DE;

-- Databases
CREATE OR REPLACE DATABASE HOL_DB_DE;
GRANT OWNERSHIP ON DATABASE HOL_DB_DE TO ROLE HOL_ROLE_DE;

-- Warehouses
CREATE OR REPLACE WAREHOUSE HOL_WH_DE WAREHOUSE_SIZE = MEDIUM, AUTO_SUSPEND = 900, AUTO_RESUME= TRUE;
GRANT OWNERSHIP ON WAREHOUSE HOL_WH_DE TO ROLE HOL_ROLE_DE;


-- ----------------------------------------------------------------------------
-- Step #3: Create the database level objects
-- ----------------------------------------------------------------------------
USE ROLE HOL_ROLE_DE;
USE WAREHOUSE HOL_WH_DE;
USE DATABASE HOL_DB_DE;

-- Schemas
CREATE OR REPLACE SCHEMA EXTERNAL;
CREATE OR REPLACE SCHEMA RAW_POS;
CREATE OR REPLACE SCHEMA RAW_CUSTOMER;
CREATE OR REPLACE SCHEMA HARMONIZED;
CREATE OR REPLACE SCHEMA ANALYTICS;

-- External Frostbyte objects
USE SCHEMA EXTERNAL;
CREATE OR REPLACE FILE FORMAT PARQUET_FORMAT
    TYPE = PARQUET
    COMPRESSION = SNAPPY
;
CREATE OR REPLACE STAGE FROSTBYTE_RAW_STAGE
    URL = 's3://sfquickstarts/data-engineering-with-snowpark-python/'
;

-- ANALYTICS objects
USE SCHEMA ANALYTICS;

CREATE OR REPLACE FUNCTION ANALYTICS.INCH_TO_MILLIMETER_UDF(INCH NUMBER(35,4))
RETURNS NUMBER(35,4)
    AS
$$
    inch * 25.4
$$;
