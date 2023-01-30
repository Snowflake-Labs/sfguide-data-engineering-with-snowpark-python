/*-----------------------------------------------------------------------------
Hands-On Lab: Data Engineering with Snowpark
Script:       09_process_incrementally.sql
Author:       Jeremiah Hansen
Last Updated: 1/9/2023
-----------------------------------------------------------------------------*/

USE ROLE HOL_ROLE;
USE WAREHOUSE HOL_WH;
USE DATABASE HOL_DB;


-- ----------------------------------------------------------------------------
-- Step #1: Add new/remaining order data
-- ----------------------------------------------------------------------------

USE SCHEMA RAW_POS;

ALTER WAREHOUSE HOL_WH SET WAREHOUSE_SIZE = XLARGE;
CALL SYSTEM$WAIT(5, 'SECONDS');

COPY INTO ORDER_HEADER
FROM @external.frostbyte_raw_stage/pos/order_header/year=2022
FILE_FORMAT = (FORMAT_NAME = EXTERNAL.PARQUET_FORMAT)
MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;

COPY INTO ORDER_DETAIL
FROM @external.frostbyte_raw_stage/pos/order_detail/year=2022
FILE_FORMAT = (FORMAT_NAME = EXTERNAL.PARQUET_FORMAT)
MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;

-- See how many new records are in the stream (this may be a bit slow)
--SELECT COUNT(*) FROM HARMONIZED.POS_FLATTENED_V_STREAM;

ALTER WAREHOUSE HOL_WH SET WAREHOUSE_SIZE = XSMALL;


-- ----------------------------------------------------------------------------
-- Step #2: Execute the tasks
-- ----------------------------------------------------------------------------

USE SCHEMA HARMONIZED;

EXECUTE TASK ORDERS_UPDATE_TASK;


-- ----------------------------------------------------------------------------
-- Step #3: Monitor tasks in Snowsight
-- ----------------------------------------------------------------------------

/*---
-- TODO: Add Snowsight details here
-- https://docs.snowflake.com/en/user-guide/ui-snowsight-tasks.html

-- Remove the filter on "User" and under "Filters" toggle the "Queries executed by user tasks"



-- Alternatively, here are some manual queries to get at the same details
SHOW TASKS;

-- Task execution history in the past day
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START=>DATEADD('DAY',-1,CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 100))
ORDER BY SCHEDULED_TIME DESC
;

-- Query history in the past hour
SELECT *
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
    DATEADD('HOURS',-1,CURRENT_TIMESTAMP()),CURRENT_TIMESTAMP()))
ORDER BY START_TIME DESC
LIMIT 100;

---*/
