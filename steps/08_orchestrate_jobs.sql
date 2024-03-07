/*-----------------------------------------------------------------------------
Hands-On Lab: Data Engineering with Snowpark
Script:       08_orchestrate_jobs.sql
Author:       Jeremiah Hansen
Last Updated: 1/9/2023
-----------------------------------------------------------------------------*/

-- SNOWFLAKE ADVANTAGE: Tasks (with Stream triggers)
-- SNOWFLAKE ADVANTAGE: Task Observability

USE ROLE HOL_ROLE;
USE WAREHOUSE HOL_WH;
USE SCHEMA HOL_DB.HARMONIZED;


-- ----------------------------------------------------------------------------
-- Step #1: Create the tasks to call our Python stored procedures
-- ----------------------------------------------------------------------------

CREATE OR REPLACE TASK ORDERS_UPDATE_TASK
WAREHOUSE = HOL_WH
WHEN
  SYSTEM$STREAM_HAS_DATA('POS_FLATTENED_V_STREAM')
AS
CALL HARMONIZED.ORDERS_UPDATE_SP();

CREATE OR REPLACE TASK DAILY_CITY_METRICS_UPDATE_TASK
WAREHOUSE = HOL_WH
AFTER ORDERS_UPDATE_TASK
WHEN
  SYSTEM$STREAM_HAS_DATA('ORDERS_STREAM')
AS
CALL ANALYTICS.DAILY_CITY_METRICS_UPDATE_SP();


-- ----------------------------------------------------------------------------
-- Step #2: Execute the tasks
-- ----------------------------------------------------------------------------

ALTER TASK DAILY_CITY_METRICS_UPDATE_TASK RESUME;
EXECUTE TASK ORDERS_UPDATE_TASK;


-- ----------------------------------------------------------------------------
-- Step #3: Monitor tasks in Snowsight
-- ----------------------------------------------------------------------------

/*---
-- TODO: Add Snowsight details here
-- https://docs.snowflake.com/en/user-guide/ui-snowsight-tasks.html



-- Alternatively, here are some manual queries to get at the same details
SHOW TASKS;

-- Task execution history in the past day
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START=>DATEADD('DAY',-1,CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 100))
ORDER BY SCHEDULED_TIME DESC
;

-- Scheduled task runs
SELECT
    TIMESTAMPDIFF(SECOND, CURRENT_TIMESTAMP, SCHEDULED_TIME) NEXT_RUN,
    SCHEDULED_TIME,
    NAME,
    STATE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE STATE = 'SCHEDULED'
ORDER BY COMPLETED_TIME DESC;

-- Other task-related metadata queries
SELECT *
  FROM TABLE(INFORMATION_SCHEMA.CURRENT_TASK_GRAPHS())
  ORDER BY SCHEDULED_TIME;

SELECT *
  FROM TABLE(INFORMATION_SCHEMA.COMPLETE_TASK_GRAPHS())
  ORDER BY SCHEDULED_TIME;
---*/