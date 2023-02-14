/*-----------------------------------------------------------------------------
Hands-On Lab: Data Engineering with Snowpark
Script:       11_teardown.sql
Last Updated: 1/9/2023
-----------------------------------------------------------------------------*/


USE ROLE ACCOUNTADMIN;

DROP DATABASE IF EXISTS HOL_DB_DE;
DROP WAREHOUSE IF EXISTS HOL_WH_DE;
DROP ROLE IF EXISTS HOL_ROLE_DE;

-- Drop the weather share
DROP DATABASE IF EXISTS FROSTBYTE_WEATHERSOURCE;
