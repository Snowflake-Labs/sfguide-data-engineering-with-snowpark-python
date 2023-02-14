/*-----------------------------------------------------------------------------
Hands-On Lab: Data Engineering with Snowpark
Script:       03_load_weather.sql
Last Updated: 1/9/2023
-----------------------------------------------------------------------------*/

-- SNOWFLAKE ADVANTAGE: Data sharing/marketplace (instead of ETL)
-- SNOWFLAKE ADVANTAGE: Visual Studio Code Snowflake native extension (PrPr, Git integration)


USE ROLE HOL_ROLE_DE;
USE WAREHOUSE HOL_WH_DE;

-- ----------------------------------------------------------------------------
-- Step #1: Connect to weather data in Marketplace
-- "Weather Source LLC: frostbyte" -> "FROSTBYTE_WEATHERSOURCE"
-- https://app.snowflake.com/marketplace/listing/GZSOZ1LLEL/weather-source-llc-weather-source-llc-frostbyte?search=Weather%20Source%20LLC%3A%20frostbyte
-- ----------------------------------------------------------------------------

-- Let's look at the data - same 3-part naming convention as any other table
SELECT * FROM FROSTBYTE_WEATHERSOURCE.ONPOINT_ID.POSTAL_CODES LIMIT 100;
