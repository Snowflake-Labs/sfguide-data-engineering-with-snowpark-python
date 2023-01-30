/*-----------------------------------------------------------------------------
Hands-On Lab: Data Engineering with Snowpark
Script:       10_deploy_via_cicd.sql
Author:       Jeremiah Hansen
Last Updated: 1/9/2023
-----------------------------------------------------------------------------*/

-- SNOWFLAKE ADVANTAGE: GitHub Actions (CI/CD) integration


-- ----------------------------------------------------------------------------
-- Step #1: Make change to fahrenheit_to_celsius_udf
-- ----------------------------------------------------------------------------

-- Make the following changes to 05_fahrenheit_to_celsius_udf/app.py
-- Add this line on line 9: "from scipy.constants import convert_temperature"
-- Replace the body of main() with the following content:
--     return convert_temperature(float(temp_f), 'F', 'C')

-- Make the following changes to 05_fahrenheit_to_celsius_udf/requirements.txt
-- Add this line to the file:
--     scipy


-- ----------------------------------------------------------------------------
-- Step #2: Test changes to fahrenheit_to_celsius_udf locally
-- ----------------------------------------------------------------------------

-- In the terminal change to the 05_fahrenheit_to_celsius_udf directory and run these commands
-- pip install -r requirements.txt
-- python app.py 35


-- ----------------------------------------------------------------------------
-- Step #3: Deploy changes to fahrenheit_to_celsius_udf via CI/CD pipeline
-- ----------------------------------------------------------------------------

-- Commit your changes to 05_fahrenheit_to_celsius_udf/app.py and 05_fahrenheit_to_celsius_udf/requirements.txt
-- Push your changes to your forked repository

-- Review .github/workflows/build_and_deploy.yaml

-- TODO: Add steps to view GitHub Actions result
