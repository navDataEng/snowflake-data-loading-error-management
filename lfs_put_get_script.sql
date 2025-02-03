--Context Setup

SELECT CURRENT_ROLE(); 		-- OUTPUT: ACCOUNTADMIN

USE ROLE SYSADMIN;
USE DATABASE DAILY_FILES;
USE SCHEMA DAILY_LOADS;
USE WAREHOUSE COMPUTE_WH;

-- Creating Internal Named Stage
CREATE STAGE LFS_STAGE;

-- Stagging files
PUT 'file://D:/Data_Engineer/Projects/P1 Snowflake Loading/csv files/lfs_emp01.csv' @lfs_stage;
PUT 'file://D:/Data_Engineer/Projects/P1 Snowflake Loading/csv files/lfs_emp02.csv' @lfs_stage;
PUT 'file://D:/Data_Engineer/Projects/P1 Snowflake Loading/csv files/lfs_emp03.csv' @lfs_stage;

LIST @lfs_stage;

-- Unloading error files

GET @lfs_stage/lfs_daily_errors.csv file://C:/Users/Naveen/Downloads/Snowflake/;

GET @lfs_stage/lfs_daily_errors2.csv file://C:/Users/Naveen/Downloads/Snowflake/;



