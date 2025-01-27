grant usage on warehouse compute_wh to role sysadmin;   -- Granting Privileges to SYSADMIN role using ACCOUNTADMIN role
use role sysadmin;                                      -- Changing role to SYSADMIN
create database daily_files;                            -- Creating database for daily files 
create schema daily_loads;                              -- Creating schema for daily files
create table daily_files.daily_loads.emp_daily (        -- Creating emp_daily table to store daily files data
    EMPNO number(10),
    ENAME varchar(30),
    GENDER varchar(1),
    JOB varchar(30),
    MGR number(10),
    HIREDATE date,
    SAL number(10),
    COMM number(10),
    DEPTNO number(2)
);


use role accountadmin;                          -- Using ACCOUNTADMIN role for storage integration creation
CREATE STORAGE INTEGRATION daily_integration    -- Creating storage integration to logically link AWS and Snowflake
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::975049574800:role/daily-files-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://daily-transaction-files');

  
grant usage on integration daily_integration to role sysadmin;  -- Granting usage privileges to SYSADMIN role.
use role sysadmin;
desc integration daily_integration;                             -- Describing integration for ARN details
create file format csv_file_format type=CSV;                    -- Creating file format of type CSV

create stage s3_stage                                           -- Creating external stage using integration
    storage_integration = daily_integration
    url='s3://daily-transaction-files'
    file_format = csv_file_format;
    
list @s3_stage;

desc file format csv_file_format;
alter file format csv_file_format set skip_header=1;    -- Altering csv_file_format to skip header record from files


-- Scenario - 1: Capturing errors after loading data to targrt 


copy into daily_files.daily_loads.emp_daily             -- Loading emp file data to emp_daily target table
    from @s3_stage/
    file_format = csv_file_format
    on_error = continue;                                -- This will avoid error rows in files while loading to target
    
select * from emp_daily;

-- Captures error records in last copy command
select * from table(validate(daily_files.daily_loads.emp_daily, job_id => '_last'));  

-- Creating an error table using CTAS to store error records
create table daily_errors as                 
    (select * from table(validate(daily_files.daily_loads.emp_daily, job_id => '_last')));
    
select * from daily_errors;

-- Unloading error records from daily_errors to a file and adding to stage
copy into @s3_stage/daily_errors_01262025.csv    
    from daily_files.daily_loads.daily_errors
    file_format = (type=CSV, field_optionally_enclosed_by = '"' field_delimiter = ',', compression = none)
    header = true
    single = true;
    
list @s3_stage;


-- Scenario - 2: Capturing errors before loading data to targrt 


-- Checking errors in files before loading into target table

copy into emp_daily
    from @s3_stage/
    file_format = (type=CSV, skip_header=1)
    on_error = continue
    validation_mode= return_all_errors;

-- Capturing copy command with validation_mode result

select * from table(result_scan(last_query_id()));

-- Creating a table to store these errors

create table daily_errors2 as
    (select * from table(result_scan(last_query_id())));

select * from daily_errors2;

-- Unloading data from errors table to external stage
copy into @s3_stage/daily_errors2_01262025.csv
    from daily_files.daily_loads.daily_errors2
    file_format = (type=csv, field_optionally_enclosed_by = '"' ,field_delimiter = ',', compression = none)
    header = true
    single = true;

list @s3_stage;