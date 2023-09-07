-- general queries
select current_user();
select current_role();
SHOW ROLES;
SHOW GRANTS TO ROLE USERADMIN; -- 

-- create first user
USE ROLE USERADMIN;
CREATE ROLE DBT_EXECUTOR_ROLE
  COMMENT = 'Role for the users running DBT models';

GRANT ROLE DBT_EXECUTOR_ROLE TO USER jeh;

-- create database
--- grant db creation rights to dbt_role
USE ROLE SYSADMIN;
GRANT CREATE DATABASE ON ACCOUNT
  TO ROLE DBT_EXECUTOR_ROLE;

-- alter the warehouse
USE ROLE ACCOUNTADMIN;
ALTER WAREHOUSE "COMPUTE_WH" SET
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    COMMENT = 'Default Warehouse';

GRANT USAGE ON WAREHOUSE COMPUTE_WH
  TO ROLE DBT_EXECUTOR_ROLE;

-- create db PORTFOLIO_TRACKING
USE ROLE DBT_EXECUTOR_ROLE;
show grants to ROLE DBT_EXECUTOR_ROLE;
CREATE DATABASE PORTFOLIO_TRACKING;

-- create user dbt_executor
USE ROLE USERADMIN;
CREATE USER IF NOT EXISTS DBT_EXECUTOR
  COMMENT = 'User running DBT commands'
  PASSWORD = 'pick_a_password'
  DEFAULT_WAREHOUSE = 'COMPUTE_WH'
  DEFAULT_ROLE = 'DBT_EXECUTOR_ROLE';

GRANT ROLE DBT_EXECUTOR_ROLE TO USER DBT_EXECUTOR;  
USE ROLE DBT_EXECUTOR_ROLE;

-- 
use database PORTFOLIO_TRACKING;
show tables;

-- loading csv file in a landing table via a Snowflake FILE FORMAT
--- Create the landing table that will hold the external data.
USE ROLE DBT_EXECUTOR_ROLE;
CREATE SCHEMA PORTFOLIO_TRACKING.SOURCE_DATA;
create or replace table PORTFOLIO_TRACKING.SOURCE_DATA.ABC_BANK_POSITION (
  accountID         TEXT,
  symbol            TEXT,
  description       TEXT,
  exchange          TEXT,
  report_date       DATE,
  quantity          NUMBER(38,0),
  cost_base         NUMBER(38,5),
  position_value    NUMBER(38,5),
  currency          TEXT
);
--- provide Snowflake with the structure of the CSV file
CREATE FILE FORMAT
  PORTFOLIO_TRACKING.SOURCE_DATA.ABC_BANK_CSV_FILE_FORMAT
    TYPE = 'CSV'
        COMPRESSION = 'AUTO'
        FIELD_DELIMITER = ','
        RECORD_DELIMITER = '\n'
        SKIP_HEADER = 1
        FIELD_OPTIONALLY_ENCLOSED_BY = '\042'
        TRIM_SPACE = FALSE
        ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
        ESCAPE = 'NONE'
        ESCAPE_UNENCLOSED_FIELD = '\134'
        DATE_FORMAT = 'AUTO'
        TIMESTAMP_FORMAT = 'AUTO'
        NULL_IF = ('\\N')
;
--- Manually load the external data from the CSV file in the landing table.
----PUT file://C:/jeroen/p/sidehustle/reinrail/dbt-book/portfolio-tracking-dbt/ABC_Bank_PORTFOLIO__2021-04-09.csv 
----  @PORTFOLIO_TRACKING.SOURCE_DATA%ABC_BANK_POSITION/csv/ABC_Bank_PORTFOLIO__2021-04-09.csv;
---- the PUT does not work, docs say a PUT can not be done in SnowUI, only in SnowSQL
select * from ABC_BANK_POSITION;

--- Define the landing table as our source table.
--- Read the data from the source table to start our ELT.


show schemas;

use schema JEH_REFINED;
use schema JEH_STAGING;
show tables;
show views;

-- generating Adapter
    SELECT
      ', ' || COLUMN_NAME || ' as '|| COLUMN_NAME  || ' -- ' || DATA_TYPE as SQL_TEXT
    FROM PORTFOLIO_TRACKING.INFORMATION_SCHEMA.COLUMNS  -- adapt to your DB
    WHERE TABLE_SCHEMA = 'SOURCE_DATA'
      AND TABLE_NAME = 'ABC_BANK_POSITION'
    ORDER BY ORDINAL_POSITION;                       -- the order of the columns in the table

describe view POSITION_ABC_BANK;
describe view STG_ABC_BANK_POSITION;

-- chapter 6: refined model names
use schema JEH_REFINED;
drop view POSITION_ABC_BANK;

-- setting up the history with a dbt snapshot
--- we drop the STG view before we make it "ephemeral"
use schema JEH_STAGING;
drop view STG_ABC_BANK_POSITION;

--
    SELECT
      ', ' || COLUMN_NAME || ' as '|| COLUMN_NAME  || ' -- ' || DATA_TYPE as SQL_TEXT
    FROM PORTFOLIO_TRACKING.INFORMATION_SCHEMA.COLUMNS  -- adapt to your DB
    WHERE TABLE_SCHEMA = 'JEH_SEED_DATA'
      AND TABLE_NAME = 'ABC_BANK_SECURITY_INFO'
    ORDER BY ORDINAL_POSITION;                       -- the order of the columns in the table
