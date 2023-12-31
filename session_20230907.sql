-- generating Adapter
    SELECT
      ', ' || COLUMN_NAME || ' as '|| COLUMN_NAME  || ' -- ' || DATA_TYPE as SQL_TEXT
    FROM PORTFOLIO_TRACKING.INFORMATION_SCHEMA.COLUMNS  -- adapt to your DB
    WHERE TABLE_SCHEMA = 'JEH_SEED_DATA'
      AND TABLE_NAME = 'ABC_BANK_SECURITY_INFO'
    ORDER BY ORDINAL_POSITION;                       -- the order of the columns in the table

--select * from {{source('seeds', 'ABC_Bank_SECURITY_INFO')}};
select SECURITY_CODE, SECURITY_HDIFF from {{ref('STG_ABC_BANK_SECURITY_INFO')}};

-- exchange
--- source
select * from {{source('seeds', 'EXCHANGE')}};

-- stg
SELECT
    ', ' || COLUMN_NAME || ' as '|| COLUMN_NAME  || ' -- ' || DATA_TYPE as SQL_TEXT
FROM PORTFOLIO_TRACKING.INFORMATION_SCHEMA.COLUMNS  -- adapt to your DB
WHERE TABLE_SCHEMA = 'JEH_SEED_DATA'
    AND TABLE_NAME = 'EXCHANGE'
ORDER BY ORDINAL_POSITION;                       -- the order of the columns in the table
---
select * from {{ ref('STG_EXCHANGE') }};



-- fixing the dates of CSV abc_bank_positions
--- investigating the problem: the report-date in the csv is interpreted by Snowflake as a year 0021 instead of 2021
select *, year(report_date), month(report_date), day(report_date) 
, year(dateadd(year, 2000, report_date))--, month(report_date), day(report_date) 
from {{ source('abc_bank', 'ABC_BANK_POSITION') }};

-- calculating hashes for HKEY, HDIFF of STG_ABC_BANK_POSITION
--- drop snsh 
select * from {{ ref('SNSH_ABC_BANK_POSITION') }}; 
--- drop table {{ ref('SNSH_ABC_BANK_POSITION') }};
--- rebuild the snapshot table: dbt snapshot -s ...
select * from {{ ref('REF_POSITION_ABC_BANK') }};
select * from {{ ref('FACT_POSITION') }};

-- calculating hashes for HKEY, HDIFF of STG_ABC_BANK_SECURITY_INFO
select * from {{ ref('SNSH_ABC_BANK_SECURITY_INFO') }};
--- drop table {{ ref('SNSH_ABC_BANK_SECURITY_INFO') }};
--- rebuild the snapshot table: dbt snapshot -s ...
select * from {{ ref('REF_ABC_BANK_SECURITY_INFO') }};
select * from {{ ref('DIM_SECURITY') }};

-- calculating hashes for HKEY, HDIFF of STG_ABC_BANK_SECURITY_INFO
select * from {{ ref('SNSH_EXCHANGE') }};
---drop table {{ ref('SNSH_EXCHANGE') }};
--- rebuild the snapshot table: dbt snapshot -s ...
select * from {{ ref('REF_EXCHANGE') }};
select * from {{ ref('DIM_EXCHANGE') }};
