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
select * from {{source('seeds', 'EXCHANGE')}};
