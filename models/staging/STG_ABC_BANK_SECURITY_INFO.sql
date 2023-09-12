{{ config(materialized='ephemeral') }}

WITH src_data as (
select
  SECURITY_CODE as SECURITY_CODE	-- TEXT
  , SECURITY_NAME as SECURITY_NAME	-- TEXT
  , SECTOR        as SECTOR_NAME			-- TEXT
  , INDUSTRY      as INDUSTRY_NAME		-- TEXT
  , COUNTRY       as COUNTRY_CODE			-- TEXT
  , EXCHANGE      as EXCHANGE_CODE		-- TEXT
  , LOAD_TS       as LOAD_TS			-- TIMESTAMP_NTZ
  , 'SEED.ABC_BANK_SECURITY_INFO' as RECORD_SOURCE   -- 
  from {{source('seeds', 'ABC_Bank_SECURITY_INFO')}}
), 


default_row as (
  select
    '-1' as SECURITY_CODE            -- '-1' is our default for all code
     , 'Missing' as SECURITY_NAME
     , 'Missing' as SECTOR_NAME
     , 'Missing' as INDUSTRY_NAME
     , '-1' as COUNTRY_CODE
     , '-1' as EXCHANGE_CODE
     , '2000-01-01'  as LOAD_TS_UTC
     , 'System.DefaultKey' as RECORD_SOURCE  -- the default row does not come from the seed !
),


with_default_row as (
  select * from src_data
  UNION 
  select * from default_row
), 


    hashed as (
      SELECT
        {{ dbt_utils.generate_surrogate_key(['SECURITY_CODE']) 
        }} as SECURITY_HKEY                             -- 1-field-PK
        , {{ dbt_utils.generate_surrogate_key([
            'SECURITY_CODE', 'SECURITY_NAME', 'SECTOR_NAME',
            'INDUSTRY_NAME', 'COUNTRY_CODE', 'EXCHANGE_CODE']) 
        }} as SECURITY_HDIFF
        , * EXCLUDE LOAD_TS                 -- renaming the LOAD_TS...
        , LOAD_TS as LOAD_TS_UTC            -- ...to LOAD_TS_UTC
      FROM with_default_row
    )

select * from hashed