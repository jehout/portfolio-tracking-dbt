WITH current_security_info as (
  select * 
    EXCLUDE (SECURITY_HDIFF, SECURITY_HKEY, RECORD_SOURCE, LOAD_TS_UTC
    , DBT_SCD_ID, DBT_UPDATED_AT, DBT_VALID_FROM, DBT_VALID_TO)
  from  {{ ref('SNSH_ABC_BANK_SECURITY_INFO') }}
  WHERE DBT_VALID_TO is NULL
)
select * from current_security_info