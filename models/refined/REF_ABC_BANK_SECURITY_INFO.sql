WITH current_security_info as (
  select * 
    EXCLUDE (DBT_SCD_ID, DBT_UPDATED_AT, DBT_VALID_FROM, DBT_VALID_TO)
  from  {{ ref('SNSH_ABC_BANK_SECURITY_INFO') }}
  WHERE DBT_VALID_TO is NULL
)
select * from current_security_info