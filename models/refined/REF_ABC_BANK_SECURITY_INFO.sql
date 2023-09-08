WITH current_security_info as (
  {{ current_from_snapshot(snsh_ref=ref('SNSH_ABC_BANK_SECURITY_INFO'), output_snsh_load_ts=false)}}
)
select * from current_security_info