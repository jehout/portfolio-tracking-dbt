WITH current_from_snapshot AS (
    {{ current_from_snapshot(snsh_ref=ref('SNSH_EXCHANGE'), output_snsh_load_ts=false)}}
)
SELECT * from current_from_snapshot