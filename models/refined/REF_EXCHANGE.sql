WITH current_from_snapshot AS (
    select *  
    from {{ ref('SNSH_EXCHANGE') }}
    WHERE DBT_VALID_TO is null
)
SELECT * from current_from_snapshot