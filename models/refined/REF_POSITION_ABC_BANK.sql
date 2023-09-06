WITH current_from_snapshot as (
    select *
    from {{ref('SNSH_ABC_BANK_POSITION')}}
    where DBT_VALID_TO is null
)

select *
  , POSITION_VALUE - COST_BASE as UNREALIZED_PROFIT
  , ROUND(UNREALIZED_PROFIT / COST_BASE, 5) as UNREALIZED_PROFIT_PCT
from current_from_snapshot  -- we only want to have the active/current records