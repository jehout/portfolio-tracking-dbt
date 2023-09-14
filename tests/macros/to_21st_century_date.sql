WITH 
input_data as (
    select 0 as id, '0001-01-01'::date as input, '2001-01-01'::date as expected_result
    UNION
    select 1 as id, '2001-01-01'::date as input, '2001-01-01'::date as expected_result
    UNION
    select 2 as id, '0023-01-01'::date as input, '2023-01-01'::date as expected_result
),
macro_data as (
    select id, {{ to_21st_century_date('input')}} as actual_result 
    from input_data
)
select *, actual_result
from input_data
join macro_data on input_data.id = macro_data.id
where expected_result != actual_result
