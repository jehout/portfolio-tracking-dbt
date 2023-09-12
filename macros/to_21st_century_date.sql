{% macro to_21st_century_date(date_column_name) %}
    case 
        when {{date_column_name}} >= '0100-01-01'::date 
        then {{date_column_name}}
        else dateadd(year, 2000, {{date_column_name}})
    end
{% endmacro %}