{{
    config(
        materialized='ephemeral'
    )
}}

with src_data as (
    select 
        NAME			as EXCHANGE_NAME	-- TEXT
        , ID			as EXCHANGE_CODE	-- TEXT
        , COUNTRY		as COUNTRY_NAME		-- TEXT
        , CITY			as CITY_NAME		-- TEXT
        , ZONE			as TIME_ZONE_CODE	-- TEXT
        , DELTA		    as UTC_OFFSET		-- FLOAT
        --, DST_PERIOD		as DST_PERIOD		-- TEXT
        , case when DST_PERIOD is null then 'N/A' else substring(DST_PERIOD, 0,3) end	as DST_START_MONTH		-- TEXT TODO: could be number
        , case when DST_PERIOD is null then 'N/A' else substring(DST_PERIOD, 5,7) end	as DST_END_MONTH		-- TEXT TODO
        , OPEN			as OPEN_TIME			-- TIME
        , CLOSE		    as CLOSE_TIME		-- TIME
        , LUNCH		    as LUNCH_PERIOD		-- TEXT
        , OPEN_UTC		as OPEN_TIME_UTC		-- TIME
        , CLOSE_UTC		as CLOSE_TIME_UTC		-- TIME
        , LUNCH_UTC		as LUNCH_PERIOD_UTC		-- TEXT
        , LOAD_TS		as LOAD_TS_UTC		-- TIMESTAMP_NTZ
        , 'SEED_EXCHANGE'	as RECORD_SOURCE
    from {{ source('seeds', 'EXCHANGE') }}
),
default_row as (
    select 
        'Missing'		as EXCHANGE_NAME	-- TEXT
        , '-1'			as EXCHANGE_CODE	-- TEXT
        , 'Missing'		as COUNTRY_NAME		-- TEXT
        , 'Missing'		as CITY_NAME		-- TEXT
        , '-1'			as TIME_ZONE_CODE	-- TEXT
        , 999  		as UTC_OFFSET		-- FLOAT
        , 'Missing'		as DST_START_MONTH		-- TEXT
        , 'Missing'		as DST_END_MONTH		-- TEXT
        , '00:00:00'		    as OPEN_TIME			-- TIME
        , '00:00:00'  		as CLOSE_TIME		-- TIME
        , 'Missing'		as LUNCH_PERIOD		-- TEXT
        , '00:00:00'		    as OPEN_TIME_UTC		-- TIME
        , '00:00:00'  		as CLOSE_TIME_UTC		-- TIME
        , 'Missing'  		as LUNCH_PERIOD_UTC		-- TEXT
        , '2000-01-01'  as LOAD_TS_UTC
        , 'System.DefaultKey' as RECORD_SOURCE  -- the default row does not come from the seed !
), 
with_default_row as (
    select * from src_data
    UNION
    select * from default_row
), 
hashed as (
    select 
          concat_ws('|', EXCHANGE_CODE) as EXCHANGE_HKEY
          , concat_ws('|' 
                , EXCHANGE_NAME	
                , COUNTRY_NAME	
                , CITY_NAME	
                , TIME_ZONE_CODE	
                , UTC_OFFSET		
                , DST_START_MONTH
                , DST_END_MONTH
                , OPEN_TIME
                , CLOSE_TIME
                , LUNCH_PERIOD
                , OPEN_TIME_UTC	
                , CLOSE_TIME_UTC	
                , LUNCH_PERIOD_UTC	          
	            ) as EXCHANGE_HDIFF
          , * 
    from with_default_row
)
select * from hashed
