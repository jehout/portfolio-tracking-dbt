{% snapshot SNSH_EXCHANGE %}
    {{
        config(
            unique_key='EXCHANGE_HKEY',
            strategy='check', 
            check_cols=['EXCHANGE_HDIFF'],
            invalidate_hard_deletes=True
        )
    }}

    select * from {{ ref('STG_EXCHANGE') }}
 {% endsnapshot %}