{% snapshot scd_raw_listings %}

{{
   config(
       target_database='AIRBNB',
       target_schema='dev',
       unique_key='id',

       strategy='timestamp',
       updated_at='updated_at',
       invalidate_hard_deletes=True
   )
}}

select * FROM {{ source('airbnb', 'raw_listings') }}

{% endsnapshot %}