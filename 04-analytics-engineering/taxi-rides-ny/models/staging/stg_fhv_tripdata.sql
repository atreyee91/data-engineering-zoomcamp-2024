{{
    config(
        materialized='view'
    )
}}

with tripdata as 
(
  select *
  from {{ source('staging','fhv_trip_data_dbt') }}
  where extract(year from pickup_datetime)=2019 
)
select * from tripdata
