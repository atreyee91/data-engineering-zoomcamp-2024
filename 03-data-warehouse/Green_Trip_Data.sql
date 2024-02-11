CREATE EXTERNAL TABLE dezoomcamp-411909.dezoomcamp_411909_terra_dataset.green_trip_data_ext
  OPTIONS (
    format ="Parquet",
    uris = ['gs://dezoomcamp-411909-terra-bucket/*.parquet']
    );

create or replace table dezoomcamp-411909.dezoomcamp_411909_terra_dataset.green_trip_data
as select * from dezoomcamp-411909.dezoomcamp_411909_terra_dataset.green_trip_data_ext;

select count(1) from dezoomcamp-411909.dezoomcamp_411909_terra_dataset.green_trip_data;

select count(distinct PULocationID) from dezoomcamp-411909.dezoomcamp_411909_terra_dataset.green_trip_data_ext;--6.41 MB

select count(distinct PULocationID) from dezoomcamp-411909.dezoomcamp_411909_terra_dataset.green_trip_data;--6.41 MB

select count(1) from dezoomcamp-411909.dezoomcamp_411909_terra_dataset.green_trip_data_ext where fare_amount=0;

create or replace table dezoomcamp-411909.dezoomcamp_411909_terra_dataset.green_trip_data_partition
partition by
Date(lpep_pickup_datetime)
cluster by (PUlocationID)
as select * from dezoomcamp-411909.dezoomcamp_411909_terra_dataset.green_trip_data;

select distinct PUlocationID
from dezoomcamp-411909.dezoomcamp_411909_terra_dataset.green_trip_data_partition
where lpep_pickup_datetime between '2022-06-01' and '2022-06-30';

select distinct PUlocationID
from dezoomcamp-411909.dezoomcamp_411909_terra_dataset.green_trip_data
where lpep_pickup_datetime between '2022-06-01' and '2022-06-30';





   
