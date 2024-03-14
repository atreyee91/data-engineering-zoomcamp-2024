
Let's start ingestion into RisingWave by running it:
```bash
seed-kafka
```

Now we can let that run in the background.

Let's open another terminal to create the trip_data table:
```bash
source commands.sh
psql -f risingwave-sql/table/trip_data.sql
```

You may look at their definitions by running:
```bash
psql -c 'SHOW TABLES;'
```

### Now we will start creating MVs to transform the latest data

$ psql -c "CREATE MATERIALIZED VIEW latest_1min_trip_data AS
 SELECT taxi_zone.Zone as pickup_zone, taxi_zone_1.Zone as dropoff_zone, tpep_pickup_datetime, tpep_dropoff_datetime
 FROM trip_data
 JOIN taxi_zone ON trip_data.PULocationID = taxi_zone.location_id
 JOIN taxi_zone as taxi_zone_1 ON trip_data.DOLocationID = taxi_zone_1.location_id;"
$ psql -c "select * from latest_1min_trip_data limit 10;"
$ psql -c "CREATE MATERIALIZED VIEW trip_data_zone as SELECT pickup_zone, dropoff_zone, tpep_pickup_datetime, tpep_dropoff_datetime, EXTRACT(EPOCH FROM tpep_dropoff_datetime - tpep_pickup_datetime) as trip_duration from latest_1min_trip_data;"
$ psql -c "select * from trip_data_zone limit 10;"
$ psql -c "CREATE MATERIALIZED VIEW trip_data_zone_grouped_by_duraion as select pickup_zone, dropoff_zone, avg(trip_duration) as avg_duration from trip_data_zone group by pickup_zone, dropoff_zone;"

### Find the pair of taxi zones with the highest average trip time.
$ psql -c "select * from trip_data_zone_grouped_by_duraion order by avg_duration desc limit 1;"

### Find the number of trips for the pair of taxi zones with the highest average trip time.
$ psql -c "select count(1) from  trip_data_zone where pickup_zone|| '-' || dropoff_zone in (select  pickup_zone|| '-' || dropoff_zone from trip_data_zone_grouped_by_duraion order by avg_duration desc limit 1);"

### From the latest pickup time to 17 hours before, what are the top 3 busiest zones in terms of number of pickups

$ psql -c "
CREATE MATERIALIZED VIEW latest_pickup AS
    SELECT
        max(tpep_pickup_datetime) AS latest_pickup_time
    FROM
        trip_data
            JOIN taxi_zone
                ON trip_data.PULocationID = taxi_zone.location_id;"
$ psql -c "
CREATE MATERIALIZED VIEW pickups_17hr_before AS
    SELECT
        pickup_zone, 
        dropoff_zone,
        count(*) AS cnt
    FROM
        trip_data_zone
            JOIN latest_pickup
                ON trip_data_zone.tpep_pickup_datetime > latest_pickup.latest_pickup_time - interval '17 hour'
  group by pickup_zone, dropoff_zone"
  $ psql -c "select * from pickups_17hr_before order by cnt desc;"

psql -c "select min(tpep_pickup_datetime) from trip_data_zone
            JOIN latest_pickup
                ON trip_data_zone.tpep_pickup_datetime > latest_pickup.latest_pickup_time - interval '17 hour';"


### Clear tables
psql -c "Drop materialized view pickups_17hr_before;"
psql -c "Drop materialized view latest_pickup;"
psql -c "Drop materialized view trip_data_zone_grouped_by_duraion;"
psql -c "Drop materialized view trip_data_zone;"
psql -c "Drop materialized view latest_1min_trip_data;"

