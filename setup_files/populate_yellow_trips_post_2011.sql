-- Ensure temporary points table with filtering conditions is not needed
-- as we're updating the coordinates directly

-- Update pickup coordinates directly from centroids in `taxi_zones`
UPDATE yellow_tripdata_staging AS trips
SET
  pickup_longitude = ST_X(taxi_zones.geom_centroid),
  pickup_latitude = ST_Y(taxi_zones.geom_centroid)
FROM taxi_zones
WHERE trips.pickup_location_id = taxi_zones.gid
  AND trips.pickup_location_id IS NOT NULL
  AND trips.pickup_location_id < 264;

-- Update dropoff coordinates directly from centroids in `taxi_zones`
UPDATE yellow_tripdata_staging AS trips
SET
  dropoff_longitude = ST_X(taxi_zones.geom_centroid),
  dropoff_latitude = ST_Y(taxi_zones.geom_centroid)
FROM taxi_zones
WHERE trips.dropoff_location_id = taxi_zones.gid
  AND trips.dropoff_location_id IS NOT NULL
  AND trips.dropoff_location_id < 264;

-- Insert the final trip data into the `trips` table
INSERT INTO trips
(cab_type_id, vendor_id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance, pickup_longitude, pickup_latitude, rate_code_id, store_and_fwd_flag, dropoff_longitude, dropoff_latitude, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, congestion_surcharge, airport_fee, total_amount, pickup_location_id, dropoff_location_id)
SELECT
  cab_types.id,
  CASE
    WHEN trim(vendor_id) IN ('1', '2') THEN vendor_id::integer
    WHEN trim(upper(vendor_id)) = 'CMT' THEN 1
    WHEN trim(upper(vendor_id)) = 'VTS' THEN 2
    WHEN trim(upper(vendor_id)) = 'DDS' THEN 3
  END,
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  passenger_count,
  trip_distance,
  pickup_longitude,
  pickup_latitude,
  CASE WHEN trim(rate_code_id) IN ('1', '2', '3', '4', '5', '6') THEN rate_code_id::integer END,
  CASE
    WHEN trim(upper(store_and_fwd_flag)) IN ('Y', '1', '1.0') THEN true
    WHEN trim(upper(store_and_fwd_flag)) IN ('N', '0', '0.0') THEN false
  END,
  dropoff_longitude,
  dropoff_latitude,
  CASE
    WHEN trim(replace(payment_type, '"', '')) IN ('1', '2', '3', '4', '5', '6') THEN payment_type::integer
    WHEN trim(lower(replace(payment_type, '"', ''))) IN ('credit', 'cre', 'crd') THEN 1
    WHEN trim(lower(replace(payment_type, '"', ''))) IN ('cash', 'cas', 'csh') THEN 2
    WHEN trim(lower(replace(payment_type, '"', ''))) IN ('no charge', 'no') THEN 3
    WHEN trim(lower(replace(payment_type, '"', ''))) IN ('dispute', 'dis') THEN 4
  END,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  congestion_surcharge,
  airport_fee,
  total_amount,
  pickup_location_id,
  dropoff_location_id
FROM yellow_tripdata_staging
  INNER JOIN cab_types ON cab_types.type = 'yellow';

-- Cleanup staging table
TRUNCATE TABLE yellow_tripdata_staging;
VACUUM ANALYZE yellow_tripdata_staging;
