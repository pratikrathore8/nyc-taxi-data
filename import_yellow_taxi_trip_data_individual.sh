#!/bin/bash

# Regular expression to extract the year and month from the filename
year_month_regex="tripdata_([0-9]{4})-([0-9]{2})"

# Schema definitions
yellow_schema="(vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, rate_code_id, store_and_fwd_flag, pickup_location_id, dropoff_location_id, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, airport_fee)"
yellow_schema_pre_2011="(vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, pickup_longitude, pickup_latitude, rate_code_id, store_and_fwd_flag, dropoff_longitude, dropoff_latitude, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, total_amount)"

# Get the first argument (Parquet filename) from the command line
parquet_filename=$1

# Extract the year and month using the regular expression
[[ $parquet_filename =~ $year_month_regex ]]
year=${BASH_REMATCH[1]}

# Determine the correct schema to use based on the year
if [ "$year" -lt 2011 ]; then
    schema=$yellow_schema_pre_2011
    sql_script="setup_files/populate_yellow_trips.sql"
else
    schema=$yellow_schema
    sql_script="setup_files/populate_yellow_trips_post_2011.sql"
fi

# Convert the Parquet file to CSV using the R script
echo "`date`: converting ${parquet_filename} to CSV"
./setup_files/convert_parquet_to_csv.R ${parquet_filename}

# Replace the .parquet extension with .csv
csv_filename=${parquet_filename/.parquet/.csv}

# Load the CSV data into the staging table
cat $csv_filename | psql nyc-taxi-data -c "COPY yellow_tripdata_staging ${schema} FROM stdin CSV HEADER;"
echo "`date`: finished raw load for ${csv_filename}"

# Execute the appropriate SQL script to populate the final table
psql nyc-taxi-data -f ${sql_script}
echo "`date`: loaded trips using ${sql_script} for ${csv_filename}"

# Remove the original and intermediate files to save space
rm -f $parquet_filename
echo "`date`: deleted ${parquet_filename}"

rm -f $csv_filename
echo "`date`: deleted ${csv_filename}"
