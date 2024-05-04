#!/bin/bash

# Path to your input text file that contains URLs
input_file="setup_files/filtered_urls.txt"

# Directory where the files will be downloaded
download_directory="data/"

# Directory where temporary files will be stored
temp_directory="temp/"

# Create the download directory if it doesn't exist
mkdir -p "$download_directory"

# Create the temporary directory if it doesn't exist
mkdir -p "$temp_directory"

# Directory to move the downloaded files to
move_directory="/Users/Pratik/Library/CloudStorage/GoogleDrive-pratikr@stanford.edu/My Drive/taxi-data"

# Read each line from the file
while IFS= read -r url
do
    # Extract the filename from the URL
    filename=$(basename "$url")

    # Download the file using wget
    wget -P "$download_directory" "$url"

#   # Display the filename
#   echo "Downloaded file: $filename"

    # Initialize the database
    bash initialize_database.sh

    # Import the data into the database
    # This will delete the parquet file after importing
    bash import_yellow_taxi_trip_data_individual.sh "$download_directory$filename"

    # Remove the file extension for use in the python script
    base_name="${filename%%.*}"

    # Convert the database to an h5py file
    python database_to_h5py.py --filename "$base_name" --save_dir "$temp_directory"

    # Move the h5py file to the Google Drive folder
    mv "$temp_directory$base_name.h5py" "$move_directory"

    # Delete the database
    psql postgres -c "DROP DATABASE \"nyc-taxi-data\";"

    # Delete the .csv and .csv.gz files in the temporary directory
    find "$temp_directory" -type f \( -name "*.csv" -o -name "*.csv.gz" \) -delete

    # Pause for 2 seconds before the next download
    sleep 2
done < "$input_file"
