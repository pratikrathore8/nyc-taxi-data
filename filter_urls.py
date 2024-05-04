# Path to your input text file that contains URLs
input_file_path = './setup_files/raw_data_urls.txt'

# Path to your output text file for filtered URLs
output_file_path = './setup_files/filtered_urls.txt'

# Function to filter URLs
def filter_urls(input_file_path, output_file_path):
    with open(input_file_path, 'r') as file:
        urls = file.readlines()

    # Filter URLs that contain 'yellow' and are from year 2015 or earlier
    filtered_urls = [url.strip() for url in urls if 'yellow' in url and int(url.split('_')[2].split('-')[0]) <= 2015]

    # Write filtered URLs to output file
    with open(output_file_path, 'w') as file:
        for url in filtered_urls:
            file.write(url + '\n')

# Call the function with the file paths
filter_urls(input_file_path, output_file_path)
