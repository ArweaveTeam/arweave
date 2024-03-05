#!/bin/bash
# Check if two arguments are passed
if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <max_concurrent_downloads> <input_file>"
  exit 1
fi

# First argument: Maximum number of concurrent downloads
max_concurrent_downloads=$1

# Second argument: Input file to read URLs from
input_file=$2

# Calculate the total number of lines (files to download) at the beginning
total_lines=$(wc -l < "$input_file")
echo "Total files to download: $total_lines"

# Counter for the number of concurrent downloads
concurrent_downloads=0

# Counter for the number of lines processed
lines_processed=0

# Read each line from the input file
while IFS= read -r line; do
  # Skip empty lines
  if [ -z "$line" ]; then
    continue
  fi

  # Dynamically determine the base URL for the current line, extracting the first three segments
  base_url=$(echo "$line" | cut -d'/' -f1-6)/

  # Remove the base URL to get the relative path
  relative_path="${line#$base_url}"

  # Create the directory structure for the file
  mkdir -p "$(dirname "$relative_path")"

  echo "Downloading $line to ./$relative_path"

  # Download the file using wget in the background, preserving the directory structure
  wget -q -c -O "$relative_path" "$line" &

  # Increment the concurrent downloads counter
  ((concurrent_downloads++))
  ((lines_processed++))

  # If the number of concurrent downloads reaches the max, wait for all to complete before continuing
  if (( concurrent_downloads >= max_concurrent_downloads )); then
    wait
    concurrent_downloads=0
    # Calculate and print the remaining lines (files) to download
    remaining_lines=$((total_lines - lines_processed))
    echo "Remaining files to download: $remaining_lines"
  fi
done < "$input_file" # Read URLs from the input file specified as the second argument

# Wait for any remaining background downloads to complete
wait
# After the last batch of downloads, print the remaining lines one last time
remaining_lines=$((total_lines - lines_processed))
echo "Remaining files to download: $remaining_lines"