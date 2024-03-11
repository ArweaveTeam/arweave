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

# Function to download and verify file
download_and_verify() {
  local url=$1
  local path=$2
  local etag=$(curl -sI "$url" | grep -i etag | awk '{print $2}' | tr -d '\"\r\n')
  local attempt=0
  local success=0

  while [ $attempt -lt 3 ]; do
    # Download the file using wget, preserving the directory structure
    wget -q -c -O "$path" "$url"
    local md5_downloaded=$(md5sum "$path" | awk '{print $1}')

    if [[ "$etag" == "$md5_downloaded" ]]; then
      success=1
      break
    else
      echo "MD5 mismatch for $path. Expected $etag, got $md5_downloaded. Retrying..."
      ((attempt++))
    fi
  done

  if [ $success -eq 0 ]; then
    rm -f "$path"
    echo "Failed to download $path after 3 attempts. File deleted."
  fi
}

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

  # Call download_and_verify function in the background
  download_and_verify "$line" "$relative_path" &

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