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

  # Fetch the expected file size from the Content-Length HTTP header
  local expected_size=$(curl -sI "$url" | grep -i Content-Length | awk '{print $2}' | tr -d '\r')

  local attempt=0
  local success=0

  while [ $attempt -lt 5 ]; do
    # Download the file using wget, preserving the directory structure
    wget_output=$(wget -c -O "$path" "$url" 2>&1)
    wget_exit_code=$?

    if [ $wget_exit_code -eq 0 ]; then
      # Get the actual size of the downloaded file
      local actual_size=$(stat -c %s "$path")
      if [[ "$expected_size" == "$actual_size" ]]; then
        success=1
        break
      elif [[ "$actual_size" -gt "$expected_size" ]]; then
        echo "Actual size greater than expected for $path. Expected $expected_size, got $actual_size. Deleting file and retrying..."
        rm -f "$path"
      else
        echo "File size mismatch for $path. Expected $expected_size, got $actual_size. Retrying..."
      fi
    else
      if echo "$wget_output" | grep -q "416 Requested Range Not Satisfiable"; then
        echo "Received 416 error for $path. Deleting the file and retrying..."
        rm -f "$path"
      else
        echo "Failed to download $path. Error: $wget_output. Retrying..."
      fi
    fi

    sleep 3
    ((attempt++))
  done

  if [ $success -eq 0 ]; then
    echo "Failed to download $path after 5 attempts."
  fi
}

# Read each line from the input file
while IFS= read -r line; do
  # Skip empty lines
  if [ -z "$line" ]; then
    continue
  fi

  # Dynamically determine the base URL for the current line, extracting the first three segments
  base_url=$(echo "$line" | cut -d'/' -f1-5)/

  # Remove the base URL to get the relative path
  relative_path="${line#$base_url}"

  # Create the directory structure for the file
  mkdir -p "$(dirname "$relative_path")"
  echo "Downloading ($((lines_processed+1))/$total_lines) $line to ./$relative_path"

  # Call download_and_verify function in the background
  download_and_verify "$line" "$relative_path" &

  # Increment the concurrent downloads counter
  ((concurrent_downloads++))
  ((lines_processed++))

  # If max concurrent downloads reached, wait for one to finish before continuing
  if (( concurrent_downloads >= max_concurrent_downloads )); then
    wait -n
    ((concurrent_downloads--))
  fi
done < "$input_file" # Read URLs from the input file specified as the second argument

# Wait for any remaining background downloads to complete
wait
# After the last batch of downloads, print the remaining lines one last time
remaining_lines=$((total_lines - lines_processed))
echo "Remaining files to download: $remaining_lines"