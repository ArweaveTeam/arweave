#!/bin/bash
######################################################################
# Arweave partition synchronization script.
#
# = File Format (v1)
#
# The first version of the file format
# 
# ```
# ${url}
# ```
#
# = File Format v2
#
# ```
# ${url} ${content_length} ${checksum} 
# ```
#
######################################################################
BASE_URL="https://s3.zephyrdev.xyz/arweave-pool"
JOBS="8"
INPUT_FILE=""
CHECK=""

_usage() {
	printf -- "Usage: %s [-hc] [-b BASE_URL] [-j JOBS] -i INPUT_FILE\n" "${0}"
}

_error() {
	printf -- "error: %s\n" "${*}"
}

# if no arguments passed, print usage
if [ "$#" -eq 0 ]
then
	_usage
	exit 0
fi

# parse arguments using getopt
args=$(getopt "hcj:i:b:" $*) 

# check if getopt correctly parsed arguments
if [ $? -ne 0 ]
then
	_usage
	exit 1
fi

# parse arguments from command lines
while [ $# -ne 0 ]
do
	case "${1}"
	in
		-h) flag_help="yes"; shift;;
		-c) flag_check="yes"; shift;;
		-j) flag_jobs="${2}"; shift; shift;;
		-i) flag_input="${2}"; shift; shift;;
		-b) flag_base_url="${2}"; shift; shift;;
		--) shift; break;;
	esac
done

# if no arguments, print usage and exit.
if test "${flag_help}"
then
	_usage
	exit 0
fi

# check input files name
if test "${flag_input}"
then
	if test -f "${flag_input}"
	then
		INPUT_FILE="${flag_input}"
	else
		_error "'${flag_input}' does not exist"
		_usage
		exit 1
	fi
else
	_error "input file missing"
	_usage
	exit 1
fi

# check jobs value
if test "${flag_jobs}"
then
	if echo $flag_jobs | grep -E '^[0-9]+$' 2>&1 >/dev/null
	then
		JOBS="${flag_jobs}"
	else
		_error "'${flag_jobs}' job argument not a number"
		_usage
		exit 1
	fi
fi

# configure checksum verification
if test "${flag_check}"
then
	CHECK="${flag_check}"
fi

if test "${flag_base_url}"
then
	if echo "${flag_base_url}" | grep -E '/$' 2>&1 >/dev/null
	then
		_error "base url '${flag_base_url}' can't end with '/'"
		_usage
		exit 1
	fi
	if echo "${flag_base_url}" | grep -vE '^https://' 2>&1 >/dev/null
	then
		_error "base url '${flag_base_url}' must use https"
		_usage
		exit 1
	fi
	BASE_URL="${flag_base_url}"
fi

exit 1

# First argument: Maximum number of concurrent downloads
max_concurrent_downloads="${JOBS}"

# Calculate the total number of lines (files to download) at the beginning
total_lines=$(wc -l < "${INPUT_FILE}")
echo "Total files to download: $total_lines"

# Counter for the number of concurrent downloads
concurrent_downloads=0

# Counter for the number of lines processed
lines_processed=0

# Function to download and verify file
download_and_verify_v1() {
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

# new functions including 
download_and_verify_v2() {
  local base_url="${1}"
  local path="${2}"
  local content="${3}"
  local checksum="${4}"
}

# Read each line from the input file
cat "${input_file}" | while IFS= read -r line
do
  # Skip empty lines
  if [ -z "$line" ]; then
    continue
  fi

  # cleanup variables
  callback=""
  params=""
  base_url=""
  relative_path=""
  content_length=""
  checksum=""

  # Dynamically determine the base URL for the current line, extracting the first three segments
  if echo "${line}" | grep -E '^https://'
  then
    # version 1 file
    callback=download_and_verify_v1
    base_url=$(echo "$line" | awk '{ print $1 }' | cut -d'/' -f1-5)/
    # Remove the base URL to get the relative path
    relative_path="${line#$base_url}"
    params="${line}" "${relative_path}"
  else
    # version 2 file
    callback=download_and_verify_v2
    relative_path=$(echo "${line}" | awk '{ print $1 }')
    content_length=$(echo "${line}" | awk '{ print $2 }')
    checksum=$(echo "${line}" | awk '{ print $3 }')
    params="${BASE_URL}" "${relative_path}" "${content_length}" "${checksum}"
  fi

  # Create the directory structure for the file
  mkdir -p "$(dirname "$relative_path")"
  echo "Downloading ($((lines_processed+1))/$total_lines) $line to ./$relative_path"

  # Call download_and_verify function in the background
  ${callback} ${params} &

  # Increment the concurrent downloads counter
  ((concurrent_downloads++))
  ((lines_processed++))

  # If max concurrent downloads reached, wait for one to finish before continuing
  if (( concurrent_downloads >= max_concurrent_downloads )); then
    wait -n
    ((concurrent_downloads--))
  fi
done

# Wait for any remaining background downloads to complete
wait
# After the last batch of downloads, print the remaining lines one last time
remaining_lines=$((total_lines - lines_processed))
echo "Remaining files to download: $remaining_lines"
