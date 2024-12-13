#!/bin/bash
######################################################################
# Arweave partition synchronization script. The goal of this script
# is to offert a simple interface to fetch unpacked storage
# partitions.
#
# = File Format (v1)
#
# The first version of the file format, simple an url pointing to
# a web server serving partition files.
#
# ```
# ${url}
# ```
#
# = File Format v2
#
# the second version of the file format includes file size and
# checksum (md5). ${relative_path} will be concatenated with the
# base_url parameter from the command line (or the one present by
# default in the script).
#
# ```
# ${relative_path} ${content_length} ${checksum}
# ```
#
######################################################################
set +e

# global variables, used during the whole script
BASE_URL="https://s3.zephyrdev.xyz/arweave-pool"
JOBS="8"
INPUT_FILE=""
CHECK=""

_usage() {
	printf -- "Usage: %s [-hcf] [-b BASE_URL] [-j JOBS] -i INPUT_FILE\n" "${0}"
}

_usage_full() {
	_usage
	printf -- "  -h: print full help\n"
	printf -- "  -c: check downloaded data (checksum)\n"
	printf -- "  -f: force download if file size is the same\n"
	printf -- "  -b BASE_URL: the base url used to fetch partitions\n"
	printf -- "               set to '%s' by default\n" "${BASE_URL}"
	printf -- "  -j JOBS: number of parallel jobs (%s)\n" "${JOBS}"
	printf -- "  -i INPUT_FILE: file containing partitions list\n"
}

_debug() {
	if test "${DEBUG}"
	then
		printf -- "debug: %s\n" "${*}" 1>&2
	fi
}

_error() {
	printf -- "error: %s\n" "${*}" 1>&2
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
		-f) flag_force="yes"; shift;;
		-j) flag_jobs="${2}"; shift; shift;;
		-i) flag_input="${2}"; shift; shift;;
		-b) flag_base_url="${2}"; shift; shift;;
		--) shift; break;;
	esac
done

# if no arguments, print usage and exit.
if test "${flag_help}"
then
	_usage_full
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

# check if the base url is valid
if test "${flag_base_url}"
then
	if echo "${flag_base_url}" | grep -E '/$' 2>&1 >/dev/null
	then
		_error "base url '${flag_base_url}' can't end with '/'"
		_usage
		exit 1
	fi
	if echo "${flag_base_url}" | grep -vE '^http(|s)://' 2>&1 >/dev/null
	then
		_error "base url '${flag_base_url}' must use https"
		_usage
		exit 1
	fi
	BASE_URL="${flag_base_url}"
fi

# curl is required
curl=$(which curl)
test -z "${curl}" && _error "curl not found" && exit 1

# wget is required
wget=$(which wget)
test -z "${wget}" && _error "wget not found" && exit 1

# md5sum is required
if test "${CHECK}"
then
	md5sum=$(which md5sum)
	test -z "${md5sum}" && _error "md5sum not found" && exit 1
fi

# download and retry if it fails.
_download_with_retry() {
	local url="${1}"
	local path="${2}"
	local expected_size="${3}"
	local attempt=0
	local max_attempts=5
	local success=1

	while [ $attempt -lt ${max_attempts} ]
	do
		# Download the file using wget, preserving the
		# directory structure
		wget_output=$(wget -c -O "$path" "$url" 2>&1)
		wget_exit_code=$?

		if [ $wget_exit_code -eq 0 ]
		then
			# Get the actual size of the downloaded file
			local actual_size=$(stat -c %s "$path")
			if [[ "$expected_size" == "$actual_size" ]]
			then
				success=0
				return 0
			elif [[ "$actual_size" -gt "$expected_size" ]]
			then
				_error "Actual size greater than expected for $path. Expected $expected_size, got $actual_size. Deleting file and retrying..."
				rm -f "$path"
			else
				_error "File size mismatch for $path. Expected $expected_size, got $actual_size. Retrying..."
			fi
		else
			if echo "$wget_output" | grep -q "416 Requested Range Not Satisfiable"
			then
				_error "Received 416 error for $path. Deleting the file and retrying..."
				rm -f "$path"
			else
				_error "Failed to download $path. Error: $wget_output. Retrying..."
			fi
		fi

		# wait 3 second and then retry
		sleep 3
		((attempt++))
	done

	if [ ${success} -ne 0 ]
	then
		_error "Failed to download $path after 5 attempts."
	fi
	return 1
}

# first version of the script, it will do a first
# request to have an idea of the file size and then
# start the download.
_download_and_verify_v1() {
	local url=$1
	local path=$2
	_debug "(v1) download from ${url} to ${path}"

  	# Fetch the expected file size from the Content-Length HTTP header
  	local expected_size=$(curl -sI "$url" \
		| grep -i Content-Length \
		| awk '{print $2}' \
		| tr -d '\r')

	_download_with_retry "${url}" "${path}" "${expected_size}"
}

# wrapper around test to check file size.
_check_size() {
	path="${1}"
	m="${2}"
	n="${3}"
	if test "${m}" -eq "${n}"
	then
		# "${path} has same size"
		return 0
	elif test "${m}" -gt "${n}"
	then
		# "${path} is greater"
		return 1
	elif test "${m}" -lt "${n}"
	then
		# "${path} is smaller"
		return 2
	else
		return 3
	fi
}

# wrapper around md5sum.
_check_md5() {
	path="${1}"
	original_checksum="${2}"
	if test "${CHECK}"
	then
		_debug "check ${path} with ${original_checksum}"
		payload="${original_checksum} ${path}"
		echo ${payload} | md5sum -c 2>&1 >/dev/null
		if test $? -eq 0
		then
			_debug "md5 for ${path} is valid"
			return 0
		else
			_error "md5 for ${path} is invalid"
			return 1
		fi
	fi
}

# this function will download files using v2 format.
# it also includes a way to check the size and the
# checksum without asking the server, but by using
# the data present in the partition list file.
_download_and_verify_v2() {
	local base_url="${1}"
	local path="${2}"
	local content_length="${3}"
	local checksum="${4}"
	local full_url="${base_url}/${path}"
	_debug "(v2) download from ${full_url} to ${path}"

	# only used for local check, using exit code from function
	local check_file_exist=1
	local check_file_size=3
	local check_file_checksum=1

	# check if the local file already exist
	# and if it's the case then check its size
	if test -f "${path}"
	then
		check_file_exist=0

		# if the file exists, extract its size
		file_size=$(stat -c '%s' "${path}")
		_check_size ${path} ${content_length} ${file_size}
		check_file_size=$?
	fi

	# check the checksum using md5sum
	if test "${flag_check}" && test "${check_file_exists}"
	then
		_check_md5 ${path} ${checksum}
		check_file_checksum=$?
	fi

	# force flag will always download the file
	# even if the size or the checksums are valid
	if test "${flag_force}"
	then
		_debug "force download..."
		_download_with_retry "${full_url}" "${path}" "${content_length}"
	else
		if test ${check_file_exist} -ne 0
		then
			_debug "download file, it does not exist"
			_download_with_retry "${full_url}" "${path}" "${content_length}"
		elif test ${flag_check} && test ${check_file_checksum} -eq 0
		then
			_debug "checksum is the same, don't download"
			return 0
		elif test ${flag_check} && test ${check_file_checksum} -ne 0
		then
			_debug "checksum is not the same, download"
			_download_with_retry "${full_url}" "${path}" "${content_length}"
		elif test ${check_file_size} -eq 0 
		then
			_debug "same size don't download"
			return 0
		elif test ${check_file_size} -eq 0
		then
			_debug "files are not the same, download"
			_download_with_retry "${full_url}" "${path}" "${content_length}"
		fi
	fi
}

# check if a line is compatible with v1 format
_check_v1_line() {
	local line="${1}"
	echo "${line}" \
		| grep -E '^http(|s)://' \
		> /dev/null
}

# check if a line is compatible with v2 format
_check_v2_line() {
	local line="${1}"
	echo "${line}" \
		| grep -iE '^[[:print:]]+\ +[0-9]+(|\ +[0-9a-f]{32})$' \
		> /dev/null
}

# First argument: Maximum number of concurrent downloads
max_concurrent_downloads="${JOBS}"

# Calculate the total number of lines (files to download) at the beginning
total_lines=$(wc -l < "${INPUT_FILE}")
echo "Total files to download: $total_lines"

# Counter for the number of concurrent downloads
concurrent_downloads=0

# Counter for the number of lines processed
lines_processed=0
line_number=1

# Read each line from the input file
cat "${INPUT_FILE}" | while read -r line
do
	# Skip empty lines
	test -z "${line}" && continue
	echo "${line}" | grep -E '^#' >/dev/null && continue

	# cleanup variables
	callback=""
	params=""
	base_url=""
	relative_path=""
	content_length=""
	checksum=""

	# Dynamically determine the base URL for the current line,
	# extracting the first three segments
	if _check_v1_line "${line}"
	then
		# version 1 file
		callback=_download_and_verify_v1
		base_url=$(echo "$line" | awk '{print $1}' | cut -d'/' -f1-4)/
		# Remove the base URL to get the relative path
		relative_path="./${line#$base_url}"
		params="${line} ${relative_path}"
	elif _check_v2_line "${line}"
	then
		# version 2 file
		callback=_download_and_verify_v2
		relative_path=./$(echo "${line}" | awk '{print $1}')
		content_length=$(echo "${line}" | awk '{print $2}')
		checksum=$(echo "${line}" | awk '{print $3}')
		params="${BASE_URL} ${relative_path} ${content_length} ${checksum}"
	else
		_error "unsupported format (line ${line_number}): ${line}"
		exit 2
	fi

	# Create the directory structure for the file
	mkdir -p "$(dirname "${relative_path}")"

	# Call download_and_verify function in the background
	echo "Downloading ($((lines_processed+1))/$total_lines) $line to $relative_path"
	_debug "executing ${callback} ${params}"
	${callback} ${params} &

	# Increment the concurrent downloads counter
	concurrent_downloads=$((concurrent_downloads+1))
	lines_processed=$((lines_processed+1))

	# If max concurrent downloads reached, wait for one to finish before continuing
	if (( concurrent_downloads >= max_concurrent_downloads ))
	then
		wait -n
		((concurrent_downloads--))
	fi

	line_number=$((line_number+1))
done

# Wait for any remaining background downloads to complete
wait

# After the last batch of downloads, print the remaining lines one last time
remaining_lines=$((total_lines - lines_processed))
echo "Remaining files to download: $remaining_lines"
