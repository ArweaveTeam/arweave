#!/bin/bash
######################################################################
# Arweave partition synchronization script. The goal of this script
# is to offert a simple and flexible interface to fetch unpacked
# arweave storage modules partitions.
#
# = Usage
#
# Help and usage can be printed using `-h` flag or by simply calling
# the script without any arguments.
#
#    Usage: ./bin/download-partition.sh [-cdhlpf] [-b BASE_URL] [-P
#    PREFIX] [-j JOBS] -i INPUT_FILE -I STORAGE_MODULE_INDEX
#
# == Download storage module index
#
# A storage module index contains a list of files with their size and
# checksum, available on a remote server (usually a CDN). This CDN is
# configured in the base_url variable (displayed in help usage). These
# indexes are stored on the remote end-point using this naming
# convention:
#
#   storage_module_${index}_unpacked.index
#
# Where ${index} is a positive integer corresponding to an arweave
# storage module. This index can be directly downloaded using -I flag
# from this script.
#
#     ./download-partition.sh -I 0
#
# The storage module index format is described in "File Format v2"
# section
#
# == Download storage module only (no checksum)
#
#     ./download-partition.sh -i ${index_file}
#
# == Download storage module and verify (checksum)
#
#     ./download-partition.sh -c -i ${index_file}
#
# == Check remote file presence
#
# This script check if the remote files are present on the CDN:
#
#     ./download-partition.sh -p -i ${index_file}
#
# == Check local file presence
#
# This script can check if the files are present (without doing
# any checksum):
#
#     ./download-partition.sh -l -i ${index_file}
#
# == Debug mode
#
# To print more information, a debug mode is available when setting
# `-d` flag or by configuring the `DEBUG` environment variable.
#
# == Extra features
#
# A local prefix can be set using `-P` flag.
#
# A force mode can be used, to overwrite files even if they have
# the same size.
#
# A custom base url can be defined with `-b` flag.
#
# A custom number of jobs/workers can be set using `-j` flag.
#
# = FAQ
#
# == Where to find index files?
#
# Indexes can be found on the CDN, example:
#
#  - https://s3.zephyrdev.xyz/arweave-pool/storage_module_0_unpacked.index
#  - https://s3.zephyrdev.xyz/arweave-pool/storage_module_1_unpacked.index
#  - https://s3.zephyrdev.xyz/arweave-pool/storage_module_${index}_unpacked.index
#
# = File Format v2
#
# The second version of the file format includes file size and
# checksum (md5). ${relative_path} will be concatenated with the
# base_url parameter from the command line (or the one present by
# default in the script):
#
#     ${relative_path} ${content_length} ${checksum}
#
######################################################################
set +e

# global variables, used during the whole script
BASE_URL="https://s3.zephyrdev.xyz/arweave-pool"
JOBS="8"
INPUT_FILE=""
CHECK=""
PREFIX=""

_usage() {
	printf -- "Usage: %s [-cdhlpf] [-b BASE_URL] [-P PREFIX] [-j JOBS] -I STORAGE_INDEX -i INPUT_FILE \n" "${0}"
}

_usage_full() {
	_usage
	printf -- "  -h: print full help\n"
	printf -- "  -d: debug mode\n"
	printf -- "  -p: check presence of files on CDN\n"
	printf -- "  -l: check local presence of files\n"
	printf -- "  -c: check downloaded data (checksum)\n"
	printf -- "  -f: force download if file size is the same\n"
	printf -- "  -b BASE_URL: the base url used to fetch partitions\n"
	printf -- "               set to '%s' by default\n" "${BASE_URL}"
	printf -- "  -j JOBS: number of parallel jobs (%s)\n" "${JOBS}"
	printf -- "  -P PREFIX: prefix for local path (%s)\n" "${PREFIX}"
	printf -- "  -i INPUT_FILE: file containing partitions list\n"
	printf -- "  -I INDEX: download the storage index from CDN\n"
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
args=$(getopt "cdhlpj:i:b:P:I:" $*)

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
		-c) flag_check="yes"; shift;;
		-d) DEBUG="yes"; shift;;
		-l) flag_local="yes"; shift;;
		-p) flag_presence="yes"; shift;;
		-h) flag_help="yes"; shift;;
		-f) flag_force="yes"; shift;;
		-j) flag_jobs="${2}"; shift; shift;;
		-i) flag_input="${2}"; shift; shift;;
		-b) flag_base_url="${2}"; shift; shift;;
		-P) flag_prefix="${2}"; shift; shift;;
		-I) flag_storage_index="${2}"; shift; shift;;
		--) shift; break;;
	esac
done

# if no arguments, print usage and exit.
if test "${flag_help}"
then
	_usage_full
	exit 0
fi

# check prefix path. it should be a directory
if test "${flag_prefix}"
then
	if ! test -e "${flag_prefix}"
	then
		_error "prefix ${flag_prefix} does not exist"
		exit 1
	fi
	# prefix MUST BE a directory
	if ! test -d "${flag_prefix}"
	then
		_error "prefix ${flag_prefix} is not a directory"
		exit 1
	fi

	# prefix MUST END with /
	if ! echo "${flag_prefix}" | grep -E '/$' 2>&1 >/dev/null
	then
		PREFIX=${flag_prefix}/
	fi
fi

# check input files name
if (test "${flag_input}" && test ! "${flag_storage_index}") \
	|| (test "${flag_input}" && test ! "${flag_storage_index}")
then
	_error "'-I' or '-i' is required"
	_usage
	exit 1
fi

# check flag input
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
fi

# check input index name
if test "${flag_storage_index}"
then
	if echo "${flag_storage_index}" | grep -E '^[0-9]+$' 2>&1 >/dev/null
	then
		INPUT_INDEX="storage_module_${flag_storage_index}_unpacked.index"
	else
		_error "'${flag_storage_index}' is not a number"
		_usage
		exit 1
	fi
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

# openssl is required
openssl=$(which openssl)
test -z "${openssl}" && _error "openssl not found" && exit 1

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
		exit 1
	fi
	return 1
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
		_debug "${path} check with ${original_checksum}"
		payload="${original_checksum} ${path}"
		echo ${payload} | md5sum -c 2>&1 >/dev/null
		if test $? -eq 0
		then
			_debug "${path} md5 is valid"
			return 0
		else
			_error "${path} md5 is invalid"
			return 1
		fi
	fi
}

# this function will download files using v2 format.
# it also includes a way to check the size and the
# checksum without asking the server, but by using
# the data present in the partition list file.
_download_and_verify() {
	local base_url="${1}"
	# set the prefix with the path
	local path="${2}"
	local path_prefix="${PREFIX}${path}"
	local content_length="${3}"
	local checksum="${4}"
	local full_url="${base_url}/${path}"
	_debug "${path_prefix} to check and download"

	# only used for local check, using exit code from function
	local check_file_exist=1
	local check_file_size=3
	local check_file_checksum=1

	# check if the local file already exist
	# and if it's the case then check its size
	if test -f "${path_prefix}"
	then
		check_file_exist=0

		# if the file exists, extract its size
		file_size=$(stat -c '%s' "${path_prefix}")
		_check_size ${path_prefix} ${content_length} ${file_size}
		check_file_size=$?
	fi

	# check the checksum using md5sum
	if test "${flag_check}" && test "${check_file_exist}" -eq 0
	then
		_check_md5 ${path_prefix} ${checksum}
		check_file_checksum=$?
	fi

	# force flag will always download the file
	# even if the size or the checksums are valid
	if test "${flag_force}"
	then
		_debug "${path_prefix} force download..."
		_download_with_retry "${full_url}" "${path_prefix}" "${content_length}"
	else
		if test ${check_file_exist} -ne 0
		then
			_debug "${path_prefix} download file, it does not exist"
			_download_with_retry "${full_url}" "${path_prefix}" "${content_length}"
		elif test ${flag_check} && test ${check_file_checksum} -eq 0
		then
			_debug "${path_prefix} checksums are the same, don't download ${full_url}"
			return 0
		elif test ${flag_check} && test ${check_file_checksum} -ne 0
		then
			_debug "${path_prefix} checksums are not the same, download ${full_url}"
			_download_with_retry "${full_url}" "${path_prefix}" "${content_length}"
		elif test ${check_file_size} -eq 0
		then
			_debug "${path_prefix} files don't have size size, download ${full_url}"
			return 0
		elif test ${check_file_size} -eq 0
		then
			_debug "${path_prefix} files are not the same, download ${full_url}"
			_download_with_retry "${full_url}" "${path_prefix}" "${content_length}"
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

# check if a file is locally present
_local() {
	local base_url=${1}
	local relative_path=${PREFIX}${2}
	local length=${3}
	local checksum=${4}
	if test -e "${relative_path}"
	then
		_debug "${relative_path}: ok"
		return 0
	else
		_error "${relative_path}: missing"
		return 1
	fi
}

# convert base64 input to hexadecimal,
# mainly used to convert md5 base64 checksum
_base64_to_hex() {
	openssl base64 -d | xxd -ps
}

# check if a file a present remotely, and compare
# it with the values from the index files
_presence() {
	local base_url="${1}"
	local relative_path="${2}"
	local length=${3}
	local checksum="${4}"
	local target="${base_url}/${relative_path}"

	set -o pipefail
	curl -sfI "${target}" | {
		while read line
		do
			# sanitize line, it seems curl adds special
			# chars at the end of each line.
			line=$(echo $line | sed 's![^[:print:]\t]!!g')

			if printf "${line}" | grep "^content-length" 2>&1 >/dev/null
			then
				remote_length=$(printf "${line}" \
					| awk '{ print $NF }')
			fi

			if printf "${line}" | grep "^x-amz-meta-md5chksum" 2>&1 >/dev/null
			then
				remote_checksum=$(printf "${line}" \
					| awk '{ print $NF }' \
					| _base64_to_hex)
			fi
		done

		let l=${length}
		let rl=${remote_length}
		if [[ "${remote_length}" ]] && [[ $l -ne $rl ]]
		then
			_debug "${relative_path} (error) local_length=${l} remote_checksum=${rl}"
			return 255
		fi

		if [[ "${remote_checksum}" && "${checksum}" != "${remote_checksum}" ]]
		then
			_debug "${relative_path} (error) local_checksum=${checksum} remote_checksum=${remote_checksum}"
			return 254
		fi

		return 0
	}

	local ret=$?
	case "$ret" in
		0) _debug "${relative_path} ${target}: ok";;
		22) _error "${relative_path} missing in ${base_url}";;
		254) _error "${relative_path} checksum issue";;
		255) _error "${relative_path} length issue";;
		*) _error "${relative_path} unknown error in ${base_url}";;
	esac
	exit ${ret}
}

# download index from
if test "${INPUT_INDEX}"
then
	if test -e "${INPUT_INDEX}" && test ! "${flag_force}"
	then
		INPUT_FILE="${INPUT_INDEX}"
	else
		storage_module_target="${BASE_URL}/${INPUT_INDEX}"
		_debug "${storage_module_target} download"
		curl -sqf "${storage_module_target}" -o "${INPUT_INDEX}"
		if test $? -ne 0
		then
			_error "${storage_module_target} can't be downloaded"
			exit 1
		fi
		INPUT_FILE="${INPUT_INDEX}"
	fi

fi

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
while read -r line
do
	# Skip empty lines and remove comments
	test -z "${line}" && continue
	echo "${line}" | grep -E '^#' >/dev/null && continue

	if ! _check_v2_line "${line}"
	then
		_error "unsupported format (line ${line_number}): ${line}"
		exit 2
	fi

	# Cleanup relative path
	relative_path=$(echo "${line}" | awk '{print $1}' | sed -E s!^/+!!)

	# Get content length
	content_length=$(echo "${line}" | awk '{print $2}')

	# Get the checksum
	checksum=$(echo "${line}" | awk '{print $3}')

	# Craft the full params list
	params="${BASE_URL} ${relative_path} ${content_length} ${checksum}"

	if test "${flag_presence}"
	then
		_presence ${params} &
		concurrent_downloads=$((concurrent_downloads+1))
		lines_processed=$((lines_processed+1))
	elif test "${flag_local}"
	then
		_local ${params} &
		concurrent_downloads=$((concurrent_downloads+1))
		lines_processed=$((lines_processed+1))
	else

		# Create the directory structure for the file
		mkdir -p "$(dirname "${relative_path}")"

		# Call download_and_verify function in the background
		echo "Downloading ($((lines_processed+1))/$total_lines) $line to $relative_path"
		_debug "executing ${callback} ${params}"
		_download_and_verify ${params} &

		# Increment the concurrent downloads counter
		concurrent_downloads=$((concurrent_downloads+1))
		lines_processed=$((lines_processed+1))
	fi

	# If max concurrent downloads reached, wait for one to finish before continuing
	if (( concurrent_downloads >= max_concurrent_downloads ))
	then
		wait -n
		ret=$?
		if [ $ret -ne 0 ]
		then
			_error "child process exited with $ret"
		fi
		((concurrent_downloads--))
	fi

	line_number=$((line_number+1))
done < "${INPUT_FILE}"

# Wait for any remaining background downloads to complete
wait

# After the last batch of downloads, print the remaining lines one last time
remaining_lines=$((total_lines - lines_processed))
echo "Remaining files to download: $remaining_lines"
