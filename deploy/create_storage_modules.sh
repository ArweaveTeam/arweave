#!/bin/bash

# Script to create storage module directories and symlinks
# Usage: ./create_storage_modules.sh [-e] [-u user] <directory_root> <start_partition> <end_partition> <mining_address>

set -e

# Parse arguments
DRY_RUN=false
CHOWN_USER=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -e)
            DRY_RUN=true
            shift
            ;;
        -u)
            CHOWN_USER="$2"
            shift 2
            ;;
        *)
            break
            ;;
    esac
done

if [ $# -ne 4 ]; then
    echo "Usage: $0 [-e] [-u user] <directory_root> <start_partition> <end_partition> <mining_address>"
    echo "  -e         Dry run mode - only echo what would be done, don't create anything"
    echo "  -u user    Set ownership of created directories and symlinks to specified user"
    echo "Example: $0 /mnt/vol02 1 10 1seRanklLU_1VTGkEk7P0xAwMwGkD8aYi1"
    echo "Example: $0 -e /mnt/vol02 1 10 1seRanklLU_1VTGkEk7P0xAwMwGkD8aYi1"
    echo "Example: $0 -u arweave /mnt/vol02 1 10 1seRanklLU_1VTGkEk7P0xAwMwGkD8aYi1"
    echo "Example: $0 -e -u arweave /mnt/vol02 1 10 1seRanklLU_1VTGkEk7P0xAwMwGkD8aYi1"
    exit 1
fi

DIRECTORY_ROOT="$1"
START_PARTITION="$2"
END_PARTITION="$3"
MINING_ADDRESS="$4"

# Validate arguments
if ! [[ "$START_PARTITION" =~ ^[0-9]+$ ]] || ! [[ "$END_PARTITION" =~ ^[0-9]+$ ]]; then
    echo "Error: Start and end partition must be numbers"
    exit 1
fi

if [ "$START_PARTITION" -gt "$END_PARTITION" ]; then
    echo "Error: Start partition must be <= end partition"
    exit 1
fi

# Validate user exists if specified
if [ -n "$CHOWN_USER" ]; then
    if ! id "$CHOWN_USER" >/dev/null 2>&1; then
        echo "Error: User '$CHOWN_USER' does not exist on this system"
        exit 1
    fi
fi

# Function to execute commands with dry run support
execute_command() {
    local description="$1"
    local command="$2"
    
    echo "$description"
    if [ "$DRY_RUN" = false ]; then
        if ! eval "$command"; then
            echo "Error: Command failed: $command" >&2
            exit 1
        fi
    fi
}

# Function to count storage modules in a directory
count_storage_modules() {
    local dir="$1"
    local mining_addr="$2"
    
    if [ ! -d "$dir" ]; then
        echo -1  # Return -1 to indicate directory doesn't exist
        return
    fi
    
    # Count directories matching storage_module_*_MINING_ADDRESS pattern
    count=$(find "$dir" -maxdepth 1 -type d -name "storage_module_*_${mining_addr}" 2>/dev/null | wc -l)
    echo "$count"
}

# Function to find the first volume directory with < 4 storage modules
find_available_volume() {
    local root="$1"
    local mining_addr="$2"
    
    local volume_num=1
    while true; do
        local volume_dir="${root}-$(printf "%02d" $volume_num)"
        local count=$(count_storage_modules "$volume_dir" "$mining_addr")
        
        # Skip if directory doesn't exist (count == -1)
        if [ "$count" -ne -1 ] && [ "$count" -lt 4 ]; then
            echo "$volume_dir"
            return
        fi
        
        volume_num=$((volume_num + 1))
        
        # Safety check to prevent infinite loop
        if [ $volume_num -gt 100 ]; then
            echo "Error: Could not find available volume directory after checking 100 volumes" >&2
            exit 1
        fi
    done
}

# Function to create storage module directory
create_storage_module() {
    local volume_dir="$1"
    local partition_num="$2"
    local mining_addr="$3"
    
    local storage_dir="${volume_dir}/storage_module_$(printf "%02d" $partition_num)_${mining_addr}"
    
    # Check if volume directory exists
    if [ ! -d "$volume_dir" ]; then
        echo "Error: Volume directory $volume_dir does not exist" >&2
        return 1
    fi
    
    # Create storage module directory
    execute_command "Creating directory: $storage_dir" "mkdir -p '$storage_dir'"
    
    # Set ownership if user specified
    if [ -n "$CHOWN_USER" ]; then
        execute_command "Setting ownership: $storage_dir -> $CHOWN_USER" "chown '$CHOWN_USER:$CHOWN_USER' '$storage_dir'"
    fi
    
    # Return the path via a global variable to avoid output mixing
    CREATED_STORAGE_DIR="$storage_dir"
}

# Function to create symlink in current directory
create_symlink() {
    local target_dir="$1"
    local partition_num="$2"
    local mining_addr="$3"
    
    local link_name="storage_module_$(printf "%02d" $partition_num)_${mining_addr}"
    
    execute_command "Creating symlink: $link_name -> $target_dir" "ln -sf '$target_dir' '$link_name'"
    
    # Set ownership of symlink if user specified
    if [ -n "$CHOWN_USER" ]; then
        execute_command "Setting symlink ownership: $link_name -> $CHOWN_USER" "chown -h '$CHOWN_USER:$CHOWN_USER' '$link_name'"
    fi
}

# Main logic
if [ "$DRY_RUN" = true ]; then
    echo "=== DRY RUN MODE - No files will be created ==="
fi

echo "Creating storage modules from partition $START_PARTITION to $END_PARTITION"
echo "Directory root: $DIRECTORY_ROOT"
echo "Mining address: $MINING_ADDRESS"
if [ -n "$CHOWN_USER" ]; then
    echo "Owner: $CHOWN_USER"
fi
echo

current_volume=""
modules_in_current_volume=0
CREATED_STORAGE_DIR=""

for partition in $(seq $START_PARTITION $END_PARTITION); do
    # Find available volume if we don't have one or current is full
    if [ -z "$current_volume" ] || [ $modules_in_current_volume -ge 4 ]; then
        current_volume=$(find_available_volume "$DIRECTORY_ROOT" "$MINING_ADDRESS")
        modules_in_current_volume=$(count_storage_modules "$current_volume" "$MINING_ADDRESS")
        echo "Using volume directory: $current_volume (current modules: $modules_in_current_volume)"
    fi
    
    # Create storage module directory
    if create_storage_module "$current_volume" $partition "$MINING_ADDRESS"; then
        # Create symlink if storage module was created successfully
        create_symlink "$CREATED_STORAGE_DIR" $partition "$MINING_ADDRESS"
        modules_in_current_volume=$((modules_in_current_volume + 1))
    else
        # If creation failed (volume doesn't exist), reset current volume and retry
        echo "Retrying with next available volume..."
        current_volume=""
        modules_in_current_volume=0
        # Retry this partition
        partition=$((partition - 1))
    fi
done

echo
echo "Storage module creation complete!"
