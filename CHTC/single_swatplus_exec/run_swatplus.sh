#!/bin/bash

# the setting of -e means the script will exit when any failures occur
set -e

echo "--- Job started on $(hostname) at $(date) ---"

# Decompress TxtInOut.tar.gz, which has been transferred by HTCondor to current work directory
echo "Decompressing TxtInOut.tar.gz..."
tar -xzf TxtInOut.tar.gz

# Run SWAT+ model
#    Now we are in the parent directory of TxtInOut,
#    so we need first go into the TxtInOut and run the exact name of SWAT+.
#    The exact name of SWAT+ is in /usr/local/bin of the container.
echo "Running SWAT+ executable..."
cd TxtInOut
/app/swatplus-61.0.2.11-ifx-lin_x86_64-Rel
cd ..

echo "Model run finished."

# Check output files
echo "Checking for essential output files..."

files_to_check=(
    "TxtInOut/basin_wb_day.txt"
    "TxtInOut/channel_sd_day.txt"
    "TxtInOut/channel_sd_mon.txt"
)

for file_to_check in "${files_to_check[@]}"; do
    if [ -f "$file_to_check" ]; then
        echo "Found: $file_to_check"
    else
        echo "Warning: $file_to_check NOT found. Creating 0-byte dummy file."
        touch "$file_to_check"
    fi
done

echo "File check complete."

echo "--- Job finished at $(date) ---"
exit 0
