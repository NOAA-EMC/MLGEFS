#!/bin/bash

input_dir="fv3anl2000_00z"
output_dir="fv3anl2000_00z_1d"

# Ensure the output directory exists
mkdir -p "$output_dir"

# Loop through files in the input directory
for file in "$input_dir"/*.pgrb2; do
    if [ -f "$file" ]; then
        filename=$(basename "$file")
        
        # Extracting parts of the filename
        prefix="${filename%.0p25.*}"
        suffix="${filename##*.}"

        # Constructing the new output filename with 1.0 degree resolution
        output_filename="${prefix}.1p00.${suffix}"
        
        # Full paths
        input_path="$input_dir/$filename"
        output_path="$output_dir/$output_filename"

        # Use wgrib2 to perform the downsampling (adjust the resolution factor as needed)
        wgrib2 "$input_path" -new_grid_winds earth -new_grid_interpolation budget -new_grid latlon 0:360:1.0 -90:181:1.0 "$output_path"

        echo "Downsampled $input_path to $output_path"
    fi
done

