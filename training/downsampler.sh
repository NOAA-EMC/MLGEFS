#!/bin/bash

# Sadegh Tabas, initial commit


# load module lib
source /etc/profile.d/modules.sh

# load necessary modules
module use /contrib/spack-stack/envs/ufswm/install/modulefiles/Core/
module load stack-intel
module load wgrib2
module list

while getopts ":i:o:" opt; do
  case $opt in
    i)
      input_dir=$OPTARG
      ;;
    o)
      output_dir=$OPTARG
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
    :)
      echo "Option -$OPTARG requires an argument." >&2
      exit 1
      ;;
  esac
done

if [ -z "$input_dir" ] || [ -z "$output_dir" ]; then
  echo "Usage: $0 -i <input_directory> -o <output_directory>"
  exit 1
fi


# Ensure the output directory exists
mkdir -p "$output_dir"

# Loop through files in the input directory
for file in "$input_dir"/*.f000; do
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
        wgrib2 "$input_path" -new_grid_winds earth -new_grid_interpolation neighbor -new_grid latlon 0:360:1.0 -90:181:1.0 "$output_path"

        echo "Downsampled $input_path to $output_path"
    fi
done

# Indicate that the job is done
echo "Job completed successfully!"
