#!/bin/bash

# Specify the directory where the archives are located
archive_directory="/lustre/Sadegh.Tabas/GEFSv12/"

# Change to the archive directory
cd "$archive_directory" || exit 1

# Loop through each .tar file
for tar_file in *.tar; do
    # Extract the base name without extension
    folder_name="${tar_file%.tar}"

    # Create a folder with the same name as the .tar file
    mkdir -p "$folder_name"

    # Extract the contents of the .tar file into the corresponding folder
    tar -xf "$tar_file" -C "$folder_name"

    # Optionally, you can also extract the corresponding .tar.idx file if it exists
    idx_file="$tar_file.idx"
    if [ -e "$idx_file" ]; then
        cp "$idx_file" "$folder_name/"
    fi
done
