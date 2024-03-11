#!/bin/bash --login
#SBATCH --nodes=1
#SBATCH --cpus-per-task=30  # Use all available CPU cores
#SBATCH --time=4:00:00  # Adjust this to your estimated run time
#SBATCH --job-name="$gefs_member"
#SBATCH --output="$gefs_member_output.txt"
#SBATCH --error="$gefs_member_error.txt"
#SBATCH --partition=compute

# load module lib
# source /etc/profile.d/modules.sh

# load necessary modules
module use /contrib/spack-stack/envs/ufswm/install/modulefiles/Core/
module load stack-intel
module load wgrib2
module list

# Get the UTC hour and calculate the time in the format yyyymmddhh
current_hour=$(date -u +%H)
current_hour=$((10#$current_hour))

if (( $current_hour >= 0 && $current_hour < 6 )); then
    datetime=$(date -u -d 'today 00:00')
elif (( $current_hour >= 6 && $current_hour < 12 )); then
    datetime=$(date -u -d 'today 06:00')
elif (( $current_hour >= 12 && $current_hour < 18 )); then
    datetime=$(date -u -d 'today 12:00')
else
    datetime=$(date -u -d 'today 18:00')
fi

# Calculate time 6 hours before
#curr_datetime=$(date -u -d "$time" +'%Y%m%d%H')
curr_datetime=$( date -d "$datetime 12 hour ago" "+%Y%m%d%H" )
prev_datetime=$( date -d "$datetime 18 hour ago" "+%Y%m%d%H" )

curr_datetime=2024022700
prev_datetime=2024022618

echo "Current state: $curr_datetime"
echo "6 hours earlier state: $prev_datetime"

forecast_length=64
echo "forecast length: $forecast_length"

num_pressure_levels=13
echo "number of pressure levels: $num_pressure_levels"

# Set Miniconda path
#export PATH="/contrib/Sadegh.Tabas/miniconda3/bin:$PATH"

# Activate Conda environment
source /contrib/Sadegh.Tabas/miniconda3/etc/profile.d/conda.sh
conda activate mlwp

# going to the model directory
cd /contrib/Sadegh.Tabas/operational/MLGEFS/oper/

start_time=$(date +%s)
echo "start runing gdas utility to generate graphcast inputs for: $curr_datetime"
# Run the Python script gdas.py with the calculated times
python3 gen_gefs_ics.py "$prev_datetime" "$curr_datetime" "$gefs_member" -l "$num_pressure_levels"

end_time=$(date +%s)  # Record the end time in seconds since the epoch

# Calculate and print the execution time
execution_time=$((end_time - start_time))
echo "Execution time for gdas_utility.py: $execution_time seconds"

start_time=$(date +%s)
echo "start runing graphcast to get real time 10-days forecasts for: $curr_datetime"
# Run another Python script
python3 run_graphcast.py -i source-ge"$gefs_member"_date-"$curr_datetime"_res-0.25_levels-"$num_pressure_levels"_steps-2.nc -w /contrib/graphcast/NCEP -m "$gefs_member" -l "$forecast_length" -p "$num_pressure_levels" -u no -k yes

end_time=$(date +%s)  # Record the end time in seconds since the epoch

# Calculate and print the execution time
execution_time=$((end_time - start_time))
echo "Execution time for graphcast: $execution_time seconds"
