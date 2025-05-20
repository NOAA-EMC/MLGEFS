#!/bin/bash --login

##SBATCH --nodes=1
##SBATCH --cpus-per-task=36  # Use all available CPU cores
####SBATCH --time=4:00:00  # Adjust this to your estimated run time
##SBATCH --job-name="$gefs_member"
##SBATCH --output="$gefs_member_output.txt"
##SBATCH --error="$gefs_member_error.txt"
###SBATCH --partition=compute

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
curr_datetime=$( date -d "$datetime 6 hour ago" "+%Y%m%d%H" )
prev_datetime=$( date -d "$datetime 12 hour ago" "+%Y%m%d%H" )

#curr_datetime=$forecast_date
## Extract the date and hour parts
#year=${curr_datetime:0:4}
#month=${curr_datetime:4:2}
#day=${curr_datetime:6:2}
#hour=${curr_datetime:8:2}
#date_string=$(printf "%04d-%02d-%02d %02d:00:00" $year $month $day $hour)
#prev_datetime=$( date -d "$date_string 6 hour ago" "+%Y%m%d%H" )


echo "Current state: $curr_datetime"
echo "6 hours earlier state: $prev_datetime"

forecast_length=64
echo "forecast length: $forecast_length"

num_pressure_levels=13
echo "number of pressure levels: $num_pressure_levels"

# Set Miniconda path
#export PATH="/contrib/Sadegh.Tabas/miniconda3/bin:$PATH"

# Activate Conda environment
source /lustre2/Linlin.Cui/miniforge3/etc/profile.d/conda.sh
conda activate graphcast

# going to the model directory
#cd /contrib/Linlin.Cui/operational/MLGEFSv1.0/MLGEFS/oper_test

start_time=$(date +%s)
echo "start runing gdas utility to generate graphcast inputs for: $curr_datetime"
# Run the Python script gdas.py with the calculated times
python3 gen_gefs_ics.py "$prev_datetime" "$curr_datetime" "$gefs_member" -l "$num_pressure_levels" -o /lustre2/Linlin.Cui/MLGEFSv1.0/"$curr_datetime"/ -d /lustre2/Linlin.Cui/MLGEFSv1.0/"$curr_datetime"/

end_time=$(date +%s)  # Record the end time in seconds since the epoch

# Calculate and print the execution time
execution_time=$((end_time - start_time))
echo "Execution time for gdas_utility.py: $execution_time seconds"

start_time=$(date +%s)
echo "start runing graphcast to get real time 10-days forecasts for: $curr_datetime"
# Run another Python script
python3 run_graphcast_ens.py -i /lustre2/Linlin.Cui/MLGEFSv1.0/"$curr_datetime"/source-ge"$gefs_member"_date-"$curr_datetime"_res-0.25_levels-"$num_pressure_levels"_steps-2.nc -o /lustre2/Linlin.Cui/MLGEFSv1.0/"$curr_datetime"/ -w /contrib/graphcast/NCEP -m "$gefs_member" -c "$config_path" -l "$forecast_length" -p "$num_pressure_levels" -u no -k yes

# Upload to s3 bucekt
cd /lustre2/Linlin.Cui/MLGEFSv1.0/"$curr_datetime"
#move input file to input/
#../job.sh 

#delete input file so that save the cost of uploading
rm source-ge"$gefs_member"_date-"$curr_datetime"_res-0.25_levels-"$num_pressure_levels"_steps-2.nc
cd ..

## Extract the date and hour parts
ymd=${curr_datetime:0:8}
hour=${curr_datetime:8:2}

#upload to noaa-nws-graphcastgfs-pds
aws s3 --profile gcgfs sync $curr_datetime/forecasts_13_levels_${gefs_member}_model_${model_id}/ s3://noaa-nws-graphcastgfs-pds/EAGLE_ensemble/pmlgefs."$ymd"/"$hour"/forecasts_13_levels_${gefs_member}_model_${model_id}/

# Delete outputs
#rm -r $curr_datetime/forecasts_13_levels_${gefs_member}_model_${model_id}

end_time=$(date +%s)  # Record the end time in seconds since the epoch

# Calculate and print the execution time
execution_time=$((end_time - start_time))
echo "Execution time for running graphcast and uploading to the bucket: $execution_time seconds"
