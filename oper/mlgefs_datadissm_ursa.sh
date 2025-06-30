#!/bin/bash --login

# load necessary modules
module use /contrib/spack-stack/spack-stack-1.9.1/envs/ue-oneapi-2024.2.1/install/modulefiles/Core/
module load stack-oneapi
module load awscli-v2/2.15.53
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


echo "Current state: $curr_datetime"
echo "6 hours earlier state: $prev_datetime"

start_time=$(date +%s)
echo "Uploading member $gefs_member for: $curr_datetime"
## Extract the date and hour parts
ymd=${curr_datetime:0:8}
hour=${curr_datetime:8:2}
aws s3 --profile gcgfs sync $curr_datetime/forecasts_13_levels_${gefs_member}_model_${model_id}/ s3://noaa-nws-graphcastgfs-pds/EAGLE_ensemble/pmlgefs."$ymd"/"$hour"/forecasts_13_levels_${gefs_member}_model_${model_id}/

rm -r $curr_datetime/forecasts_13_levels_${gefs_member}_model_${model_id}

end_time=$(date +%s)  # Record the end time in seconds since the epoch

# Calculate and print the execution time
execution_time=$((end_time - start_time))
echo "Execution time for uploading: $execution_time seconds"
