#!/bin/bash --login

# load modules
module use /contrib/spack-stack/spack-stack-1.9.1/envs/ue-oneapi-2024.2.1/install/modulefiles/Core/
module load stack-oneapi
module load awscli-v2/2.15.53
module list

COMROOT=/scratch3/NCEPDEV/stmp/Linlin.Cui/ptmp/com
num_pressure_levels=13

echo "Current state: $curr_datetime"

start_time=$(date +%s)
echo "Uploading member $gefs_member for: $curr_datetime"

## Extract the date and hour parts
ymd=${curr_datetime:0:8}
hour=${curr_datetime:8:2}

# upload forecast outputs
aws s3 --profile gcgfs sync $curr_datetime/forecasts_13_levels_${gefs_member}_model_${model_id}/ s3://noaa-nws-graphcastgfs-pds/EAGLE_ensemble/pmlgefs."$ymd"/"$hour"/forecasts_13_levels_${gefs_member}_model_${model_id}/
rm -r $curr_datetime/forecasts_13_levels_${gefs_member}_model_${model_id}

# upload input file
aws s3 --profile gcgfs cp $curr_datetime/source-ge"$gefs_member"_date-"$curr_datetime"_res-0.25_levels-"$num_pressure_levels"_steps-2.nc s3://noaa-nws-graphcastgfs-pds/EAGLE_ensemble/pmlgefs."$ymd"/"$hour"/forecasts_13_levels_${gefs_member}_model_${model_id}/input/
rm $curr_datetime/source-ge"$gefs_member"_date-"$curr_datetime"_res-0.25_levels-"$num_pressure_levels"_steps-2.nc

#upload tc tracker file
tctracker=$COMROOT/aigfs.$ymd/$hour/products/atmos/cyclone/tracks/g${gefs_member}p.t${hour}z.cyclone.trackatcfunix

if [ -s "$tctracker" ]; then
    echo "Uploading the tracker file to s3 bucket:"
    aws s3 --profile gcgfs cp $COMROOT/aigfs.$ymd/$hour/products/atmos/cyclone/tracks/g${gefs_member}p.t${hour}z.cyclone.trackatcfunix s3://noaa-nws-graphcastgfs-pds/EAGLE_ensemble/pmlgefs."$ymd"/"$hour"/forecasts_13_levels_${gefs_member}_model_${model_id}/g${gefs_member}p.t${hour}z.cyclone.trackatcfunix
else
    echo "tracker file for ${ymd}${hour}is empty!"
fi

end_time=$(date +%s)  # Record the end time in seconds since the epoch
# Calculate and print the execution time
execution_time=$((end_time - start_time))
echo "Execution time for uploading: $execution_time seconds"
