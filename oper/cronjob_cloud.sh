#!/bin/bash
sbatch --export=gefs_member=c00 gcjob_cloud.sh

# Loop from 01 to 30
for i in {01..30}; do
  sbatch --export=gefs_member=p$i gcjob_cloud.sh
done
