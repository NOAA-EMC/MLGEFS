#!/bin/bash --login

# load necessary modules
module use /contrib/spack-stack/spack-stack-1.9.1/envs/ue-oneapi-2024.2.1/install/modulefiles/Core/
module load stack-oneapi
module load awscli-v2/2.15.53

# Get TC tracker data
aws s3 --profile gcgfs sync s3://noaa-nws-graphcastgfs-pds/hurricanes/syndat /scratch3/NCEPDEV/nems/Linlin.Cui/Tests/MLGEFSv1.0/oper/tracker/syndat

module purge

source /scratch3/NCEPDEV/nems/Linlin.Cui/miniforge3/etc/profile.d/conda.sh
conda activate graphcast

cd /scratch3/NCEPDEV/nems/Linlin.Cui/Tests/MLGEFSv1.0/oper
python submit_mlgefs_job.py

