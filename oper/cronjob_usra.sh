#!/bin/bash --login

source /scratch3/NCEPDEV/nems/Linlin.Cui/miniforge3/etc/profile.d/conda.sh
conda activate graphcast

python submit_mlgefs_job.py

