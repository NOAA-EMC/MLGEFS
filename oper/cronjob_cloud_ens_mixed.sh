#!/bin/bash
cd /lustre/EAGLE_ensemble/oper

source /lustre/EAGLE_ensemble/miniforge3/etc/profile.d/conda.sh
conda activate graphcast

python submit_job.py -w /lustre/EAGLE_ensemble
