#!/bin/bash
cd /lustre2/Linlin.Cui/MLGEFSv1.0/oper

source /lustre2/Linlin.Cui/miniforge3/etc/profile.d/conda.sh
conda activate graphcast

python submit_job.py
