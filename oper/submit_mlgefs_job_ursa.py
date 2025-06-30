import os
import socket
from datetime import datetime, timedelta
import argparse
import pathlib
from time import time
import subprocess
import json

import numpy as np

def get_job_id(command):
    result = subprocess.run(
        command, 
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    if result.returncode != 0:
        print("Job submission failed:", result.stderr)
        exit(1)

    job_id = result.stdout.strip().split()[-1]

    return job_id


def submit_slurm_run(member, param, model_id):

    #Step 1 - generate input file
    command1 = ['sbatch', '--nodes=1', '--ntasks=1', '--account=nems', '--partition=u1-service', \
        f'--job-name={getdata_member}', f'--output=slurm/getdata_{member}.out', \
        f'--error=slurm/getdata_{member}.err', '--exclusive', \
        f'--export=gefs_member={member},config_path={param},model_id={model_id}', 'mlgefs_prepdata_ursa.sh']

    job_id1 = get_job_id(command1)

    command2 = ['sbatch', f'--dependency=afterok:{job_id1}', '--nodes=1', '--account=nems', '--partition=u1-h100', \
        '--qos=gpuwf', '--time=30:00', f'--job-name={run_member}', f'--output=slurm/gcgfs_{member}.out', \
        f'--error=slurm/gcgfs_{member}.err', f'--export=gefs_member={member},config_path={param},model_id={model_id}', \
        'mlgefs_runfcst_ursa.sh']

    #Step 2 - run graphcast
    job_id2 = get_job_id(command2)

    command3 = ['sbatch', f'--dependency=afterok:{job_id2}', '--nodes=1', '--ntasks=1', '--account=nems', \
        '--partition=u1-service', f'--job-name={datadissm_member}', f'--output=slurm/datadissm_{member}.out', \
        f'--error=slurm/datadissm_{member}.err', f'--export=gefs_member={member},model_id={model_id}', \
        'mlgefs_datadissm_ursa.sh']

    #Step 3 - upload data to s3 bucket
    job_id3 = get_job_id(command3)

if __name__ == '__main__':

    hostname = socket.gethostname()
    if hostname.startswith('ufe'):
        param_path = '/scratch3/NCEPDEV/nems/Linlin.Cui/Tests/MLGEFSv1.0/oper/graphcast_gefs_params'
    elif hostname.startswith('linlincui'):
        param_path = '/lustre2/Linlin.Cui/MLGEFSv1.0/weights'
    else:
        raise NotImplementedError(f'{hostname} is not supported yet!')

    with open('model_weights_ursa.json', 'r') as file:
        models = json.load(file)

    for key, values in models.items():
        if key == '0':
            member = f'c{int(key):02d}'
        else:
            member = f'p{int(key):02d}'

        param = f'{param_path}/{values.get("params")}'
        submit_slurm_run(member, param, key)
