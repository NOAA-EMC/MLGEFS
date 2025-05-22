import argparse
import subprocess
import json

import numpy as np

def submit_slurm_run(member, param, model_id):

    command = f'sbatch --nodes=1 --cpus-per-task=36 --job-name={member} --output=output_{member}.txt ' \
    f'--error=error_{member}.txt --exclusive --export=gefs_member={member},config_path={param},model_id={model_id} gcjob_cloud_ens.sh'

    #print(command)
    subprocess.run(command, shell=True)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Submit slurm jobs")
    parser.add_argument("-w", "--weights", help="directory where model weights are saved")
    args = parser.parse_args()
    param_path = args.weights


    with open('model_weights.json', 'r') as file:
        models = json.load(file)

    for key, values in models.items():
        if key == '0':
            member = f'c{int(key):02d}'
        else:
            member = f'p{int(key):02d}'

        param = f'{param_path}/{values.get("params")}'
        submit_slurm_run(member, param, key)
