# Global EAGLE ensemble
This project is part of Experimental AI Global and Limited-area Ensemble project (EAGLE), which is ensemble GraphCast tuned on GDAS and initialized with GEFSv12. This repo contains scripts to run an ensemble-based cascaded version of the GraphCast weather model for the Global Ensemble Forecast System (GEFS). [Model weights](https://noaa-nws-graphcastgfs-pds.s3.amazonaws.com/index.html#EAGLE_ensemble/model_weights/) were obtained from training/finetuning GraphCast on GDAS dataset.

## Table of Contents
- [Overview](#overview)
- [Prerequisites and Installation](#prerequisites-and-installation)
- [Usage](#usage)
- [Output](#output)
- [Contact](#contact)

## Overview

The National Centers for Environmental Prediction (NCEP) provides GEFS data that can be used for ensemble weather prediction and analysis. Currently, a cron job is set up to transfer GEFS data from WCOSS2 to `NOAA-NCEPDEV-NONE-CA-UFS-CPLDCLD`
bucket. 

## Installation

Creating an environment from an environment.yml file

```bash
conda env create -f environment.yml
```

To activate the env:
```bash
conda activate graphcast
```

## Usage
### Download model weights and statistics files:
```bash
aws s3 cp --recursive s3://noaa-nws-graphcastgfs-pds/EAGLE_ensemble/model_weights model_weights --no-sign-request
```

### Generate IC from an individual ensemble member:
```bash
python gen_gefs_ics.py prev_datetime curr_datetime gefs_member -l 13 -o /path/to/output -d /path/to/download -k no
```

### Run the model for an individual ensemble member:
```bash
python run_graphcast_ens.py -i /path/to/inputfile -o /path/to/output -w model_weights/stats -m gefs_member -c model_weights/params  -l forecast_length(steps) -p num_pressure_levels -u no -k yes
```
Slurm jobs for 31 members can be submitted with `oper/submit_jobs.py`. Change the env path in `oper/gcjob_cloud_ens.sh` accordingly, then run the script:
```bash
python submit_jobs.py -w /path/to/ens_weights
```

## Contact

For questions or issues, please contact:
    [Linlin.Cui@noaa.gov](mailto:Linlin.Cui@noaa.gov)\
    [Jun.Wang@noaa.gov](mailto:Jun.Wang@noaa.gov)
