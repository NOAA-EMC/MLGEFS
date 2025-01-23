# MLGEFS: Machine Learning-based Global Ensemble Forecast System
This package contains scripts to run and an ensemble-based cascaded version of the GraphCast weather model for the Global Ensemble Forecast System (GEFS). It also provides the pre-trained model (weights) to run MLGEFS:

## Table of Contents
- [Overview](#overview)
- [Prerequisites and Installation](#prerequisites-and-installation)
- [Usage](#usage)
- [Output](#output)
- [Contact](#contact)

## Overview

The National Centers for Environmental Prediction (NCEP) provides GEFS data that can be used for ensemble weather prediction and analysis. 

## Prerequisites and Installation

To install the package, run the following commands:

```bash
conda create --name mlgefs python=3.10
```

```bash
conda activate mlgefs
```

```bash
pip install dm-tree boto3 xarray netcdf4
```

```bash
conda install --channel conda-forge cartopy
```

```bash
pip install --upgrade https://github.com/deepmind/graphcast/archive/master.zip
```

```bash
pip install ecmwflibs
````
```bash
pip install iris
````

```bash
pip install iris-grib
````

This will install the packages and most of their dependencies.


## Usage
### Input data
```bash
python gdas_utility.py YYYYMMDDHH -l 13 -m wgrib2(or pygrib) -s s3 -l /path/to/output -d /path/to/download -k no
````

### Run the model
```bash
python run_gencast.py -i /path/to/inputfile -w /path/to/model/ -l lead_time(steps) -m num_of_ensemble_members -o /path/to/output -p num_of_pls -u yes(no) -k yes(no)
````

## Output


## Contact

For questions or issues, please contact:
    [Linlin.Cui@noaa.gov](mailto:Linlin.Cui@noaa.gov)
    [Jun.Wang@noaa.gov](mailto:Jun.Wang@noaa.gov)
