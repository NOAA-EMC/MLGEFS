import os
import argparse
from time import time
from datetime import timedelta
import dataclasses
import functools
import re
import haiku as hk
import jax
import numpy as np
import xarray as xr
import pandas as pd
#import boto3

from graphcast import rollout
from graphcast import xarray_jax
from graphcast import normalization
from graphcast import checkpoint
from graphcast import data_utils
from graphcast import gencast
from graphcast import denoiser
from graphcast import nan_cleaning

from utils.nc2grib import Netcdf2Grib

class GenCast: 
    def __init__(
        self,
        pretrained_model_path,
        gdas_data_path,
        output_dir=None,
        num_pressure_levels=13,
        forecast_length=30,
        num_ensemble_members=32, 
    ):
        self.pretrained_model_path = pretrained_model_path
        self.gdas_data_path = gdas_data_path
        self.forecast_length = forecast_length
        self.num_pressure_levels = num_pressure_levels
        self.num_ensemble_members = num_ensemble_members
      
        if output_dir is None:
            self.output_dir = os.path.join(os.getcwd(), f"forecasts_{str(self.num_pressure_levels)}_levels")  # Use current directory if not specified
        else:
            self.output_dir = os.path.join(output_dir, f"forecasts_{str(self.num_pressure_levels)}_levels")
        os.makedirs(self.output_dir, exist_ok=True)

        self.load_pretrained_model()
        self.load_stats()
        self.load_gdas_data()

    def load_pretrained_model(self):
        """Load pre-trained GenCast model
        """
        model_weights_path = f"{self.pretrained_model_path}/params/GenCast 0p25deg <2019.npz"
        with open(model_weights_path, "rb") as f:
            ckpt = checkpoint.load(f, gencast.CheckPoint)
        
        self.params = ckpt.params
        self.state = {}

        self.task_config = ckpt.task_config
        self.sampler_config = ckpt.sampler_config
        self.noise_config = ckpt.noise_config
        self.noise_encoder_config = ckpt.noise_encoder_config

        #Replace attention mechanism
        splash_spt_cfg = ckpt.denoiser_architecture_config.sparse_transformer_config
        tbd_spt_cfg = dataclasses.replace(splash_spt_cfg, attention_type='triblockdiag_mha', mask_type='full')
        self.denoiser_architecture_config = dataclasses.replace(ckpt.denoiser_architecture_config, sparse_transformer_config=tbd_spt_cfg)

        print("Model description:\n", ckpt.description, "\n")
        print("Model license:\n", ckpt.license, "\n")

    def load_gdas_data(self):
        #with open(DATA_PATH, "rb") as f:
        #  example_batch = xarray.load_dataset(f).compute()
        self.current_batch = xr.load_dataset(self.gdas_data_path).compute()
        self.dates =  pd.to_datetime(self.current_batch.datetime.values)

        #expand time dimensiont for long lead time forecast
        if (self.forecast_length + 2) > len(self.current_batch['time']):
            diff = int(self.forecast_length + 2 - len(self.current_batch['time']))
            ds = self.current_batch

            curr_time_range = ds['time'].values.astype('timedelta64[ns]')
            new_time_range = (np.arange(len(curr_time_range) + diff) * np.timedelta64(12, 'h')).astype('timedelta64[ns]')
            ds = ds.reindex(time = new_time_range)

            curr_datetime_start = ds['datetime'][0,0].values
            new_datetime_range = curr_datetime_start + np.arange(len(new_time_range)) * np.timedelta64(12, 'h')
            ds['datetime'][0] = new_datetime_range
            #ds = ds.assign_coords({"time": new_time_coords})

            self.current_batch = ds

        #Extract input
        self.inputs, self.targets, self.forcings = data_utils.extract_inputs_targets_forcings(
            self.current_batch, target_lead_times=slice("12h", f"{self.forecast_length*12}h"), # Only 1AR training.
            **dataclasses.asdict(self.task_config))

        print("All Examples:  ", self.current_batch.dims.mapping)
        print("Train Inputs:  ", self.inputs.dims.mapping)
        print("Train Targets: ", self.targets.dims.mapping)
        print("Train Forcings:", self.forcings.dims.mapping)

    def load_stats(self):
        #load normalization data
        with open(f"{self.pretrained_model_path}/stats/diffs_stddev_by_level.nc", "rb") as f:
            self.diffs_stddev_by_level = xr.load_dataset(f).compute()

        with open(f"{self.pretrained_model_path}/stats/mean_by_level.nc", "rb") as f:
            self.mean_by_level = xr.load_dataset(f).compute()

        with open(f"{self.pretrained_model_path}/stats/stddev_by_level.nc", "rb") as f:
            self.stddev_by_level = xr.load_dataset(f).compute()

        with open(f"{self.pretrained_model_path}/stats/min_by_level.nc", "rb") as f:
            self.min_by_level = xr.load_dataset(f).compute()

    def load_model(self):
        #Build jitted functions, and possibly initialize ramdom weights
        def construct_wrapped_gencast(sampler_config, task_config, denoiser_architecture_config, noise_config, noise_encoder_config):
            """Constructs and wraps the GenCast Predictor."""
            predictor = gencast.GenCast(
                sampler_config=sampler_config,
                task_config=task_config,
                denoiser_architecture_config=denoiser_architecture_config,
                noise_config=noise_config,
                noise_encoder_config=noise_encoder_config,
            )

            predictor = normalization.InputsAndResiduals(
                predictor,
                diffs_stddev_by_level=self.diffs_stddev_by_level,
                mean_by_level=self.mean_by_level,
                stddev_by_level=self.stddev_by_level,
            )

            predictor = nan_cleaning.NaNCleaner(
                predictor=predictor,
                reintroduce_nans=True,
                fill_value=self.min_by_level,
                var_to_clean='sea_surface_temperature',
            )

            return predictor


        @hk.transform_with_state
        def run_forward(inputs, targets_template, forcings):
          predictor = construct_wrapped_gencast()
          return predictor(inputs, targets_template=targets_template, forcings=forcings)


        self.run_forward_jitted = jax.jit(
            lambda rng, i, t, f: run_forward.apply(self.params, self.state, rng, i, t, f)[0]
        )

    def get_predictions(self):
        """Autoregressiver rollout
        """
        self.load_model()

        rng = jax.random.PRNGKey(0)
        # We fold-in the ensemble member, this way the first N members should always
        # match across different runs which use take the same inputs
        # regardless of total ensemble size.
        rngs = np.stack(
            [jax.random.fold_in(rng, i) for i in range(self.num_ensemble_members)], axis=0)

        run_forward_pmap = xarray_jax.pmap(self.run_forward_jitted, dim="sample")

        chunks = []
        for chunk in rollout.chunked_prediction_generator_multiple_runs(
            predictor_fn=run_forward_pmap,
            rngs=rngs,
            inputs=self.inputs,
            targets_template=self.targets * np.nan,
            forcings=self.forcings,
            num_steps_per_chunk = 1,
            num_samples = self.num_ensemble_members,
            pmap_devices=jax.local_devices()
        ):
            chunks.append(chunk)

        predictions = xr.combine_by_coords(chunks)
        
        self.save_outputs(predictions)

    def save_outputs(self, predictions):

        converter = Netcdf2Grib()

        predictions = predictions.drop_vars('sea_surface_temperature')

        for im in range(predictions.shape[0]):
            dataset = predictions.isel(sample=im)
            #predictions.to_netcdf(f'forecast_gdas_2024022700_{num_ensemble_members}members_{forecast_length}steps.nc')
            converter.save_grib2(self.dates, dataset, im, self.output_dir)

    """  def upload_to_s3(self, keep_data):
        s3 = boto3.client('s3')
        
        # Extract date and time information from the input file name
        input_file_name = os.path.basename(self.gdas_data_path)
        
        date_start = input_file_name.find("date-")

        # Check if "date-" is found in the input_file_name
        if date_start != -1:
            date_start += len("date-")  # Move to the end of "date-"
            date = input_file_name[date_start:date_start + 8]  # Extract 8 characters as the date
        
            time_start = date_start + 8  # Move to the character after the date
            time = input_file_name[time_start:time_start + 2]  # Extract 2 characters as the time


        # Define S3 key paths for input and output files
        input_s3_key = f'graphcastgfs.{date}/{time}/input/{self.gdas_data_path}'

        # Upload input file to S3
        s3.upload_file(self.gdas_data_path, self.s3_bucket_name, input_s3_key)
        
        # Upload output files to S3
        # Iterate over all files in the local directory and upload each one to S3
        s3_prefix = f'graphcastgfs.{date}/{time}/forecasts_{self.num_pressure_levels}_levels'
        
        for root, dirs, files in os.walk(self.output_dir):

            for file in files:
                local_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_path, self.output_dir)
                s3_path = os.path.join(s3_prefix, relative_path)
            
                # Upload the file
                s3.upload_file(local_path, self.s3_bucket_name, s3_path)

        print("Upload to s3 bucket completed.")

        # Delete local files if keep_data is False
        if not keep_data:
            # Remove forecast data from the specified directory
            print("Removing input and forecast data from the specified directory...")
            try:
                os.system(f"rm -rf {self.output_dir}")
                os.remove(self.gdas_data_path)
                print("Local input and output files deleted.")
            except Exception as e:
                print(f"Error removing input and forecast data: {str(e)}") """

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run GenCast model.")
    parser.add_argument("-i", "--input", help="input file path (including file name)", required=True)
    parser.add_argument("-w", "--weights", help="parent directory of the gencast params and stats", required=True)
    parser.add_argument("-l", "--length", help="length of forecast (12-hourly), for example 30 is 15 days forecast", required=True)
    parser.add_argument("-m", "--member", help="gefs member [c00, p01, ..., p30]", required=True)
    parser.add_argument("-o", "--output", help="output directory", default=None)
    parser.add_argument("-p", "--pressure", help="number of pressure levels", default=13)
    parser.add_argument("-u", "--upload", help="upload input data as well as forecasts to noaa s3 bucket (yes or no)", default = "no")
    parser.add_argument("-k", "--keep", help="keep input and output after uploading to noaa s3 bucket (yes or no)", default = "no")
    
    args = parser.parse_args()
    runner = GenCast(args.weights, args.input, args.output, int(args.pressure), int(args.length), args.member,)
    runner.get_predictions()
    
    upload_data = args.upload.lower() == "yes"
    keep_data = args.keep.lower() == "yes"
    
    if upload_data:
        runner.upload_to_s3(keep_data)
