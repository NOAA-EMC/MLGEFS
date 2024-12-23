import os
import sys
from time import time
import glob
import argparse
import subprocess
from datetime import datetime, timedelta
import re
import pathlib
from typing import Union

import boto3
import xarray as xr
import numpy as np
from botocore.config import Config
from botocore import UNSIGNED
import pygrib
#import requests
#from bs4 import BeautifulSoup


class GFSDataProcessor:
    def __init__(
        self, 
        start_datetime: datetime = None, 
        interval: Union[int, float, timedelta] = 12.0, 
        num_pressure_levels: int = 13, 
        download_source = 'nomads', 
        output_directory = None, 
        download_directory = None, 
        keep_downloaded_data: bool = True, 
    ):
        self.start_datetime = start_datetime
        self.interval = interval if isinstance(interval, timedelta) else timedelta(hours=interval)
        self.num_levels = num_pressure_levels
        self.download_source = download_source
        self.output_directory = output_directory
        self.download_directory = download_directory
        self.keep_downloaded_data = keep_downloaded_data

        self.root_directory = 'gdas'

        self.date_2steps = [start_datetime - self.interval, start_datetime]

        # Specify the local directory where you want to save the files
        if self.download_directory is None:
            self.local_base_directory = os.path.join(os.getcwd(), self.bucket+'_'+str(self.num_levels))  # Use current directory if not specified
        else:
            self.local_base_directory = os.path.join(self.download_directory, self.bucket+'_'+str(self.num_levels))

    def download_data(self):

        current_datetime = self.start_datetime
        
        while current_datetime >= (self.start_datetime - timedelta(days=1)):
            print(current_datetime)

            if self.download_source == 's3':
                self.from_s3bucket(current_datetime)
            else:
                self.nomads(current_datetime)

            current_datetime -= timedelta(hours=6)


    def from_s3bucket(self, target_datetime):

        timestr = f'{target_datetime.strftime("%Y%m%d")}/{target_datetime.strftime("%H")}'
        local_directory = pathlib.Path(f'{self.local_base_directory}/{timestr}')
        local_directory.mkdir(parents=True, exist_ok=True)

        #s3_prefix = f'{self.root_directory}.{target_datetime.strftime("%Y%m%d")}/{target_datetime.strftime("%H")}/'

        for fh in range(0, 12,  6):
            obj_key = f'{self.root_directory}.{timestr}/atmos/gdas.t{target_datetime.strftime("%H")}z.pgrb2.0p25.f{fh:03d}'
            local_filename = f'{local_directory}/gdas.t{target_datetime.strftime("%H")}z.pgrb2.0p25.f{fh:03d}'

            print(obj_key)
            print(local_filename)

            try:
                self.s3.download_file(self.bucket, obj_key, local_filename)
            except:
                print(f'Error in downloading file {obj_key}!')

    def process_data_with_wgrib2(self):

        # Create a dictionary to specify the variables, levels, and whether to extract only the first time step (if needed)
        variables_to_extract = {
            'f000': {
                'HGT': {
                'levels': ['surface'],
                #    'first_time_step_only': True,  # Extract only the first time step
                },
                'LAND': {
                    'levels': ['surface'],
                },
                'TMP1': {
                    'levels': ['2 m above ground'],
                },
                'TMP2': {
                    'levels': ['surface'],
                },
                'PRMSL': {
                    'levels': ['mean sea level'],
                },
                'VGRD|UGRD': {
                    'levels': ['10 m above ground'],
                },
                'SPFH|VVEL|VGRD|UGRD|HGT|TMP': {
                    'levels': ['(50|100|150|200|250|300|400|500|600|700|850|925|1000) mb'],
                },
            },
            'f006': {
                'APCP': {  # APCP
                    'levels': ['surface'],
                },
            },
        }

        if self.num_levels == 37:
            variables_to_extract[':SPFH|VVEL|VGRD|UGRD|HGT|TMP:']['levels'] = [':(125|175|225|775|825|875) mb:']
       
        # Create an empty list to store the extracted datasets
        extracted_datasets = []
        files = []
        print("Start extracting variables and associated levels from grib2 files:")
        # Loop through each folder (e.g., gdas.yyyymmdd)
        #date_folders = sorted(next(os.walk(data_directory))[1])
        for file_extension, variables_data in variables_to_extract.items():

            grib2_files = sorted(glob.glob(f'{self.local_base_directory}/**/*.{file_extension}', recursive=True))

            for i, grib2_file in enumerate(grib2_files):
                for varname, varattr in variables_data.items():

                        if varname.startswith('TMP'):
                            varname2 = f':{varname[:-1]}:'
                        elif varname == 'APCP':
                            varname2 = '^(597):'
                        else:
                            varname2 = f':{varname}:'

                        levels = varattr['levels']
                            
                        # Extract the specified variables with levels from the GRIB2 file
                        for level in levels:
                            output_file = f'{varname}_{level.replace(" ", "")}_{self.num_levels}_{i}.nc'
                            files.append(output_file)
                            
                            # Extracting levels using regular expression
                            matches = re.findall(r'\d+', level)
                            
                            # Convert the extracted matches to integers
                            curr_levels = [int(match) for match in matches]
                            
                            # Get the number of levels
                            number_of_levels = len(curr_levels)
                            
                            # Use wgrib2 to extract the variable with level
                            wgrib2_command = ['wgrib2', '-nc_nlev', f'{number_of_levels}', grib2_file, '-match', f'{varname2}', '-match', f':{level}:', '-netcdf', output_file]
                            subprocess.run(wgrib2_command, check=True)

                            # Open the extracted netcdf file as an xarray dataset
                            ds = xr.open_dataset(output_file)

                            if varname == 'APCP':
                                ds['time'] = ds['time'] - np.timedelta64(6, 'h')

                            # If specified, extract only the first time step
                            extracted_datasets.append(ds)
                            
                            # Optionally, remove the intermediate GRIB2 file
                            # os.remove(output_file)
        print("Merging grib2 files:")
        ds = xr.merge(extracted_datasets)
        
        print("Merging process completed.")
        
        print("Processing, Renaming and Reshaping the data")
        # Drop the 'level' dimension
        ds = ds.drop_dims('level')

        ds['total_precipitation_12hr'] = ds['APCP_surface'].cumsum(axis=0)
        s = ds.drop_vars('APCP_surface')

        ds['TMP_surface'][:] =  np.ma.masked_array(ds['TMP_surface'], ds['LAND_surface'])

        ds = ds.sel(time=self.date_2steps)

        # Rename variables and dimensions
        ds = ds.rename({
            'latitude': 'lat',
            'longitude': 'lon',
            'plevel': 'level',
            'HGT_surface': 'geopotential_at_surface',
            'LAND_surface': 'land_sea_mask',
            'PRMSL_meansealevel': 'mean_sea_level_pressure',
            'TMP_2maboveground': '2m_temperature',
            'TMP_surface': 'sea_surface_temperature',
            'UGRD_10maboveground': '10m_u_component_of_wind',
            'VGRD_10maboveground': '10m_v_component_of_wind',
            'HGT': 'geopotential',
            'TMP': 'temperature',
            'SPFH': 'specific_humidity',
            'VVEL': 'vertical_velocity',
            'UGRD': 'u_component_of_wind',
            'VGRD': 'v_component_of_wind'
        })

        # Assign 'datetime' as coordinates
        ds = ds.assign_coords(datetime=ds.time)
        
        # Convert data types
        ds['lat'] = ds['lat'].astype('float32')
        ds['lon'] = ds['lon'].astype('float32')
        ds['level'] = ds['level'].astype('int32')

        # Adjust time values relative to the first time step
        ds['time'] = ds['time'] - ds.time[0]

        # Expand dimensions
        ds = ds.expand_dims(dim='batch')
        ds['datetime'] = ds['datetime'].expand_dims(dim='batch')

        # Squeeze dimensions
        ds['geopotential_at_surface'] = ds['geopotential_at_surface'].isel(time=0).squeeze('batch')
        ds['land_sea_mask'] = ds['land_sea_mask'].isel(time=0).squeeze('batch')

        # Update geopotential unit to m2/s2 by multiplying 9.80665
        ds['geopotential_at_surface'] = ds['geopotential_at_surface'] * 9.80665
        ds['geopotential'] = ds['geopotential'] * 9.80665

        # Update total_precipitation_6hr unit to (m) from (kg/m^2) by dividing it by 1000kg/m³
        ds['total_precipitation_12hr'] = ds['total_precipitation_12hr'] / 1000

        
        # Define the output NetCDF file
        date = self.start_datetime.strftime('%Y%m%d%H')
        steps = str(len(ds['time']))

        if self.output_directory is None:
            self.output_directory = os.getcwd()  # Use current directory if not specified
        output_netcdf = os.path.join(self.output_directory, f"source-gdas_date-{date}_res-0.25_levels-{self.num_levels}_steps-{steps}.nc")

        # Save the merged dataset as a NetCDF file
        ds.to_netcdf(output_netcdf)
        print(f"Saved output to {output_netcdf}")
        for file in files:
            os.remove(file)
            
        # Optionally, remove downloaded data
        if not self.keep_downloaded_data:
            self.remove_downloaded_data()

        print(f"Process completed successfully, your inputs for GraphCast model generated at:\n {output_netcdf}")

    def process_data_with_pygrib(self):
        # Define the directory where your GRIB2 files are located
        data_directory = self.local_base_directory

        #Get time-varying variables
        variables_to_extract = {
            'f000': {
                '2t': {
                    'typeOfLevel': 'heightAboveGround',
                    'level': 2,
                },
                'prmsl': {
                    'typeOfLevel': 'meanSea',
                    'level': 0,
                },
                '10u, 10v': {
                    'typeOfLevel': 'heightAboveGround',
                    'level': 10,
                },
                't, orog, lsm': {
                    'typeOfLevel': 'surface',
                    'level': 0,
                    
                },
                'w, u, v, q, t, gh': {
                    'typeOfLevel': 'isobaricInhPa',
                    'level': [50, 100, 150, 200, 250, 300, 400, 500, 600, 700, 850, 925, 1000],
                },
            },
            'f006': {
                'tp': {  # total precipitation 
                    'typeOfLevel': 'surface',
                    'level': 0,
                },
            }
        }

        # Create an empty list to store the extracted datasets
        mergeDSs = []
        print("Start extracting variables and associated levels from grib2 files:")
        # Loop through each folder (e.g., gdas.yyyymmdd)
        date_folders = sorted(next(os.walk(data_directory))[1])
        for date_folder in date_folders:
            date_folder_path = os.path.join(data_directory, date_folder)

            # Loop through each hour (e.g., '00', '06', '12', '18')
            for hour in ['00', '06', '12', '18']:
                subfolder_path = os.path.join(date_folder_path, hour)

                # Check if the subfolder exists before processing
                if os.path.exists(subfolder_path):

                    mergeDAs = []

                    for file_extension, variables in variables_to_extract.items():
                        pattern = os.path.join(subfolder_path, f'gdas.t*{file_extension}')
                        # Use glob to search for files matching the pattern
                        matching_files = glob.glob(pattern)
                        
                        # Check if there's exactly one matching file
                        if len(matching_files) == 1:
                            fname = matching_files[0]
                            print("Found file:", fname)
                        else:
                            print("Error: Found multiple or no matching files.")

                        #open grib file
                        grbs = pygrib.open(fname)

                        for key, value in variables.items():

                            variable_names = key.split(', ')
                            levelType = value['typeOfLevel']
                            desired_level = value['level']
                    
                            for var_name in variable_names:

                                print(f'Get variable {var_name} from file {fname}:')
                                da = self.get_dataarray(grbs, var_name, levelType, desired_level)

                                print(f'var {var_name}, time is {da.time}')
                                mergeDAs.append(da)

                    ds = xr.merge(mergeDAs)

                    mergeDSs.append(ds)
                    ds.close()

        #Concatenate ds
        ds = xr.concat(mergeDSs, dim='time')

        # #Get 2D static variables
        # grbfiles = glob.glob(f'{data_directory}/*/*/*.f000')
        # grbfiles.sort()
        # #Get lsm/orog from the first file
        # grbs = pygrib.open(grbfiles[0])
        # levelType = 'surface'
        # desired_level = 0
        # for var_name in ['lsm', 'orog']:
        #     da = self.get_dataarray(grbs, var_name, levelType, desired_level)
        #     ds = xr.merge([ds, da])

        ds['total_precipitation_12hr'] = ds['tp'].cumsum(axis=0)
        ds['tmpsfc'][:] =  np.ma.masked_array(ds['tmpsfc'], ds['lsm'])
        ds.drop_vars('tp')

        ds = ds.sel(time=self.date_2steps)
        
        ds = ds.rename({
            'lsm': 'land_sea_mask',
            'orog': 'geopotential_at_surface',
            'prmsl': 'mean_sea_level_pressure',
            '2t': '2m_temperature',
            '10u': '10m_u_component_of_wind',
            '10v': '10m_v_component_of_wind',
            'tmpsfc': 'sea_surface_temperature',
            'gh': 'geopotential',
            't': 'temperature',
            'q': 'specific_humidity',
            'w': 'vertical_velocity',
            'u': 'u_component_of_wind',
            'v': 'v_component_of_wind'
        })

        ds = ds.assign_coords(datetime=ds.time)

        # Adjust time values relative to the first time step
        ds['time'] = ds['time'] - ds.time[0]

        # Expand dimensions
        ds = ds.expand_dims(dim='batch')
        ds['datetime'] = ds['datetime'].expand_dims(dim='batch')

        # Squeeze dimensions
        ds['geopotential_at_surface'] = ds['geopotential_at_surface'].isel(time=0).squeeze('batch')
        ds['land_sea_mask'] = ds['land_sea_mask'].isel(time=0).squeeze('batch')

        # Update geopotential unit to m2/s2 by multiplying 9.80665
        ds['geopotential_at_surface'] = ds['geopotential_at_surface'] * 9.80665
        ds['geopotential'] = ds['geopotential'] * 9.80665

        # Update total_precipitation_6hr unit to (m) from (kg/m^2) by dividing it by 1000kg/m³
        ds['total_precipitation_12hr'] = ds['total_precipitation_12hr'] / 1000

        # Define the output NetCDF file
        #date = (self.start_datetime + timedelta(hours=6)).strftime('%Y%m%d%H')
        steps = str(len(ds['time']))

        if self.output_directory is None:
            self.output_directory = os.getcwd()  # Use current directory if not specified
        output_netcdf = os.path.join(self.output_directory, f"source-gdas_date-{self.start_datetime.strftime('%Y%m%d%H')}_res-0.25_levels-{self.num_levels}_steps-{steps}.nc")

        #final_dataset = ds.assign_coords(datetime=ds.time)
        ds.to_netcdf(output_netcdf)
        ds.close()
        
        # Optionally, remove downloaded data
        if not self.keep_downloaded_data:
            self.remove_downloaded_data()

        print(f"Process completed successfully, your inputs for GraphCast model generated at:\n {output_netcdf}")
            
    def remove_downloaded_data(self):
        # Remove downloaded data from the specified directory
        print("Removing downloaded grib2 data...")
        try:
            os.system(f"rm -rf {self.local_base_directory}")
            print("Downloaded data removed.")
        except Exception as e:
            print(f"Error removing downloaded data: {str(e)}")

    def get_dataarray(self, grbfile, var_name, level_type, desired_level):
        
        # Find the matching grib message
        variable_message = grbfile.select(shortName=var_name, typeOfLevel=level_type, level=desired_level)
    
        #change var_name for 't' at surface, otherwise it will conflict with t at pressure level
        if var_name == 't' and level_type == 'surface':
            var_name = 'tmpsfc'

        # create a netcdf dataset using the matching grib message
        lats, lons = variable_message[0].latlons()
        lats = lats[:,0]
        lons = lons[0,:]
    
        #check latitude range
        reverse_lat = False
        if lats[0] > 0:
            reverse_lat = True
            lats = lats[::-1]
    
        steps = variable_message[0].validDate
        #if var_name == 'tp':
        #    steps = steps - timedelta(hours=6)
        #precipitation rate has two stepType ('instant', 'avg'), use 'instant')
        if len(variable_message) > 2:
            data = []
            for message in variable_message:
                data.append(message.values)
            data = np.array(data)
            if reverse_lat:
                data = data[:, ::-1, :]
        else:
            data = variable_message[0].values
            if reverse_lat:
                data = data[::-1, :]
    
        if len(data.shape) == 2:
            da = xr.Dataset(
                data_vars={
                    var_name: (['lat', 'lon'], data.astype('float32'))
                },
                coords={
                    'lon': lons.astype('float32'),
                    'lat': lats.astype('float32'),
                    'time': steps,  
                }
            )
        elif len(data.shape) == 3:
            da = xr.Dataset(
                data_vars={
                    var_name: (['level', 'lat', 'lon'], data.astype('float32'))
                },
                coords={
                    'lon': lons.astype('float32'),
                    'lat': lats.astype('float32'),
                    'level': np.array(desired_level).astype('int32'),
                    'time': steps,  
                }
            )
    
        return da
        
    @property
    def s3(self):
        return boto3.client('s3', config=Config(signature_version=UNSIGNED))

    @property
    def bucket(self):
        return "noaa-gfs-bdp-pds"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download and process GDAS data")
    parser.add_argument("start_datetime", help="Start datetime in the format 'YYYYMMDDHH'")
    parser.add_argument("--interval", help="forecast interval (hours)", default="12")
    parser.add_argument("-l", "--levels", help="number of pressure levels, options: 13, 37", default="13")
    parser.add_argument("-m", "--method", help="method to extact variables from grib2, options: wgrib2, pygrib", default="wgrib2")
    parser.add_argument("-s", "--source", help="the source repository to download gdas grib2 data, options: nomads (up-to-date), s3", default="s3")
    parser.add_argument("-o", "--output", help="Output directory for processed data")
    parser.add_argument("-d", "--download", help="Download directory for raw data")
    parser.add_argument("-k", "--keep", help="Keep downloaded data (yes or no)", default="no")

    args = parser.parse_args()

    start_datetime = datetime.strptime(args.start_datetime, "%Y%m%d%H")
    interval = float(args.interval)
    num_pressure_levels = int(args.levels)
    download_source = args.source
    method = args.method
    output_directory = args.output
    download_directory = args.download
    keep_downloaded_data = args.keep.lower() == "yes"

    data_processor = GFSDataProcessor(start_datetime, interval, num_pressure_levels, download_source, output_directory, download_directory, keep_downloaded_data)
    data_processor.download_data()
    
    if method == "wgrib2":
      data_processor.process_data_with_wgrib2()
    elif method == "pygrib":
      data_processor.process_data_with_pygrib()
    else:
      raise NotImplementedError(f"Method {method} is not supported!")

