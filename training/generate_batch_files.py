'''
Description
@uthor: Sadegh Sadeghi Tabas (sadegh.tabas@noaa.gov)
Revision history:
    -20240201: Sadegh Tabas, initial code
'''
import os
import sys
import glob
import argparse
import subprocess
from datetime import datetime, timedelta
import xarray as xr
import numpy as np

class GFSDataProcessor:
    def __init__(self, start_datetime, end_datetime, num_pressure_levels=13, download_source='nomads', output_directory=None, download_directory=None, keep_downloaded_data=True):
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime
        self.num_levels = num_pressure_levels
        self.output_directory = output_directory
        self.file_formats = ['0p25.f000',] # , '0p25.f001', '0p25.f006'

    def process_data(self):
        # Define the directory where your GRIB2 files are located
        data_directory = self.local_base_directory

        # Create a dictionary to specify the variables, levels, and whether to extract only the first time step (if needed)
        variables_to_extract = {
            '.f000': {
                ':HGT:': {
                    'levels': [':surface:'],
                    'first_time_step_only': True,  # Extract only the first time step
                },
                ':TMP:': {
                    'levels': [':2 m above ground:'],
                },
                ':PRMSL:': {
                    'levels': [':mean sea level:'],
                },
                ':VGRD|UGRD:': {
                    'levels': [':10 m above ground:'],
                },
                ':SPFH|VVEL|VGRD|UGRD|HGT|TMP:': {
                    'levels': [':(50|100|150|200|250|300|400|500|600|700|850|925|1000) mb:'],
                },
                ':APCP:': {  # APCP
                    'levels': [':surface:'],
            },
            '''  
            '.f006': {
                ':LAND:': {
                    'levels': [':surface:'],
                    'first_time_step_only': True,  # Extract only the first time step
                },
                ':APCP:': {  # APCP
                    'levels': [':surface:'],
                },
            }
            '''
        }
        if self.num_levels == 37:
            variables_to_extract['.f000'][':SPFH|VVEL|VGRD|UGRD|HGT|TMP:']['levels'] = [':(1|2|3|5|7|10|20|30|50|70|100|125|150|175|200|225|250|300|350|400|450|500|550|600|650|700|750|775|800|825|850|875|900|925|950|975|1000) mb:']

        # Create an empty list to store the extracted datasets
        extracted_datasets = []
        files = []
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
                    # Loop through each GRIB2 file (.f000, .f001, .f006)
                    for file_extension, variable_data in variables_to_extract.items():
                        for variable, data in variable_data.items():
                            levels = data['levels']
                            first_time_step_only = data.get('first_time_step_only', False)  # Default to False if not specified

                            grib2_file = os.path.join(subfolder_path, f'gdas.t{hour}z.pgrb2.0p25{file_extension}')
                    
                            # Extract the specified variables with levels from the GRIB2 file
                            for level in levels:
                                output_file = f'{variable}_{level}_{date_folder}_{hour}{file_extension}.nc'
                                files.append(output_file)
                                # Use wgrib2 to extract the variable with level
                                wgrib2_command = ['wgrib2', '-nc_nlev', f'{self.num_levels}', grib2_file, '-match', f'{variable}', '-match', f'{level}', '-netcdf', output_file]
                                subprocess.run(wgrib2_command, check=True)

                                # Open the extracted netcdf file as an xarray dataset
                                ds = xr.open_dataset(output_file)

                                if variable == '^(597):':
                                    ds['time'] = ds['time'] - np.timedelta64(6, 'h')

                                # If specified, extract only the first time step
                                if variable not in [':LAND:', ':HGT:']:
                                    extracted_datasets.append(ds)
                                else:
                                    if first_time_step_only:
                                        # Append the dataset to the list
                                        ds = ds.isel(time=0)
                                        extracted_datasets.append(ds)
                                        variables_to_extract[file_extension][variable]['first_time_step_only'] = False
                                
                                # Optionally, remove the intermediate GRIB2 file
                                # os.remove(output_file)
        print("Merging grib2 files:")
        ds = xr.merge(extracted_datasets)
        
        print("Merging process completed.")
        
        print("Processing, Renaming and Reshaping the data")
        # Drop the 'level' dimension
        ds = ds.drop_dims('level')

        # Rename variables and dimensions
        ds = ds.rename({
            'latitude': 'lat',
            'longitude': 'lon',
            'plevel': 'level',
            'HGT_surface': 'geopotential_at_surface',
            'LAND_surface': 'land_sea_mask',
            'PRMSL_meansealevel': 'mean_sea_level_pressure',
            'TMP_2maboveground': '2m_temperature',
            'UGRD_10maboveground': '10m_u_component_of_wind',
            'VGRD_10maboveground': '10m_v_component_of_wind',
            'APCP_surface': 'total_precipitation_6hr',
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
        ds['geopotential_at_surface'] = ds['geopotential_at_surface'].squeeze('batch')
        ds['land_sea_mask'] = ds['land_sea_mask'].squeeze('batch')

        # Update geopotential unit to m2/s2 by multiplying 9.80665
        ds['geopotential_at_surface'] = ds['geopotential_at_surface'] * 9.80665
        ds['geopotential'] = ds['geopotential'] * 9.80665

        # Update total_precipitation_6hr unit to (m) from (kg/m^2) by dividing it by 1000kg/m³
        ds['total_precipitation_6hr'] = ds['total_precipitation_6hr'] / 1000
        
        # Define the output NetCDF file
        date = (self.start_datetime + timedelta(hours=6)).strftime('%Y%m%d%H')
        steps = str(len(ds['time']))

        if self.output_directory is None:
            self.output_directory = os.getcwd()  # Use current directory if not specified
        output_netcdf = os.path.join(self.output_directory, f"source-gdas_date-{date}_res-0.25_levels-{self.num_levels}_steps-{steps}.nc")

        # Save the merged dataset as a NetCDF file
        ds.to_netcdf(output_netcdf)
        print(f"Saved output to {output_netcdf}")
        for file in files:
            os.remove(file)

        print(f"Process completed successfully, your inputs for GraphCast model generated at:\n {output_netcdf}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download and process GDAS data")
    parser.add_argument("start_datetime", help="Start datetime in the format 'YYYYMMDDHH'")
    parser.add_argument("end_datetime", help="End datetime in the format 'YYYYMMDDHH'")
    parser.add_argument("-l", "--levels", help="number of pressure levels, options: 13, 37", default="13")
    parser.add_argument("-m", "--method", help="method to extact variables from grib2, options: wgrib2, pygrib", default="wgrib2")
    parser.add_argument("-s", "--source", help="the source repository to download gdas grib2 data, options: nomads (up-to-date), s3", default="s3")
    parser.add_argument("-o", "--output", help="Output directory for processed data")
    parser.add_argument("-d", "--download", help="Download directory for raw data")
    parser.add_argument("-k", "--keep", help="Keep downloaded data (yes or no)", default="no")

    args = parser.parse_args()

    start_datetime = datetime.strptime(args.start_datetime, "%Y%m%d%H")
    end_datetime = datetime.strptime(args.end_datetime, "%Y%m%d%H")
    num_pressure_levels = int(args.levels)
    download_source = args.source
    method = args.method
    output_directory = args.output
    download_directory = args.download
    keep_downloaded_data = args.keep.lower() == "yes"

    data_processor = GEFSDataProcessor(start_datetime, end_datetime, num_pressure_levels, download_source, output_directory, download_directory, keep_downloaded_data)
