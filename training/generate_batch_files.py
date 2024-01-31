'''
Description
@uthor: Sadegh Sadeghi Tabas (sadegh.tabas@noaa.gov)
Revision history:
    -20240201: Sadegh Tabas, initial code
'''
import os
import subprocess
import argparse
import xarray as xr
import numpy as np
import copy
import re

class GEFSDataProcessor:
    def __init__(self, input_directory, output_directory, variables, num_pressure_levels=13):
        self.input_directory = input_directory
        self.output_directory = output_directory
        self.variables = variables
        self.num_levels = num_pressure_levels
        self.file_formats = ['1p00.f000']
        os.makedirs(self.output_directory, exist_ok=True)

    def process_data(self):
        data_directory = self.input_directory
        grib2_file_extension = self.file_formats[0]

        grib2_file_list = [file for file in os.listdir(data_directory) if file.endswith(grib2_file_extension)]

        for grib2_file in grib2_file_list:
            variables_to_extract = copy.deepcopy(self.variables)
            extracted_datasets = []
            files = []

            for file_extension, variable_data in variables_to_extract.items():
                for variable, data in variable_data.items():
                    levels = data['levels']
                    first_time_step_only = data.get('first_time_step_only', False)

                    for level in levels:
                        output_file = f'{variable}_{level}_{file_extension}_{grib2_file}.nc'
                        files.append(output_file)
                        grib2_file_path = os.path.join(data_directory, grib2_file)
                        wgrib2_command = ['wgrib2', '-nc_nlev', f'{self.num_levels}', grib2_file_path, '-match', f'{variable}', '-match', f'{level}', '-netcdf', output_file]
                        subprocess.run(wgrib2_command, check=True)
                        ds = xr.open_dataset(output_file)

                        if variable not in [':LAND:', ':HGT:']:
                            extracted_datasets.append(ds)
                        else:
                            if first_time_step_only:
                                ds = ds.isel(time=0)
                                extracted_datasets.append(ds)
                                variables_to_extract[file_extension][variable]['first_time_step_only'] = False

            print("Merging grib2 files:")
            ds = xr.merge(extracted_datasets)
            print("Merging process completed.")

            print("Processing, Renaming and Reshaping the data")
            ds = self.reshape_ds(ds)

            base_name, _ = os.path.splitext(grib2_file)
            output_file_name = self.generate_new_file_name(base_name)
            output_netcdf = os.path.join(self.output_directory, output_file_name)

            ds.to_netcdf(output_netcdf)
            print(f"Saved output to {output_netcdf}")

            for file in files:
                os.remove(file)

            print(f"Process completed successfully, your inputs for GraphCast model generated at:\n {output_netcdf}")

    def reshape_ds(self, ds):
        ds = ds.drop_dims('level')
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

        ds = ds.assign_coords(datetime=ds.time)

        ds['lat'] = ds['lat'].astype('float32')
        ds['lon'] = ds['lon'].astype('float32')
        ds['level'] = ds['level'].astype('int32')

        ds['time'] = ds['time'] - ds.time[0]

        ds = ds.expand_dims(dim='batch')
        ds['datetime'] = ds['datetime'].expand_dims(dim='batch')

        ds['geopotential_at_surface'] = ds['geopotential_at_surface'].squeeze('batch')
        ds['land_sea_mask'] = ds['land_sea_mask'].squeeze('batch')

        ds['geopotential_at_surface'] = ds['geopotential_at_surface'] * 9.80665
        ds['geopotential'] = ds['geopotential'] * 9.80665

        ds['total_precipitation_6hr'] = ds['total_precipitation_6hr'] / 1000

        return ds

    def generate_new_file_name(self, original_file_name):
        # Extract relevant information using regular expressions
        match = re.match(r"gec(\d{2})\.t(\d{2})z\.pgrb2\.(\d{8})\.(\dp\d{2})", original_file_name)
        
        if match:
            model_run = match.group(1)
            forecast_time = match.group(2)
            date = match.group(3)
            resolution = match.group(4)
            
            new_file_name = f"{date}{forecast_time}.{resolution}.{self.num_levels}lvl.nc"
            return new_file_name
        else:
            # Handle the case where the original file name doesn't match the expected pattern
            print(f"Warning: Unable to parse file name - {original_file_name}")
            return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process GEFS data to generate GraphCast inputs")
    parser.add_argument("-i", "--input", help="directory to grib2 files")
    parser.add_argument("-o", "--output", help="Output directory for processed data")
    parser.add_argument("-l", "--levels", help="number of pressure levels, options: 13, 31", default="13")

    args = parser.parse_args()
    input_directory = args.input
    output_directory = args.output
    num_pressure_levels = int(args.levels)

    variables = {
        '.f000': {
            ':HGT:': {'levels': [':surface:'], 'first_time_step_only': True},
            ':TMP:': {'levels': [':2 m above ground:']},
            ':PRMSL:': {'levels': [':mean sea level:']},
            ':VGRD|UGRD:': {'levels': [':10 m above ground:']},
            ':SPFH|VVEL|VGRD|UGRD|HGT|TMP:': {'levels': [':(50|100|150|200|250|300|400|500|600|700|850|925|1000) mb:']},
            ':APCP:': {'levels': [':surface:']},
            ':LAND:': {'levels': [':surface:'], 'first_time_step_only': True},
        }
    }

    if num_pressure_levels == 31:
        variables['.f000'][':SPFH|VVEL|VGRD|UGRD|HGT|TMP:']['levels'] = [':(1|2|3|5|7|10|20|30|50|70|100|150|200|250|300|350|400|450|500|550|600|650|700|750|800|850|900|925|950|975|1000) mb:']

    data_processor = GEFSDataProcessor(input_directory, output_directory, variables, num_pressure_levels)
    data_processor.process_data()

