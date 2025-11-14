'''
This module is a package for calling and processing data from various sources using different methods.
Specifically this module is meant to handle and package the data it handles in a way that can be easily
formated into a netCDF file. Once processed the data can be shipped into different files for plotting or 
general analysis.

Author: Nathan Beach
Last Commit: October 28, 2025
Last Modified: November 4, 2025
'''

#Imports required for class to work properly
from netCDF4 import Dataset
import numpy as np
import requests
import io
import os
import xarray as xr
from datetime import datetime, timedelta
import metpy as mp
import pandas as pd
import geopandas as gpd
import cartopy.crs as ccrs
from typing import Union, Optional, List, Tuple
import matplotlib.pyplot as plt

def web_fetch(api_url : Optional[str] = None,
                  url : Optional[str] = None,
                  variables : Optional[Union[List[str], str]] = None, 
                  time_range : Optional[Union[List[Union[str, pd.Timestamp]], str]] = None, 
                  spatial_bounds : Optional[List[Union[float, int]]] = None, 
                  levels : Optional[Union[List[str], str]] = None, 
                  format : Optional[str] = None) -> xr.Dataset:
    '''
    Fetches data from a given API or URL and processes it into a netCDF format. The specific parameters of the
    data to be fetched can be customized using the functions and argumetns provided.
    Parameters:
        api_url (str): The base URL of the API to fetch data from (e.g. https://example.com/Data/api).
        
        url (str): The specific endpoint or URL to fetch the data from (e.g. https://example.com/Data).
        
        variables (Optional str or List of str): The variables to be fetched from the data source (e.g. Temperature, Humidity, etc...).
        
        time_range (Optional List of str or pd.Timestamp): The time range for which the data is to be fetched (e.g. YYYY/MM/DD).
        
        spatial_bounds (Optional List of float or int): The spatial bounds (e.g. latitude and longitude) for the data.
        
        levels (Optional str or List of str): The levels (e.g. pressure or height levels) for the data to be fetched.
        
        format (Optional str): The desired format of the output data (e.g. 'netCDF', 'CSV', 'Pandas DataFrame', etc.).
    '''
    # Helper function for normalizing variables and levels
    def _normalize_list_input(data, param_name: str):
        """Converts string input to a list of strings, or validates existing list."""
        if data is None:
            return None
        
        if isinstance(data, str):
            return [data]
        
        if isinstance(data, list):
            try:
                # Attempt to convert all elements to string
                return [str(item) for item in data]
            except Exception as e:
                raise ValueError(f"The '{param_name}' parameter must contain items that can be converted to strings.") from e
        
        # Handle unexpected types (e.g., set, int, float)
        raise ValueError(f"The '{param_name}' parameter must be a string or a list of strings.")

    #=========================# PREPROCESSING LOGIC #=========================#

    ## 1. Check if either api_url or url is provided.
    if not api_url and not url:
        raise ValueError("Either 'api_url' or 'url' must be provided to fetch data. Please provide at least one of these parameters to proceed.")

    ## 2. Normalize and validate variables.
    variables = _normalize_list_input(variables, 'variables')

    ## 3. Normalize and validate levels.
    levels = _normalize_list_input(levels, 'levels')

    ## 4. Check and normalize time_range.
    if time_range:
        if not (isinstance(time_range, list) and len(time_range) >= 2):
            raise ValueError("The 'time_range' parameter must be a list with at least two elements (start and end point).")
        try:
            # Convert all elements to pandas Timestamps for internal consistency
            time_range = [pd.to_datetime(time) for time in time_range]
        except Exception as e:
            raise ValueError(f"The 'time_range' elements must be convertible to pd.Timestamp (e.g., 'YYYY-MM-DD'). Error: {e}")

    ## 5. Check and normalize spatial_bounds.
    if spatial_bounds:
        # Check for list/tuple first
        if not isinstance(spatial_bounds, (list, tuple)):
            # Added a clearer message for non-list/tuple/dict
            raise ValueError("The 'spatial_bounds' must be a list or tuple (e.g., [min_lon, min_lat, max_lon, max_lat]).")
        
        # If it's a list/tuple, ensure it has the expected number of coordinates
        if len(spatial_bounds) != 4:
            print("Warning: Common spatial bounds use 4 elements [min_lon, min_lat, max_lon, max_lat]. Proceeding with the provided list/tuple.")
            
        try:
            # Attempt to convert all elements to float
            spatial_bounds = [float(coord) for coord in spatial_bounds]
        except Exception as e:
            raise ValueError(f"The 'spatial_bounds' parameter must contain only values convertible to floats. Error: {e}")

    ## 6. Check and normalize format.
    # Standardize accepted formats to lowercase
    ACCEPTED_FORMATS = ['netcdf', 'csv', 'pandas dataframe', 'xarray dataset'] 

    if format:
        if not isinstance(format, str):
            try:
                format = str(format)
            except Exception as e:
                raise ValueError(f"The 'format' parameter must be a string. Conversion error: {e}")
        
        # Convert input format to lowercase for case-insensitive check
        format = format.lower().replace(' ', '') # 'pandas dataframe' -> 'pandasdataframe'
        
        # Check against the standardized list
        standardized_formats = [f.lower().replace(' ', '') for f in ACCEPTED_FORMATS]
        
        if format not in standardized_formats:
            raise ValueError(
                f"The 'format' parameter must be one of the following: {', '.join(ACCEPTED_FORMATS)}. "
                f"The format provided was '{format}'."
            )
            
    #=========================# API HANDLING LOGIC #=========================#
    # Note: The actual implementation of API calls would go here but is omitted until such a time when API
    # calling is required and can be properly tested.
    
    #=========================# URL HANDLING LOGIC #=========================#
    ## 1. Attempt to fetch data from the url provided in the most simple way possible.
    if url:
        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f"Failed to fetch data from the provided URL: {url}. \nError: {e}")
        xr_dataset = xr.open_dataset(io.BytesIO(response.content), decode_times=False)
        # Check if the dataset contains anything at all
        if xr_dataset is None or len(xr_dataset.data_vars) == 0:
            raise ValueError(f"The dataset fetched from the URL is empty or invalid: {url}.")
        with open(file='debug_dataset.nc', mode='wb') as f:
            f.write(response.content)
            
    
    return print("The processor worked")

def local_fetch(URL : Optional[str] = None,
                     CSV_PATH : Optional[Union[List[str], str]] = None,
                     TYPE : Optional[str] = None,
                     variables: Optional[Union[List[str], str]] = None,
                     time_range: Optional[Union[List[Union[str,pd.Timestamp]], str]] = None,
                     spatial_bounds: Optional[Union[List[Union[float, int]], Tuple[Union[float, int]]]] = None,
                     output:Optional[Union[os.PathLike, str]] = None,
                     format_method: Optional[str] = None) -> xr.Dataset:
    '''
    Fetches non-netCDF data from given URLs or local paths and processes it into a netCDF format. The specific parameters of
    data can be customized using the functions and arguments provided. None of the parsing arguments are required, but if at
    least one of the path arguments is not provided a ValueError will be raised.
    Parameters:
        URL (str): The specific URL to fetch the data from (e.g. https://example.com/Data). This can be in either CSV or JSON format preferably not in HTML or XML format but can be handled.
        
        CSV_PATH (str or list of str): The specific or local path on a local machine to fetch the data from (e.g. /home/user/data/file.csv). This can be in the format of CSV, JSON, or Excel files. The user can provide multiple paths in which case all paths will be concatenated into one xr.Dataset.
        
        TYPE (str): The type of data being fetched or the format of the data being fetched (e.g. 'CSV', 'JSON', 'Excel', etc...). This is not required but will make the processer more efficient.
        
        variables (str or list of str): The variables to be fetched from the data source (e.g. Temperature, Humidity, etc...). This can be multiple variables or a singular variable but these will be the variables that end up concatenated into the final xr.Dataset.
        
        time_range (list of str or pd.Timestamp): The time range for which the data is being fetched (e.g. YYYY/MM/DD). This can be a singular date or a date range but it is not required for the full dataset to be fetched.
        
        spatial_bounds (list or tuple of float or int): The spatial bounds (e.g. latitude and longitude) for the data. This is not required but will make the function more efficient and allow for more specific data analysis.
        
        output (str or os.PathLike): The output path where the final netCDF file will be saved. If not provided the file will be saved at the root drive of the processor file.
        
        format_method (str): The desired format of the output data (e.g. 'netCDF', 'CSV', 'Pandas DataFrame', etc...). If not provided the default format will be 'netCDF' for the output file and xr.Dataset for the output dataframe.
    '''
    
    # Helper functioin for normalizing variables and levels
    
    def _normalize_list_input(data, param_name: str):
        """Converts string input to a list of strings, or validates existing list."""
        if data is None:
            return None
        
        if isinstance(data, str):
            return [data]
        
        if isinstance(data, list):
            try:
                # Attempt to convert all elements to string
                return [str(item) for item in data]
            except Exception as e:
                raise ValueError(f"The '{param_name}' parameter must contain items that can be converted to strings.") from e
        
        # Handle unexpected types (e.g., set, int, float)
        raise ValueError(f"The '{param_name}' parameter must be a string or a list of strings.")
    
    #=========================# PREPROCESSING LOGIC #=========================#
    pass

def data_handler(PATH : Union[List[str], str],
                 PATH_METHOD : Optional[str] = None,
                 TYPE : Optional[str] = None,
                 variables: Optional[Union[List[str], str]] = None,
                 time_range: Optional[Union[List[Union[str, pd.Timestamp]], str]] = None,
                 time_filter: Optional[str] = None,
                 spatial_bounds: Optional[Union[List[Union[float, int]], Tuple[Union[float, int]]]] = None,
                 levels: Optional[Union[List[str], str]] = None,
                 regrid_resolution: Optional[Union[Tuple[Union[float, int]], List[Union[float, int]]]] = None,
                 regrid_method: Optional[str] = None,
                 statistic: str = 'mean',
                 squeeze_dims: bool = False,
                 format_method: Optional[str] = None) -> xr.Dataset:
    '''
    Handles and processes data from local paths into a netCDF format. The specific parameters of which can be set
    by the user using the arguments provided. None of the parsing arguments are required, but if PATH is not provided
    a ValueError will be raised.
    Parameters:
        PATH (str or list of str): The specific or absolute path on a local machine to fetch the data from (e.g. /home/user/data/file.nc4).
        
        PATH_METHOD (Optional str): The method by which the data will be fetched from the path provided (e.g. 'multiple', 'single', 'list', etc...).
        
        TYPE (Optional str): The type of data being fetched or the format of the data being fetched (e.g. 'netCDF', 'CSV', 'JSON', etc...).
        this is not required but will make the processer more efficient.
        
        variables (Optional str or list of str): The variables to be parsed out of the data source (e.g. Temperature, Humidity, etc...). This
        
        can be multiple variables or a singular variable and all of them will be concatenated into the final xr.Dataset.
        
        time_range (Optional list of str or pd.Timestamp): The time range for which the data is being parsed (e.g. YYYY/MM/DD).
        
        time_filter (Optional str): The method by which to filter the time range provided (e.g. 'between', 'before', 'after', etc...).
        
        spatial_bounds (optional list or tuple of float or int): The spatial bounds (e.g. latitude and longitude) for the data. This
        is not required but will make plotting the data in the future easier and allow for more specific data analysis.
        
        levels (Optional str or list of str): The levels (e.g. pressure or height) for the data to be parsed.
        
        regrid_resolution (Optional tuple or list of float or int): The resolution to which the data should be regridded.
        
        regrid_method (Optional str): The method by which the data will be regridded (e.g. 'nearest', 'linear', 'cubic', etc...).
        
        statistic (Optional str): The statistic to be applied when regridding the data (e.g. 'mean', 'median', 'max', etc...)
        
        squeeze_dims (Optional bool): Whether or not to squeeze the dimensions of the final xr.Dataset.
        
        format_method (Optional str): The desired format of the output data (e.g. 'netCDF', 'CSV', 'Pandas DataFrame', etc...).
    '''
    # Create two data trees one that takes all modifications and one that stores them by one modification.
    
    data_full = {}
    data_mod = {}
    
    # Helper functioin for normalizing variables and levels
    
    def _normalize_list_input(data, param_name: str):
        """Converts string input to a list of strings, or validates existing list."""
        if data is None:
            return None
        
        if isinstance(data, str):
            return [data]
        
        if isinstance(data, list):
            try:
                # Attempt to convert all elements to string
                return [str(item) for item in data]
            except Exception as e:
                raise ValueError(f"The '{param_name}' parameter must contain items that can be converted to strings.") from e
        
        # Handle unexpected types (e.g., set, int, float)
        raise ValueError(f"The '{param_name}' parameter must be a string or a list of strings.")
    
    #=========================# PREPROCESSING LOGIC #=========================#
    
    ## 1. Process the PATH variable to ensure that at least one path has been provided. Will Check the file extention later.
    
    if not PATH:
        raise ValueError("The 'PATH' parameter must be provided to fetch data. Please provide at least one path to proceed.")
    
    ## 2. Process the rest of the normalized variables with the helper function. Shouldn't need too much help from outside functions.
    
    path_method = _normalize_list_input(PATH_METHOD, 'PATH_METHOD')
    
    Type = _normalize_list_input(TYPE, 'TYPE')
    
    variables = _normalize_list_input(variables, 'variables')
    
    time_filter = _normalize_list_input(time_filter, 'time_filter')
    
    levels = _normalize_list_input(levels, 'levels')
    
    regrid_method = _normalize_list_input(regrid_method, 'regrid_method')
    
    statistic = _normalize_list_input(statistic, 'statistic')
    
    format_method = _normalize_list_input(format_method, 'format_method')
    
    ## 3. Reformat and normalize the abnormally formatted variables such as time and spatioal bounds. 
    
    if time_range:
        if not (isinstance(time_range, list) and len(time_range) >= 2):
            raise ValueError("The 'time_range' parameter must be a list with at least two elements (start and end point).")
        try:
            # Convert all elements to pandas Timestamps for internal consistency
            time_range = [pd.to_datetime(time) for time in time_range]
        except Exception as e:
            raise ValueError(f"The 'time_range' elements must be convertible to pd.Timestamp (e.g., 'YYYY-MM-DD'). Error: {e}")
    
    if spatial_bounds:
        # Check for list/tuple first
        if not isinstance(spatial_bounds, (list, tuple)):
            # Added a clearer message for non-list/tuple/dict
            raise ValueError("The 'spatial_bounds' must be a list or tuple (e.g., [min_lon, min_lat, max_lon, max_lat]).")
        
        # If it's a list/tuple, ensure it has the expected number of coordinates
        if len(spatial_bounds) != 4:
            print("Warning: Common spatial bounds use 4 elements [min_lon, min_lat, max_lon, max_lat]. Proceeding with the provided list/tuple.")
            
        try:
            # Attempt to convert all elements to float
            spatial_bounds = [float(coord) for coord in spatial_bounds]
        except Exception as e:
            raise ValueError(f"The 'spatial_bounds' parameter must contain only values convertible to floats. Error: {e}")
    
    #=========================# FETCHING LOGIC #=========================#
    ACCEPTED_FORMATS = ('.nc4', '.nc', 'netCDF', 'csv', '.xml')
    
    if PATH_METHOD:
        MULTIPLE_TYPE = ['list', 'multiple', 'index', 'many']
        SINGLULAR_TYPE = ['single', 'one', 'singular']
        if PATH_METHOD.lower() in MULTIPLE_TYPE:
            file_dirs = [os.path.join(PATH,dir) for dir in os.listdir(PATH) if dir.lower().endswith(ACCEPTED_FORMATS)]
            print(file_dirs)
            data = xr.open_mfdataset(file_dirs, data_vars='all')
        elif PATH_METHOD.lower() in SINGLULAR_TYPE:
            data = xr.open_dataset(PATH)
        else:
            raise KeyError(f"The method provided to parse your path wasn't recognized {PATH_METHOD}.\nPlease provide the method you prefer in type \n   multiple: {MULTIPLE_TYPE}\n   single: {SINGLULAR_TYPE}")

    elif PATH.endswith('/'):
        try:
            file_dirs = [os.path.join(PATH,dir) for dir in os.listdir(PATH) if dir.lower().endswith(ACCEPTED_FORMATS)]
            data = xr.open_mfdataset(file_dirs, data_vars='all')
            if not data:
                raise ValueError(f"When trying to parse through your files encountered one of the following problems: \
                                    \n   1. Your PATH doesn't contain any files of the format {ACCEPTED_FORMATS} \
                                    \n   2. Your PATH doesn't contain any files in which case '{file_dirs}' will be empty. \
                                    \n   3. Your PATH does contian files but the files don't contain any data.")
        except Exception as e:
            raise ValueError(f"An error occured after trying to parse through multiple files in the provided path.\nError: {e}")
    elif PATH.endswith(ACCEPTED_FORMATS):
        try:
            data = xr.open_dataset(PATH)
            if not data:
                raise ValueError(f"When trying to open your dataset from {PATH} encountered one of the following problems: \
                    \n   1. Your PATH doesn't point to any file with an accepted format {ACCEPTED_FORMATS} \
                    \n   2. Your PATH pointed to a file but the file didn't contain any data and thus your data is empty")
        except Exception as e:
            raise ValueError(f"When trying to open your file, ran into an error: {e}")
    
    data_mod = data.copy()
    
    #=========================# Spatial Filtering #=========================#
    
    if spatial_bounds:
        if len(spatial_bounds) != 4:
            raise ValueError("Latitude and Longitude must contain 4 elements: [min_lat, max_lat, min_lon, max_lon]")
        
        ## 1. Standardize the coordinate variables
        try:
            min_lat, max_lat, min_lon, max_lon = [float(coord) for coord in spatial_bounds]
        except Exception as e:
            raise ValueError(f"Lat/Lon parsing was broken by an error: {e}")
        
        ## 2. Identify coordinate names
        lat_dim = next((d for d in data.dims if d.lower() in ['lat', 'latitude']), 'lat')
        lon_dim = next((d for d in data.dims if d.lower() in ['lon', 'longitude']), 'lon')
        
        ## 3. Apply filtering
        try:
            data = data.sel(**{
                lat_dim: slice(min_lat, max_lat),
                lon_dim: slice(min_lon, max_lon)
            })
            data_spatial = data_mod.sel(**{
                lat_dim: slice(min_lat, max_lat),
                lon_dim: slice(min_lon, max_lon)
            })
        except KeyError as e:
            raise KeyError(f"An error occured when trying to filter the data array: \n   {e}")

                                 
class processor():
    def __init__(self, url,):
        pass
    def net_cdf_fetch(self):
        pass

spatial_bounds = (35, 42, -96, -87)

data = xr.open_dataset("/home/nsbtwd/Code/Thermo/net_cdf_files/MERRA2_HAQAST_CNN_L4_V1.20200908.nc4")

data_mod = data.copy()

if len(spatial_bounds) != 4:
    raise ValueError("Latitude and Longitude must contain 4 elements: [min_lat, max_lat, min_lon, max_lon]")

## 1. Standardize the coordinate variables
try:
    min_lat, max_lat, min_lon, max_lon = [float(coord) for coord in spatial_bounds]
except Exception as e:
    raise ValueError(f"Lat/Lon parsing was broken by an error: {e}")

## 2. Identify coordinate names
lat_dim = next((d for d in data.dims if d.lower() in ['lat', 'latitude']), 'lat')
lon_dim = next((d for d in data.dims if d.lower() in ['lon', 'longitude']), 'lon')
print(lat_dim)
print(lon_dim)

## 3. Apply filtering
try:
    data = data.sel(**{
        lat_dim: slice(min_lat, max_lat),
        lon_dim: slice(min_lon, max_lon)
    })
    data_spatial = data_mod.sel(**{
        lat_dim: slice(min_lat, max_lat),
        lon_dim: slice(min_lon, max_lon)
    })
except KeyError as e:
    raise KeyError(f"An error occured when trying to filter the data array: \n   {e}")

print(data)
print(data_spatial)
print(f"data times: \n{data['time']}")
fig, ax = plt.subplots(1,2, figsize=(20,4))
data["MERRA2_CNN_Surface_PM25"].sel(time='2020-09-08T00:30:00.000000000').plot(ax=ax[0])
data_spatial["MERRA2_CNN_Surface_PM25"].sel(time='2020-09-08T00:30:00.000000000').plot(ax=ax[1])
plt.show()
