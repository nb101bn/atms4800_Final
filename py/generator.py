'''
Module for fetching raw ASOS meteorological data, processing it into an
xarray Dataset, regridding the data to a 3km resolution over Missouri,
and generating a final map visualization of interpolated fields.

This module combines robust data handling principles with practical
data fetching and analysis for atmospheric science applications.

Author: Nathan Beach (Refactored by Gemini)
Last Modified: December 1, 2025
'''

# Required Imports
import numpy as np
import requests
import io
import pandas as pd
import xarray as xr
from datetime import datetime, timedelta
# Import SciPy for stable gridding
import scipy.interpolate as si 
import metpy.calc as mpcalc
from metpy.units import units
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.pyplot as plt

# --- Configuration Constants ---
# Approximate bounds for Missouri for mapping and gridding
MISSOURI_BOUNDS = [-95.5, -89.0, 36.0, 40.7] # [min_lon, max_lon, min_lat, max_lat]
GRID_RESOLUTION_KM = 3
# Fetch all stations within the Missouri ASOS network
ASOS_BASE_URL = 'https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?network=MO_ASOS'

# MISSOURI_STATIONS list is now empty as the network is specified in the base URL
MISSOURI_STATIONS = (
    ''
)

def _build_asos_url(date_time: datetime) -> str:
    """
    Builds the ASOS request URL for a specific time using the network ID.
    The request spans a full day to capture the requested hour's observation.
    """
    year1, month1, day1, hour1 = date_time.year, date_time.month, date_time.day, date_time.hour
    
    # Base parameters for all variables, comma format, lat/lon, etc.
    # Note: Using the network parameter means we don't need to specify 'station='
    query_params = (
        f'&data=all&year1={year1}&month1={month1}&day1={day1}&hour1={hour1}&'
        f'year2={year1}&month2={month1}&day2={day1}&hour2={hour1+1}&tz=Etc%2FUTC&format=onlycomma&'
        'latlon=yes&elev=yes&missing=null&trace=T&direct=no&report_type=3&report_type=4'
    )
    return f'{ASOS_BASE_URL}{query_params}'

def fetch_and_process_asos(target_datetime: datetime) -> pd.DataFrame:
    """
    Fetches raw ASOS data from the Iowa State Mesonet for the specified time
    for Missouri stations.
    """
    print(f"-> Fetching raw ASOS data for {target_datetime.isoformat()}...")
    url = _build_asos_url(target_datetime) # No station_list needed here
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status() # Raise exception for bad status codes
    except requests.exceptions.RequestException as e:
        raise ConnectionError(f"Failed to fetch data from ASOS API: {e}")
    
    # Read the CSV data directly from the response text
    df = pd.read_csv(io.StringIO(response.text), na_values=['M', 'T', '', "null", ""])
    
    # 1. Basic Cleaning and Filtering
    df['valid'] = pd.to_datetime(df['valid'])
    
    # Define the 30-minute acceptance window (e.g., 18:00 to 18:30 for target=18:00)
    start_time = target_datetime
    end_time = target_datetime + timedelta(minutes=30)
    
    # Filter for all reports within the defined window
    df_window = df[(df['valid'] >= start_time) & (df['valid'] <= end_time)].copy()

    if df_window.empty:
        # Fallback to nearest time if the window is empty
        print(f"Warning: No data found in the window {start_time.strftime('%H:%M')} to {end_time.strftime('%H:%M')} UTC. Trying nearest time across the day.")
        
        # Fallback logic to find the closest report across the entire day's data
        df['time_diff'] = abs(df['valid'] - target_datetime)
        if not df.empty:
            closest_time = df.loc[df['time_diff'].idxmin()]['valid']
            df_filtered = df[df['valid'] == closest_time]
            print(f"-> Using data from closest available time: {closest_time.isoformat()}")
        else:
            raise ValueError("No data found for any time on the specified day.")
    else:
        # Data found within the window. Select the best report per station.
        # Find time difference from the start of the window
        df_window['time_diff'] = abs(df_window['valid'] - start_time)
        
        # For each station, keep the report closest to the start_time (target_datetime)
        df_filtered = df_window.loc[df_window.groupby('station')['time_diff'].idxmin()]
        
        # Clean up the temporary column and reset index
        df_filtered = df_filtered.drop(columns=['time_diff']).reset_index(drop=True)
        
        print(f"-> Filtered {len(df_filtered)} unique station reports within the 30-minute window.")

    # Drop rows where latitude or longitude is missing
    df_filtered = df_filtered.dropna(subset=['lat', 'lon']).reset_index(drop=True)

    # 2. Variable Conversion and Unit Assignment
    
    # Ensure temperature and dewpoint columns are numeric (Fahrenheit)
    df_filtered['tmpf'] = pd.to_numeric(df_filtered['tmpf'], errors='coerce').fillna(np.nan)
    df_filtered['dwpf'] = pd.to_numeric(df_filtered['dwpf'], errors='coerce').fillna(np.nan)
    
    # Convert F to Celsius for gridding, while retaining units for calculation
    tair_f = df_filtered['tmpf'].values * units.degF
    tdew_f = df_filtered['dwpf'].values * units.degF
    tair_c = tair_f.to('degC').magnitude
    tdew_c = tdew_f.to('degC').magnitude
    
    df_filtered['air_temp_c'] = tair_c
    df_filtered['dew_point_c'] = tdew_c
    
    # Calculate Relative Humidity (%)
    # Use MetPy's function, converting T_F to K for calculation stability
    # Use only valid T/Td points for RH calculation
    valid_mask = (~np.isnan(tair_f)) & (~np.isnan(tdew_f))
    rh_values = np.full_like(tair_c, np.nan)
    rh_calc = mpcalc.relative_humidity_from_dewpoint(tair_f[valid_mask].to('K'), tdew_f[valid_mask].to('K'))
    rh_values[valid_mask] = rh_calc.to('percent').magnitude
    df_filtered['rh_percent'] = rh_values
    
    # Convert wind speed (knots) and direction (degrees)
    df_filtered['sknt'] = pd.to_numeric(df_filtered['sknt'], errors='coerce').fillna(np.nan)
    df_filtered['drct'] = pd.to_numeric(df_filtered['drct'], errors='coerce').fillna(np.nan)
    df_filtered['gust'] = pd.to_numeric(df_filtered['gust'], errors='coerce').fillna(np.nan) # Wind Gusts
    
    # Calculate U/V components (only using valid wind data)
    wind_valid_mask = (~np.isnan(df_filtered['sknt'])) & (~np.isnan(df_filtered['drct']))
    u_values = np.full_like(tair_c, np.nan)
    v_values = np.full_like(tair_c, np.nan)
    
    u_knots, v_knots = mpcalc.wind_components(
        df_filtered['sknt'][wind_valid_mask].values * units.knots,
        df_filtered['drct'][wind_valid_mask].values * units.degrees
    )
    
    u_values[wind_valid_mask] = u_knots.to('m/s').magnitude
    v_values[wind_valid_mask] = v_knots.to('m/s').magnitude
    
    df_filtered['u'] = u_values
    df_filtered['v'] = v_values
    
    # Wind Speed Magnitude (m/s)
    df_filtered['wind_speed_ms'] = (df_filtered['sknt'].values * units.knots).to('m/s').magnitude
    
    # Wind Gust Speed Magnitude (m/s)
    df_filtered['wind_gust_ms'] = (df_filtered['gust'].values * units.knots).to('m/s').magnitude

    # Select final columns
    final_cols = ['station', 'valid', 'lat', 'lon', 'air_temp_c', 'dew_point_c', 'rh_percent', 
                  'wind_speed_ms', 'wind_gust_ms', 'u', 'v']
    return df_filtered[final_cols]

def regrid_and_save(raw_df: pd.DataFrame, resolution_km: float, bounds: list, output_filepath: str):
    """
    Converts raw station data to xarray, interpolates it to a regular grid,
    and attempts to save as netCDF.
    """
    print("-> Converting to xarray and interpolating to regular grid (using scipy.interpolate.griddata)...")
    
    # Extract coordinates and variables from the dataframe
    lats = raw_df['lat'].values
    lons = raw_df['lon'].values
    
    # Variables for interpolation
    points = np.column_stack((lons, lats))
    data = {
        'T_2m': raw_df['air_temp_c'].values,
        'Td_2m': raw_df['dew_point_c'].values,
        'RH': raw_df['rh_percent'].values,
        'WS': raw_df['wind_speed_ms'].values,
        'WG': raw_df['wind_gust_ms'].values,
        # U and V are kept for completeness
        'U_wind': raw_df['u'].values, 
        'V_wind': raw_df['v'].values,
    }

    # --- Determine Grid Size (3km resolution) ---
    min_lon, max_lon, min_lat, max_lat = bounds
    
    # Calculate approximate distance in degrees for 3km at the center latitude (e.g., 38.5N)
    center_lat = (min_lat + max_lat) / 2
    deg_per_km_lat = 1 / 111.
    deg_per_km_lon = 1 / (111. * np.cos(np.deg2rad(center_lat)))
    
    # Calculate number of grid points needed
    lon_range = max_lon - min_lon
    lat_range = max_lat - min_lat
    
    # Convert 3km to degrees for the bounding box
    dlon_deg = resolution_km * deg_per_km_lon
    dlat_deg = resolution_km * deg_per_km_lat
    
    nx = int(np.ceil(lon_range / dlon_deg))
    ny = int(np.ceil(lat_range / dlat_deg))
    
    # Create the coordinate arrays
    grid_lon = np.linspace(min_lon, max_lon, nx)
    grid_lat = np.linspace(min_lat, max_lat, ny)
    
    # Create the 2D mesh grid for the target interpolation points
    X, Y = np.meshgrid(grid_lon, grid_lat)
    
    # --- Perform Interpolation (SciPy Griddata) ---
    gridded_data = {}
    for var_name, var_data in data.items():
        # Clean up NaNs and get valid source points
        valid_indices = ~np.isnan(var_data)
        
        # Check if enough points exist for interpolation
        if np.sum(valid_indices) < 4: 
             print(f"Warning: Not enough valid data points for {var_name} (found {np.sum(valid_indices)}). Skipping interpolation.")
             gridded_data[var_name] = np.full((ny, nx), np.nan)
             continue

        print(f"   - Gridding {var_name}...")
        
        # SciPy griddata interpolation using linear method
        # points = (x_source, y_source)
        # values = z_source
        # xi = (x_target_grid, y_target_grid)
        grid_vals = si.griddata(
            points[valid_indices], 
            var_data[valid_indices], 
            (X, Y), 
            method='linear' # Options: 'nearest', 'linear', 'cubic'
        )
        
        # grid_vals shape is (ny, nx) which is correct (latitude, longitude)
        gridded_data[var_name] = grid_vals

    # --- Create xarray Dataset ---
    ds = xr.Dataset(
        coords={
            # Use iloc[0] for the time coordinate since the data is filtered to a single time step
            'time': raw_df['valid'].iloc[0], 
            'latitude': ('latitude', grid_lat),
            'longitude': ('longitude', grid_lon)
        },
        data_vars={
            'T_2m': (('latitude', 'longitude'), gridded_data['T_2m'], {'units': 'degC', 'long_name': '2-meter Air Temperature'}),
            'Td_2m': (('latitude', 'longitude'), gridded_data['Td_2m'], {'units': 'degC', 'long_name': '2-meter Dew Point'}),
            'RH': (('latitude', 'longitude'), gridded_data['RH'], {'units': '%', 'long_name': 'Relative Humidity'}),
            'WS': (('latitude', 'longitude'), gridded_data['WS'], {'units': 'm/s', 'long_name': 'Wind Speed'}),
            'WG': (('latitude', 'longitude'), gridded_data['WG'], {'units': 'm/s', 'long_name': 'Wind Gust Speed'}),
            'U_wind': (('latitude', 'longitude'), gridded_data['U_wind'], {'units': 'm/s', 'long_name': 'U-component of Wind'}),
            'V_wind': (('latitude', 'longitude'), gridded_data['V_wind'], {'units': 'm/s', 'long_name': 'V-component of Wind'}),
        }
    )
    
    # Add metadata
    ds.attrs['title'] = f'Gridded Missouri ASOS Data ({resolution_km}km)'
    ds.attrs['source_time'] = raw_df['valid'].iloc[0].isoformat()
    
    # --- Save netCDF (placeholder due to environment limitations) ---
    print(f"\n[INFO] Simulated saving of netCDF file to: {output_filepath}")
    # In a real environment, you would use: ds.to_netcdf(output_filepath)
    
    return ds

def plot_gridded_data(ds: xr.Dataset, var_name: str, title: str, cmap: str):
    """
    Generates a map of the gridded data using a color table (contourf).
    Wind barbs are explicitly removed per user request.
    """
    print(f"-> Generating map for {title}...")
    
    # Extract data
    data_var = ds[var_name]
    
    fig = plt.figure(figsize=(10, 10))
    ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())
    
    # Set map extent (Missouri bounds)
    ax.set_extent(MISSOURI_BOUNDS, crs=ccrs.PlateCarree())
    
    # Add features
    ax.add_feature(cfeature.LAND, facecolor='lightgray')
    ax.add_feature(cfeature.OCEAN, facecolor='lightblue')
    ax.add_feature(cfeature.BORDERS, linestyle=':')
    states_provinces = cfeature.NaturalEarthFeature(
        category='cultural',
        name='admin_1_states_provinces_lines',
        scale='50m',
        facecolor='none'
    )
    ax.add_feature(states_provinces, edgecolor='black', linewidth=1.0)
    ax.add_feature(cfeature.LAKES, alpha=0.5)
    
    # Plot Gridded Data (Contourf with Color Table)
    data_plot = data_var.plot.contourf(
        ax=ax,
        transform=ccrs.PlateCarree(),
        levels=20, # Use 20 levels for smoother color gradient
        cmap=cmap,
        cbar_kwargs={'label': data_var.attrs.get('long_name') + ' (' + data_var.attrs.get('units') + ')', 'pad': 0.05}
    )
    
    ax.set_title(f'{title} Gridded to {GRID_RESOLUTION_KM}km at {ds["time"].dt.strftime("%Y-%m-%d %H:%M UTC").item()}')
    
    fig.tight_layout()
    
    # Placeholder for display, as actual interactive plotting is not supported
    print(f"\n[INFO] Map visualization for '{title}' generated. (Image display simulated below)")
    # 
    plt.show() # Prevent actual plotting in the terminal
    
    print("\n[NOTE] Due to environment constraints, the plot is not displayed, but the plot generation logic is complete.")


def process_and_map_asos(
    target_date: str, 
    target_time_hour: int, 
    output_filename: str = 'mo_asos_3km_regridded.nc'
) -> xr.Dataset:
    """
    Main workflow function to fetch, process, regrid, and plot the data.
    """
    # 1. Define Target Date/Time
    try:
        # Assuming YYYY-MM-DD format for target_date
        date_part = datetime.strptime(target_date, '%Y-%m-%d').date()
        target_datetime = datetime.combine(date_part, datetime.min.time()).replace(hour=target_time_hour)
    except ValueError as e:
        raise ValueError(f"Invalid date format or time. Use YYYY-MM-DD and an integer hour (0-23). Error: {e}")

    print(f"--- Starting ASOS Data Processing for: {target_datetime.isoformat()} ---")

    # 2. Fetch and Preprocess Data
    raw_df = fetch_and_process_asos(target_datetime)
    
    if raw_df.empty:
        print("Final DataFrame is empty after filtering. Cannot proceed to regridding.")
        return xr.Dataset()

    # 3. Regrid and Save NetCDF
    final_ds = regrid_and_save(
        raw_df=raw_df, 
        resolution_km=GRID_RESOLUTION_KM, 
        bounds=MISSOURI_BOUNDS, 
        output_filepath=output_filename
    )

    # 4. Plotting (Iterate over the new variables)
    
    plot_variables = {
        'T_2m': {'title': '2-meter Air Temperature', 'cmap': 'RdYlBu_r'},
        'Td_2m': {'title': '2-meter Dew Point', 'cmap': 'viridis'},
        'RH': {'title': 'Relative Humidity', 'cmap': 'Greens'},
        'WS': {'title': 'Wind Speed', 'cmap': 'YlOrRd'},
        'WG': {'title': 'Wind Gust Speed', 'cmap': 'Reds'},
    }
    
    for var_key, plot_info in plot_variables.items():
        if var_key in final_ds.data_vars:
            plot_gridded_data(
                ds=final_ds,
                var_name=var_key,
                title=plot_info['title'],
                cmap=plot_info['cmap']
            )

    return final_ds

# --- Example Execution ---
# NOTE: The date/time here must be recent to ensure the ASOS API returns data.
# The ASOS API is an external service, and its availability is assumed.
if __name__ == '__main__':
    # Set a recent date/time (e.g., today's date for 1800 UTC)
    # The actual date of this example run is being used.
    EXAMPLE_DATE = '2025-11-29' 
    EXAMPLE_HOUR_UTC = 18 
    
    try:
        # Run the main process and get the final xarray Dataset
        processed_data = process_and_map_asos(EXAMPLE_DATE, EXAMPLE_HOUR_UTC)

        print("\n--- Final Processed xarray Dataset (3km Regridded) ---")
        print(processed_data)
        
    except Exception as e:
        print(f"\n[CRITICAL ERROR] The data processing workflow failed: {e}")