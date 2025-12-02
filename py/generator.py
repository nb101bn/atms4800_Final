'''
Module for fetching, processing, and merging meteorological data from 
Missouri ASOS and Mesonet stations, regridding it to a high-resolution 
grid, and generating map visualizations.

Author: Nathan Beach (Refactored by Gemini)
Last Modified: December 2, 2025
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
import re
from typing import Dict, Any, List

# --- Configuration Constants ---
# Approximate bounds for Missouri for mapping and gridding
MISSOURI_BOUNDS = [-95.5, -89.0, 36.0, 40.7] # [min_lon, max_lon, min_lat, max_lat]
GRID_RESOLUTION_KM = 3
# ASOS URL (network specified)
ASOS_BASE_URL = 'https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?network=MO_ASOS'
# Mesonet Base URL
MESONET_BASE_URL = 'http://agebb.missouri.edu/weather/stations/'

# --- Mesonet Station Metadata ---
# This list is used to iterate over all Mesonet stations
MESONET_URL_LIST = [
    'http://agebb.missouri.edu/weather/stations/boone/bull75s.htm',
    'http://agebb.missouri.edu/weather/stations/audrain/bull65s.htm',
    'http://agebb.missouri.edu/weather/stations/audrain/bull20s.htm',
    'http://agebb.missouri.edu/weather/stations/atchison/bull5s.htm',
    'http://agebb.missouri.edu/weather/stations/boone/bull70s.htm',
    'http://agebb.missouri.edu/weather/stations/boone/bull75s.htm',
    'http://agebb.missouri.edu/weather/stations/sanborn/bull35s.htm',
    'http://agebb.missouri.edu/weather/stations/boone/bull30s.htm',
    'http://agebb.missouri.edu/weather/stations/buchanan/bull10s.htm',
    "http://agebb.missouri.edu/weather/stations/callaway/bull55s.htm",
    'http://agebb.missouri.edu/weather/stations/carroll/bull40s.htm',
    'http://agebb.missouri.edu/weather/stations/gentry/bull15s.htm',
    'http://agebb.missouri.edu/weather/stations/knox/bull25s.htm',
    "http://agebb.missouri.edu/weather/stations/lafayette/bull110s.htm",
    'http://agebb.missouri.edu/weather/stations/lawrence/bull85t.htm',
    'http://agebb.missouri.edu/weather/stations/lincoln/bull90s.htm',
    'http://agebb.missouri.edu/weather/stations/linn/bull45s.htm',
    'http://agebb.missouri.edu/weather/stations/morgan/bull60t.htm',
    'http://agebb.missouri.edu/weather/stations/monroe/bull50s.htm',
    'http://agebb.missouri.edu/weather/stations/putnam/bull95s.htm',
    'http://agebb.missouri.edu/weather/stations/saline/bull100s.htm',
    'http://agebb.missouri.edu/weather/stations/stlouiscity/bull105s.htm',
    'http://agebb.missouri.edu/weather/stations/wright/bull80t.htm'
]

# Manual Metadata for this specific station URL
STATION_METADATA: Dict[str, Dict[str, Any]] = {
    "bull5": {"station_id": "GravesMemorial_Atchison", "lat": 39.75, "lon": -94.79, "year": 2025},
    "bull10": {"station_id": "StJoe_Buchanan", "lat": 37.49, "lon": -94.31, "year": 2025},
    "bull15": {"station_id": "Albany_Gentry", "lat": 40.24, "lon": -94.34, "year": 2025},
    "bull20": {"station_id": "Auxvasse_Audrain", "lat": 39.30, "lon": -91.51, "year": 2025},
    "bull25": {"station_id": "Greenley_Knox", "lat": 40.01, "lon": -92.19, "year": 2025},
    "bull30": {"station_id": "SouthFarm_boone", "lat": 38.92, "lon": -92.33, "year": 2025},
    "bull35": {"station_id": "Sanborn_Boone", "lat": 38.93, "lon": -92.32, "year": 2025},
    "bull40": {"station_id": "Brunswick_carroll", "lat": 39.41, "lon": -93.19, "year": 2025},
    "bull45": {"station_id": "Linneus_Linn", "lat": 39.63, "lon": -91.72, "year": 2025},
    "bull50": {"station_id": "MonroeCity_Monroe", "lat": 39.33, "lon": -92.02, "year": 2025},
    "bull55": {"station_id": "Williamsburg_Callaway", "lat": 38.90, "lon": -91.73, "year": 2025},
    "bull60": {"station_id": "Versailles_Morgan", "lat": 38.43, "lon": -92.85, "year": 2025},
    "bull65": {"station_id": "Van-Far_Audrian", "lat": 39.30, "lon": -91.51, "year": 2025},
    "bull70": {'station_id': 'Bradford_Boone', 'lat': 38.93, 'lon': -92.32, 'year': 2025},
    "bull75": {'station_id': 'CapenPark_Boone', 'lat': 38.93, 'lon': -92.32, 'year': 2025},
    "bull80": {"station_id": "MountainGrove_Wright", "lat": 37.15, "lon": -92.26, "year": 2025}, 
    "bull85": {"station_id": "MountVernon_Lawrence", "lat": 37.07, "lon": -93.87, "year": 2025},
    "bull90": {"station_id": "Moscow_Lincoln", "lat": 38.93, "lon": -90.93, "year": 2025},
    "bull95": {"station_id": "Unionville_Putnam", "lat": 40.46, "lon": -93.00, "year": 2025},
    "bull100": {"station_id": "Marshall_Saline", "lat": 39.12, "lon": -92.21, "year": 2025},
    "bull105": {"station_id": "StScienceCenter_StLouisCity", "lat": 38.63, "lon": -90.27, "year": 2025},
    "bull110": {"station_id": "Alma_Lafayette", "lat": 39.09, "lon": -93.55, "year": 2025},
}
# Regex to extract the unique bull# identifier from the URL path
BULL_ID_PATTERN = re.compile(r'/(bull\d+)[st]\.htm', re.IGNORECASE)


# --- ASOS Fetching and Processing ---

def _build_asos_url(date_time: datetime) -> str:
    """Builds the ASOS request URL."""
    year, month, day, hour = date_time.year, date_time.month, date_time.day, date_time.hour
    query_params = (
        f'&data=all&year1={year}&month1={month}&day1={day}&hour1={hour}&'
        f'year2={year}&month2={month}&day2={day}&hour2={hour+1}&tz=Etc%2FUTC&format=onlycomma&'
        'latlon=yes&elev=yes&missing=null&trace=T&direct=no&report_type=3&report_type=4'
    )
    return f'{ASOS_BASE_URL}{query_params}'

def fetch_and_process_asos(target_datetime: datetime) -> pd.DataFrame:
    """Fetches raw ASOS data, standardizes units, and filters reports."""
    print(f"\n[ASOS] Fetching raw ASOS data for {target_datetime.isoformat()}...")
    url = _build_asos_url(target_datetime)
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Failed to fetch ASOS data: {e}")
        return pd.DataFrame()
    
    df = pd.read_csv(io.StringIO(response.text), na_values=['M', 'T', '', "null", ""])
    df['valid'] = pd.to_datetime(df['valid'])
    
    # Filtering Logic (30-minute window for ASOS)
    start_time = target_datetime
    end_time = target_datetime + timedelta(minutes=30)
    df_window = df[(df['valid'] >= start_time) & (df['valid'] <= end_time)].copy()

    if df_window.empty:
        # Fallback to nearest time
        print("[ASOS] Warning: No data in window. Trying nearest time across the day.")
        df['time_diff'] = abs(df['valid'] - target_datetime)
        df_filtered = df[df['time_diff'] == df['time_diff'].min()] if not df.empty else pd.DataFrame()
    else:
        # Select report closest to start_time for each station
        df_window['time_diff'] = abs(df_window['valid'] - start_time)
        df_filtered = df_window.loc[df_window.groupby('station')['time_diff'].idxmin()].drop(columns=['time_diff']).reset_index(drop=True)
    
    df_filtered = df_filtered.dropna(subset=['lat', 'lon']).reset_index(drop=True)

    # Unit Conversion and Calculation (ASOS)
    tair_f = df_filtered['tmpf'].values * units.degF
    tdew_f = df_filtered['dwpf'].values * units.degF
    df_filtered['air_temp_c'] = tair_f.to('degC').magnitude
    df_filtered['dew_point_c'] = tdew_f.to('degC').magnitude
    
    # RH Calculation
    valid_mask = (~np.isnan(tair_f.magnitude)) & (~np.isnan(tdew_f.magnitude))
    rh_values = np.full_like(df_filtered['air_temp_c'].values, np.nan)
    rh_calc = mpcalc.relative_humidity_from_dewpoint(tair_f[valid_mask].to('K'), tdew_f[valid_mask].to('K'))
    rh_values[valid_mask] = rh_calc.to('percent').magnitude
    df_filtered['rh_percent'] = rh_values
    
    # Wind Calculation
    df_filtered['sknt'] = pd.to_numeric(df_filtered['sknt'], errors='coerce').fillna(np.nan)
    df_filtered['drct'] = pd.to_numeric(df_filtered['drct'], errors='coerce').fillna(np.nan)
    df_filtered['gust'] = pd.to_numeric(df_filtered['gust'], errors='coerce').fillna(np.nan) 
    
    wind_valid_mask = (~np.isnan(df_filtered['sknt'])) & (~np.isnan(df_filtered['drct']))
    u_values = np.full_like(df_filtered['air_temp_c'].values, np.nan)
    v_values = np.full_like(df_filtered['air_temp_c'].values, np.nan)
    
    u_knots, v_knots = mpcalc.wind_components(
        df_filtered['sknt'][wind_valid_mask].values * units.knots,
        df_filtered['drct'][wind_valid_mask].values * units.degrees
    )
    u_values[wind_valid_mask] = u_knots.to('m/s').magnitude
    v_values[wind_valid_mask] = v_knots.to('m/s').magnitude
    
    df_filtered['u'] = u_values
    df_filtered['v'] = v_values
    df_filtered['wind_speed_ms'] = (df_filtered['sknt'].values * units.knots).to('m/s').magnitude
    df_filtered['wind_gust_ms'] = (df_filtered['gust'].values * units.knots).to('m/s').magnitude

    # Final standardized columns for ASOS
    final_cols = ['station', 'valid', 'lat', 'lon', 'air_temp_c', 'dew_point_c', 'rh_percent', 
                  'wind_speed_ms', 'wind_gust_ms', 'u', 'v']
    print(f"[ASOS] Processed {len(df_filtered)} unique reports.")
    return df_filtered[final_cols]

# --- Mesonet Fetching and Processing ---

def fetch_and_extract_pre_tag(url: str) -> str:
    """Fetches HTML and extracts the raw text block found inside the <pre> tag."""
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Failed to fetch URL {url}: {e}")
        return ""

    pre_match = re.search(r'<pre>(.*?)</pre>', response.text, re.DOTALL | re.IGNORECASE)
    return pre_match.group(1).strip() if pre_match else ""

def parse_mesonet_data(raw_text: str, metadata: dict) -> pd.DataFrame:
    """
    Parses raw Mesonet text data into a DataFrame and performs standardization.
    Now uses dynamic column assignment to handle varying soil depth columns.
    """
    print(f"       -[MESONET] Parsing data for station: {metadata['station_id']}...")
    
    # 1. Dynamic Skip Line Calculation
    lines = raw_text.splitlines()
    data_start_line = -1
    # Find the first line starting with numerical date/time data
    data_line_pattern = re.compile(r'^\s*\d{1,2}\s+\d{1,2}\s+\d{3,4}')

    for i, line in enumerate(lines):
        if data_line_pattern.match(line):
            data_start_line = i
            break

    if data_start_line == -1:
        print(f"[ERROR] Could not find the start of the hourly data block for {metadata['station_id']}. Skipping.")
        return pd.DataFrame()
    
    SKIP_LINES_DYNAMIC = data_start_line
    
    # 2. Extract Year
    year_match = re.search(r'Year\s*=\s*(\d{4})', raw_text)
    data_year = int(year_match.group(1)) if year_match else metadata.get('year', datetime.now().year)
        
    # Standard output columns list (14 columns total)
    mesonet_cols_full = [
        'Month', 'Day', 'Time_HHMM', 'Air_Temp_F', 'Rel_Hum_pct', 
        'Soil_Temp_2in_F', 'Soil_Temp_4in_F', 'Soil_Temp_8in_F', 
        'Soil_Temp_20in_F', 'Soil_Temp_40in_F', 
        'Wind_Speed_MPH', 'Wind_Dir_Deg', 'Solar_Rad_Wm2', 'Precip_Inches'
    ]
    
    try:
        # Read the entire block of data starting from the dynamic line.
        # Use header=None initially to get raw columns.
        df_raw = pd.read_csv(
            io.StringIO(raw_text), sep='\s+', 
            skiprows=SKIP_LINES_DYNAMIC, header=None,
            engine='python', skipinitialspace=True, 
            skipfooter=3
        )
    except Exception as e:
        print(f"[ERROR] Mesonet read_csv failed for {metadata['station_id']}: {e}")
        return pd.DataFrame()
        
    # --- Dynamic Column Mapping (CRITICAL FIX) ---
    df_raw = df_raw.dropna(how='all', axis=1)
    
    # Standard columns have consistent positions:
    # 0: Month, 1: Day, 2: Time, 3: Air Temp, 4: RH, ..., Last-2: Wind Speed, Last-1: Wind Dir, Last: Precip.
    # The variable columns are the soil temperatures in the middle.
    
    num_cols = df_raw.shape[1]
    
    if num_cols < 9: # We need at least Month/Day/Time/T/RH/WS/WD/Solar/Precip (9 core columns)
        print(f"[ERROR] Station {metadata['station_id']} has an unusual structure ({num_cols} cols). Skipping.")
        return pd.DataFrame()

    # Create the standardized final DataFrame
    df = pd.DataFrame(index=df_raw.index)

    # Assign known meteorological columns (these are almost always standard)
    df['Month'] = pd.to_numeric(df_raw[0], errors='coerce')
    df['Day'] = pd.to_numeric(df_raw[1], errors='coerce')
    df['Time_HHMM'] = pd.to_numeric(df_raw[2], errors='coerce')
    df['Air_Temp_F'] = pd.to_numeric(df_raw[3], errors='coerce')
    df['Rel_Hum_pct'] = pd.to_numeric(df_raw[4], errors='coerce')
    
    # Wind, Solar, Precip columns are anchored from the right/end
    df['Precip_Inches'] = pd.to_numeric(df_raw[num_cols - 1], errors='coerce')
    df['Solar_Rad_Wm2'] = pd.to_numeric(df_raw[num_cols - 2], errors='coerce')
    df['Wind_Dir_Deg'] = pd.to_numeric(df_raw[num_cols - 3], errors='coerce')
    df['Wind_Speed_MPH'] = pd.to_numeric(df_raw[num_cols - 4], errors='coerce')
    
    # Soil columns are explicitly set to NaN unless manually parsed later.
    # Since we only care about surface met, we set these to NaN for standardization.
    soil_cols_to_nan = ['Soil_Temp_2in_F', 'Soil_Temp_4in_F', 'Soil_Temp_8in_F', 
                        'Soil_Temp_20in_F', 'Soil_Temp_40in_F']
    for col in soil_cols_to_nan:
        df[col] = np.nan
        
    # --- Start Cleaning ---
    df = df.dropna(how='all', subset=['Time_HHMM', 'Month', 'Day']).reset_index(drop=True)
    
    # CLEANING FIX: Ensure Time_HHMM is a clean integer before date construction
    df['Time_HHMM'] = df['Time_HHMM'].astype('Int64', errors='ignore')
    df = df.dropna(subset=['Time_HHMM', 'Month', 'Day'])
    
    # 3. Create Datetime Column (Naive LST)
    def combine_date_time(row, year):
        try:
            month = str(int(row['Month'])).zfill(2)
            day = str(int(row['Day'])).zfill(2)
            time_int = row['Time_HHMM']
            
            if time_int == 2400:
                dt_str = f"{year}-{month}-{day} 23:59"
                dt = pd.to_datetime(dt_str, format='%Y-%m-%d %H:%M')
                return dt + pd.Timedelta(minutes=1)
            else:
                time_str = str(int(time_int)).zfill(4) 
                hour = time_str[:-2].zfill(2)
                minute = time_str[-2:]
                dt_str = f"{year}-{month}-{day} {hour}:{minute}"
                return pd.to_datetime(dt_str, format='%Y-%m-%d %H:%M', errors='coerce')
        except:
            return pd.NaT

    df['valid'] = df.apply(lambda row: combine_date_time(row, data_year), axis=1)
    df = df.dropna(subset=['valid'])
    
    # 4. Add Metadata
    df['station'] = metadata['station_id']
    df['lat'] = metadata['lat']
    df['lon'] = metadata['lon']
    
    # 5. Standardize Units and Variables (Matching ASOS output)
    tair_f = pd.to_numeric(df['Air_Temp_F'], errors='coerce').values * units.degF
    rel_hum = pd.to_numeric(df['Rel_Hum_pct'], errors='coerce').values * units.percent
    
    # Td Calculation
    tdew_k = mpcalc.dewpoint_from_relative_humidity(tair_f.to('K'), rel_hum)
    
    df['air_temp_c'] = tair_f.to('degC').magnitude
    df['dew_point_c'] = tdew_k.to('degC').magnitude
    df['rh_percent'] = rel_hum.magnitude
    
    # Wind Conversion
    wind_speed_mph = pd.to_numeric(df['Wind_Speed_MPH'], errors='coerce').fillna(np.nan)
    wind_dir_deg = pd.to_numeric(df['Wind_Dir_Deg'], errors='coerce').fillna(np.nan)

    wind_valid_mask = (~np.isnan(wind_speed_mph)) & (~np.isnan(wind_dir_deg))
    u_values = np.full_like(df['air_temp_c'].values, np.nan)
    v_values = np.full_like(df['air_temp_c'].values, np.nan)
    
    u_mph, v_mph = mpcalc.wind_components(
        wind_speed_mph[wind_valid_mask].values * units('mile/hour'),
        wind_dir_deg[wind_valid_mask].values * units.degrees
    )
    
    df['u'] = u_mph.to('m/s').magnitude
    df['v'] = v_mph.to('m/s').magnitude
    df['wind_speed_ms'] = (wind_speed_mph.values * units('mile/hour')).to('m/s').magnitude
    df['wind_gust_ms'] = np.nan 

    final_cols = ['station', 'valid', 'lat', 'lon', 'air_temp_c', 'dew_point_c', 'rh_percent', 
                  'wind_speed_ms', 'wind_gust_ms', 'u', 'v']
    
    print(f"       -[MESONET] Cleaned data for {metadata['station_id']} (Num Rows: {len(df)}):\n{df[final_cols].head()}")
    return df[final_cols]


def fetch_and_process_mesonet(target_datetime: datetime) -> pd.DataFrame:
    """Iterates through all Mesonet stations, fetches data, converts LST to UTC, and filters."""
    print(f"\n[MESONET] Starting fetch for Mesonet stations...")
    all_stations_data: List[pd.DataFrame] = []
    
    # Fixed offset for LST -> UTC conversion (Missouri is UTC-6 during standard time)
    LST_TO_UTC_OFFSET = timedelta(hours=6)
    
    print(f"[MESONET] Assuming Mesonet reports are in LST (UTC-6). All times will be converted to UTC.")
    
    # Time filter is now simplified to target the exact hour only
    target_time_utc = target_datetime
    
    for url in MESONET_URL_LIST:
        match = BULL_ID_PATTERN.search(url)
        if not match:
            continue
            
        metadata_key = match.group(1).lower()
        metadata = STATION_METADATA.get(metadata_key)
        
        if not metadata:
            continue
            
        # 1. Fetch and Parse raw Mesonet data (resulting in Naive LST times)
        raw_text_data = fetch_and_extract_pre_tag(url)
        
        if raw_text_data:
            mesonet_df = parse_mesonet_data(raw_text_data, metadata)
            
            if not mesonet_df.empty:
                # 2. Convert Naive LST column to UTC
                # Apply the fixed offset to the entire 'valid' column (LST + 6 hours = UTC)
                mesonet_df['valid_utc'] = mesonet_df['valid'] + LST_TO_UTC_OFFSET
                
                # 3. Filter using the standardized UTC time (exactly the target hour, 0-59 minutes)
                # We target the date and hour of the observation time.
                filtered_df = mesonet_df[
                    (mesonet_df['valid_utc'].dt.date == target_time_utc.date()) & 
                    (mesonet_df['valid_utc'].dt.hour == target_time_utc.hour)
                ].copy()
                
                if not filtered_df.empty:
                    print(f"[MESONET] Found {len(filtered_df)} reports in the target hour for {metadata['station_id']}. Selecting best report...")
                    # Select the report closest to the target UTC time (which is the start of the hour)
                    filtered_df['time_diff'] = abs(filtered_df['valid_utc'] - target_time_utc)
                    best_report = filtered_df.loc[filtered_df['time_diff'].idxmin()].copy()
                    
                    # Set the final valid time to the precise UTC time
                    best_report['valid'] = best_report['valid_utc']
                    
                    # Append cleaned report to list
                    all_stations_data.append(best_report.drop(columns=['valid_utc', 'time_diff']).to_frame().T)

    if all_stations_data:
        final_df = pd.concat(all_stations_data, ignore_index=True)
        print(f"[MESONET] Processed and merged {len(final_df)} unique Mesonet reports.")
        return final_df
    else:
        print("[MESONET] No Mesonet data successfully processed.")
        return pd.DataFrame()


# --- Gridding, NetCDF, and Plotting Functions ---

def regrid_and_save(raw_df: pd.DataFrame, resolution_km: float, bounds: list, output_filepath: str):
    """
    Converts raw station data to xarray, interpolates it to a regular grid,
    and attempts to save as netCDF.
    """
    print("\n-> Converting to xarray and interpolating to regular grid (scipy.interpolate.griddata)...")
    
    # Ensure all data columns are numeric before extraction
    numeric_cols = ['lat', 'lon', 'air_temp_c', 'dew_point_c', 'rh_percent', 'wind_speed_ms', 'wind_gust_ms', 'u', 'v']
    for col in numeric_cols:
        raw_df[col] = pd.to_numeric(raw_df[col], errors='coerce')
        
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
        'WG': raw_df['wind_gust_ms'].values.copy(), # Copy gust data (may contain NaNs from mesonet)
        'U_wind': raw_df['u'].values, 
        'V_wind': raw_df['v'].values,
    }

    # --- Determine Grid Size (3km resolution) ---
    min_lon, max_lon, min_lat, max_lat = bounds
    center_lat = (min_lat + max_lat) / 2
    deg_per_km_lat = 1 / 111.
    deg_per_km_lon = 1 / (111. * np.cos(np.deg2rad(center_lat)))
    
    lon_range = max_lon - min_lon
    lat_range = max_lat - min_lat
    dlon_deg = resolution_km * deg_per_km_lon
    dlat_deg = resolution_km * deg_per_km_lat
    
    nx = int(np.ceil(lon_range / dlon_deg))
    ny = int(np.ceil(lat_range / dlat_deg))
    
    grid_lon = np.linspace(min_lon, max_lon, nx)
    grid_lat = np.linspace(min_lat, max_lat, ny)
    
    X, Y = np.meshgrid(grid_lon, grid_lat)
    
    # --- Perform Interpolation (SciPy Griddata) ---
    gridded_data: Dict[str, np.ndarray] = {}
    for var_name, var_data in data.items():
        valid_indices = ~np.isnan(var_data)
        
        if np.sum(valid_indices) < 4: 
             print(f"Warning: Not enough valid data points for {var_name} (found {np.sum(valid_indices)}). Skipping interpolation.")
             gridded_data[var_name] = np.full((ny, nx), np.nan)
             continue

        print(f"   - Gridding {var_name}...")
        
        grid_vals = si.griddata(
            points[valid_indices], 
            var_data[valid_indices], 
            (X, Y), 
            method='linear'
        )
        gridded_data[var_name] = grid_vals

    # --- Create xarray Dataset ---
    ds = xr.Dataset(
        coords={
            'time': raw_df['valid'].iloc[0] if not raw_df.empty else datetime.now(), 
            'latitude': ('latitude', grid_lat),
            'longitude': ('longitude', grid_lon)
        },
        data_vars={
            'T_2m': (('latitude', 'longitude'), gridded_data.get('T_2m', np.zeros((ny, nx))), {'units': 'degC', 'long_name': '2-meter Air Temperature'}),
            'Td_2m': (('latitude', 'longitude'), gridded_data.get('Td_2m', np.zeros((ny, nx))), {'units': 'degC', 'long_name': '2-meter Dew Point'}),
            'RH': (('latitude', 'longitude'), gridded_data.get('RH', np.zeros((ny, nx))), {'units': '%', 'long_name': 'Relative Humidity'}),
            'WS': (('latitude', 'longitude'), gridded_data.get('WS', np.zeros((ny, nx))), {'units': 'm/s', 'long_name': 'Wind Speed'}),
            'WG': (('latitude', 'longitude'), gridded_data.get('WG', np.zeros((ny, nx))), {'units': 'm/s', 'long_name': 'Wind Gust Speed'}),
            'U_wind': (('latitude', 'longitude'), gridded_data.get('U_wind', np.zeros((ny, nx))), {'units': 'm/s', 'long_name': 'U-component of Wind'}),
            'V_wind': (('latitude', 'longitude'), gridded_data.get('V_wind', np.zeros((ny, nx))), {'units': 'm/s', 'long_name': 'V-component of Wind'}),
        }
    )
    
    ds.attrs['title'] = f'Gridded Missouri ASOS/Mesonet Data ({resolution_km}km)'
    ds.attrs['source_time'] = ds['time'].item()
    
    # --- Save netCDF (placeholder due to environment limitations) ---
    print(f"\n[OUTPUT] Simulated saving of netCDF file to: {output_filepath}")
    # In a real environment, you would use: ds.to_netcdf(output_filepath)
    
    return ds

def plot_gridded_data(ds: xr.Dataset, var_name: str, title: str, cmap: str):
    """Generates a map of the gridded data using a color table (contourf)."""
    print(f"\n-> Generating map for {title}...")
    
    data_var = ds[var_name]
    
    fig = plt.figure(figsize=(10, 10))
    ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())
    
    ax.set_extent(MISSOURI_BOUNDS, crs=ccrs.PlateCarree())
    
    ax.add_feature(cfeature.LAND, facecolor='lightgray')
    ax.add_feature(cfeature.OCEAN, facecolor='lightblue')
    ax.add_feature(cfeature.BORDERS, linestyle=':')
    states_provinces = cfeature.NaturalEarthFeature(
        category='cultural', name='admin_1_states_provinces_lines', scale='50m', facecolor='none'
    )
    ax.add_feature(states_provinces, edgecolor='black', linewidth=1.0)
    ax.add_feature(cfeature.LAKES, alpha=0.5)
    
    # Plot Gridded Data (Contourf with Color Table)
    data_plot = data_var.plot.contourf(
        ax=ax, transform=ccrs.PlateCarree(), levels=20, cmap=cmap,
        cbar_kwargs={'label': data_var.attrs.get('long_name') + ' (' + data_var.attrs.get('units') + ')', 'pad': 0.05}
    )
    
    ax.set_title(f'{title} Gridded to {GRID_RESOLUTION_KM}km at {ds["time"].dt.strftime("%Y-%m-%d %H:%M UTC").item()}')
    fig.tight_layout()
    
    # Placeholder for display and saving PNG (using plt.close to prevent blocking)
    print(f"[OUTPUT] Map visualization for '{title}' generated.")
    plt.show()


def process_and_map_data(
    target_date: str, 
    target_time_hour: int, 
    output_filename: str = 'mo_surface_3km_regridded.nc'
) -> pd.DataFrame:
    """
    Main workflow function to fetch, process, merge, regrid, and plot the data.
    Returns the final merged raw DataFrame for inspection.
    """
    # 1. Define Target Date/Time
    try:
        date_part = datetime.strptime(target_date, '%Y-%m-%d').date()
        target_datetime = datetime.combine(date_part, datetime.min.time()).replace(hour=target_time_hour)
    except ValueError as e:
        raise ValueError(f"Invalid date format or time. Use YYYY-MM-DD and an integer hour (0-23). Error: {e}")

    print(f"--- Starting Data Processing for: {target_datetime.isoformat()} UTC ---")

    # 2. Fetch and Preprocess Data from both sources
    raw_df_asos = fetch_and_process_asos(target_datetime)
    raw_df_mesonet = fetch_and_process_mesonet(target_datetime)
    
    # 3. Merge Datasets
    # Concatenate the two DataFrames. Since columns are standardized, they merge cleanly.
    all_raw_df = pd.concat([raw_df_asos, raw_df_mesonet], ignore_index=True)
    
    # *** FIX: Ensure the 'valid' column retains its datetime type after concatenation ***
    all_raw_df['valid'] = pd.to_datetime(all_raw_df['valid'])

    # Drop rows that are missing critical data (Lat/Lon/Temp) to prevent gridding failure
    all_raw_df = all_raw_df.dropna(subset=['lat', 'lon', 'air_temp_c']).reset_index(drop=True)

    print(f"\n--- Merging Complete ---")
    print(f"Total Unique Reports for Gridding: {len(all_raw_df)}")

    if all_raw_df.empty:
        print("Final DataFrame is empty. Cannot proceed to regridding.")
        return pd.DataFrame()

    # 4. Regrid and Save NetCDF
    final_ds = regrid_and_save(
        raw_df=all_raw_df, 
        resolution_km=GRID_RESOLUTION_KM, 
        bounds=MISSOURI_BOUNDS, 
        output_filepath=output_filename
    )

    # 5. Plotting
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
                ds=final_ds, var_name=var_key, title=plot_info['title'], cmap=plot_info['cmap']
            )

    return all_raw_df

# --- Example Execution ---
if __name__ == '__main__':
    # Set a date/time. Note: Mesonet URLs often contain current/recent data only.
    EXAMPLE_DATE = '2025-11-30' 
    EXAMPLE_HOUR_UTC = 18 # Assuming 18Z (12 PM CST/1PM CDT)
    
    try:
        # Run the main process, which returns the merged DataFrame for inspection
        merged_raw_data = process_and_map_data(EXAMPLE_DATE, EXAMPLE_HOUR_UTC)

        print("\n--- Final Merged Data Table (ASOS + Mesonet) ---")
        print(merged_raw_data[['station', 'valid', 'lat', 'lon', 'air_temp_c', 'rh_percent', 'wind_speed_ms']].head(10))
        
    except Exception as e:
        print(f"\n[CRITICAL ERROR] The data processing workflow failed: {e}")