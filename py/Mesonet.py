'''
Standalone module for testing the parsing of fixed-width, pre-formatted 
weather data typically found within <pre> tags on older Mesonet websites.

This script fetches a sample URL, extracts the data, parses it using a 
whitespace delimiter, and standardizes column names and types.

Author: Nathan Beach (Refactored by Gemini)
Last Modified: December 2, 2025
'''

import requests
import pandas as pd
import io
import re
from datetime import datetime

# --- Configuration Constants for Testing ---
MESONET_URL_LIST = ['http://agebb.missouri.edu/weather/stations/boone/bull75s.htm',
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

# METADATA: Map the unique file identifier to static location data
# NOTE: The keys are modified to match the full bull# identifier (e.g., 'bull75' instead of 'bull75s')
STATION_METADATA = {
    "bull5" : {
        "station_id": "GravesMemorial_Atchison",
        "lat": 39.75,
        "lon": -94.79,
        "year" : 2025,
    },
    "bull10" : {
        "station_id" : "StJoe_Buchanan",
        "lat" : 37.49,
        "lon" : -94.31,
        "year" : 2025,
    },
    "bull15" : {
        "station_id": "Albany_Gentry",
        "lat": 40.24,
        "lon": -94.34,
        "year": 2025,
    },
    "bull20" : {
        "station_id": "Auxvasse_Audrain",
        "lat": 39.30,
        "lon":-91.51,
        "year": 2025,
    },
    "bull25" : {
        "station_id": "Greenley_Knox",
        "lat": 40.01,
        "lon": -92.19,
        "year": 2025,
    },
    "bull30" : {
        "station_id": "SouthFarm_boone",
        "lat": 38.92,
        "lon": -92.33,
        "year": 2025
    },
    "bull35" : {
        "station_id": "Sanborn_Boone",
        "lat": 38.93,
        "lon": -92.32,
        "year": 2025
    },
    "bull40" : {
        "station_id" : "Brunswick_carroll",
        "lat": 39.41,
        "lon": -93.19,
        "year" : 2025,
    },
    "bull45" : {
        "station_id": "Linneus_Linn",
        "lat": 39.63,
        "lon": -91.72,
        "year": 2025,
    },
    "bull50" : {
        "station_id": "MonroeCity_Monroe",
        "lat": 39.33,
        "lon": -92.02,
        "year": 2025,
    },
    "bull55" : {
        "station_id": "Williamsburg_Callaway",
        "lat": 38.90,
        "lon": -91.73,
        "year": 2025,
    },
    "bull60" : {
        "station_id": "Versailles_Morgan",
        "lat": 38.43,
        "lon": -92.85,
        "year": 2025,
    },
    "bull65" : {
        "station_id": "Van-Far_Audrian",
        "lat": 39.30,
        "lon": -91.51,
        "year": 2025
    },
    "bull70" : {
        'station_id': 'Bradford_Boone',
        'lat': 38.93,
        'lon': -92.32,
        # Assuming the year from the <pre> tag is current for datetime construction
        'year': 2025
    },
    "bull75" : {
        'station_id': 'CapenPark_Boone',
        'lat': 38.93,
        'lon': -92.32,
        # Assuming the year from the <pre> tag is current for datetime construction
        'year': 2025
    },
    "bull80" : {
        "station_id" : "MountainGrove_Wright",
        "lat" : 37.15,
        "lon" : -92.26,
        "year" : 2025,
    }, 
    "bull85" : {
        "station_id" : "MountVernon_Lawrence",
        "lat" : 37.07,
        "lon" : -93.87,
        "year" : 2025,
    },
    "bull90" : {
        "station_id" : "Moscow_Lincoln",
        "lat" : 38.93,
        "lon" : -90.93,
        "year" : 2025,
    },
    "bull95" : {
        "station_id" : "Unionville_Putnam",
        "lat": 40.46,
        "lon": -93.00,
        "year": 2025,
    },
    "bull100" : {
        "station_id" : "Marshall_Saline",
        "lat": 39.12,
        "lon": -92.21,
        "year": 2025,
    },
    "bull105" : {
        "station_id" : "StScienceCenter_StLouisCity",
        "lat": 38.63,
        "lon": -90.27,
        "year": 2025,
    },
    "bull110" : {
        "station_id" : "Alma_Lafayette",
        "lat": 39.09,
        "lon": -93.55,
        "year": 2025,
    },

}

def fetch_and_extract_pre_tag(url: str) -> str:
    """
    Fetches the HTML content and extracts the raw text block found inside the <pre> tag.
    
    Args:
        url (str): The URL of the Mesonet station page.
        
    Returns:
        str: The raw text block from the <pre> tag, or an empty string on failure.
    """
    print(f"-> Attempting to fetch content from: {url}")
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status() # Raise exception for bad status codes
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Failed to fetch URL: {e}")
        return ""

    raw_html = response.text

    # Use simple string splitting to reliably extract content between <pre> tags
    # We look for <pre> and </pre> ignoring case
    pre_match = re.search(r'<pre>(.*?)</pre>', raw_html, re.DOTALL | re.IGNORECASE)
    
    if pre_match:
        raw_data_text = pre_match.group(1).strip()
        print("-> Successfully extracted data from <pre> tag.")
        return raw_data_text
    else:
        print("[ERROR] Could not find <pre> tag content in the HTML.")
        return ""

def parse_mesonet_data(raw_text: str, metadata: dict) -> pd.DataFrame:
    """
    Parses the space-separated, pre-formatted weather data into a DataFrame.
    
    Args:
        raw_text (str): The raw text extracted from the <pre> tag.
        metadata (dict): Dictionary containing 'station_id', 'lat', 'lon', and 'year'.
        
    Returns:
        pd.DataFrame: A clean DataFrame with parsed data and metadata.
    """
    print(f"-> Starting parse for station: {metadata['station_id']}")
    
    # 1. Extract Year from the raw text for robustness
    year_match = re.search(r'Year\s*=\s*(\d{4})', raw_text)
    if year_match:
        data_year = int(year_match.group(1))
    else:
        # Fallback if the Year line is missing (using the year provided in metadata as a last resort)
        print("[Warning] Could not extract year from raw text. Using metadata year.")
        data_year = metadata.get('year', datetime.now().year)
        
    # Define clean, flattened column names based on the structure provided
    mesonet_cols = [
        'Month', 'Day', 'Time_HHMM', 'Air_Temp_F', 'Rel_Hum_pct', 
        'Soil_Temp_2in_F', 'Soil_Temp_4in_F', 'Soil_Temp_8in_F', 
        'Soil_Temp_20in_F', 'Soil_Temp_40in_F', 
        'Wind_Speed_MPH', 'Wind_Dir_Deg', 'Solar_Rad_Wm2', 'Precip_Inches'
    ]
    
    # We determined the header/metadata is 12 lines long from the sample
    SKIP_LINES = 12 
    
    try:
        # Crucial step: Use sep='\s+' (one or more whitespace characters) to handle 
        # the variable spacing between columns effectively.
        df = pd.read_csv(
            io.StringIO(raw_text),
            sep='\s+',
            skiprows=SKIP_LINES,
            names=mesonet_cols,
            engine='python', # Python engine is necessary for regex separator
            skipinitialspace=True, # Helps with leading whitespace
            # Skip the generic footer lines (usually 3 lines of links/graphics)
            skipfooter=3 
        )
    except Exception as e:
        print(f"[ERROR] Failed to parse Mesonet data into DataFrame: {e}")
        return pd.DataFrame()
    
    # Remove any completely empty rows that might result from trailing metadata lines
    df = df.dropna(how='all').reset_index(drop=True)
    
    # 2. Create a proper datetime column
    def combine_date_time(row, year):
        try:
            # Pad month, day, and time strings (e.g., 100 -> 0100)
            month = str(int(row['Month'])).zfill(2)
            day = str(int(row['Day'])).zfill(2)
            
            # Time can be 100 (1:00) or 1000 (10:00) or 2400 (00:00 of next day)
            time_int = int(row['Time_HHMM'])
            
            if time_int == 2400:
                # Handle 2400 (end of day) as 00:00 of the next day
                date_str = f"{year}-{month}-{day} 23:59"
                dt = pd.to_datetime(date_str, format='%Y-%m-%d %H:%M')
                return dt + pd.Timedelta(minutes=1) # Roll over to 00:00 of next day
            else:
                time_str = str(time_int).zfill(4)
                hour = time_str[:-2].zfill(2)
                minute = time_str[-2:]
                date_str = f"{year}-{month}-{day} {hour}:{minute}"
                return pd.to_datetime(date_str, format='%Y-%m-%d %H:%M', errors='coerce')
        except:
            return pd.NaT

    df['valid'] = df.apply(lambda row: combine_date_time(row, data_year), axis=1)
    
    # Filter out any rows where datetime creation failed
    df = df.dropna(subset=['valid'])
    
    # 3. Add Location Metadata
    df['station'] = metadata['station_id']
    df['lat'] = metadata['lat']
    df['lon'] = metadata['lon']
    
    print(f"-> Successfully parsed {len(df)} records for Year {data_year}.")
    return df

if __name__ == '__main__':
    
    all_stations_data = []
    
    # Regex to extract the unique bull# identifier from the URL path
    # Example: extracts "bull75" from ".../boone/bull75s.htm"
    BULL_ID_PATTERN = re.compile(r'/(bull\d+)[st]\.htm', re.IGNORECASE)
    
    # Loop through the list of Mesonet station full URLs
    for url in MESONET_URL_LIST:
        
        # 1. Dynamically extract the metadata key (bull#) from the URL
        match = BULL_ID_PATTERN.search(url)
        
        if not match:
            print(f"[SKIP] Could not extract bull ID from URL: {url}. Skipping.")
            continue
            
        metadata_key = match.group(1).lower() # e.g., 'bull75'
        
        # 2. Identify the station's static metadata
        metadata = STATION_METADATA.get(metadata_key)
        
        if not metadata:
            print(f"[SKIP] No metadata found for key '{metadata_key}'. Skipping URL: {url}.")
            continue
            
        # 3. Fetch and Extract Raw Text
        raw_text_data = fetch_and_extract_pre_tag(url)
        
        if raw_text_data:
            # 4. Parse into DataFrame
            mesonet_df = parse_mesonet_data(raw_text_data, metadata)
            
            if not mesonet_df.empty:
                all_stations_data.append(mesonet_df)
                
    # 5. Combine all station data into one master DataFrame
    if all_stations_data:
        final_mesonet_df = pd.concat(all_stations_data, ignore_index=True)
        
        print("\n--- MASTER Mesonet Data Parsing Complete ---")
        print(f"Total Records Across All Stations: {len(final_mesonet_df)}")
        print("\nDataFrame Head (First 5 records):")
        print(final_mesonet_df.head())
        print("\nFull Dataframe Columns:")
        print(final_mesonet_df)
        print("\nDataFrame Info:")
        final_mesonet_df.info()
    else:
        print("\n--- Mesonet Data Parsing Failed ---")
        print("No data frames were successfully parsed or fetched.")