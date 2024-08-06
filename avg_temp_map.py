import argparse
import os
import requests
import pandas as pd
import gzip
from io import BytesIO
import plotly.express as px

def fetch_and_process_data(year, country_code):
    output_file = f'{country_code}_temperature_{year}_processed.csv'

    if os.path.exists(output_file):
        print(f"Data for {country_code} in {year} already exists. Skipping data fetch.")
        return output_file

    try:
        inventory_url = "https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt"

        response = requests.get(inventory_url)
        response.raise_for_status()  
        inventory_data = response.text

       
        stations = []
        for line in inventory_data.splitlines():
            country = line[0:2]
            station_id = line[0:11]
            lat = float(line[12:20])
            lon = float(line[21:30])
            if country == country_code:
                stations.append((station_id, lat, lon))

        print(f"Found {len(stations)} stations in {country_code}")

        ghcn_url = f"https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/{year}.csv.gz"

        # Fetch the data
        response = requests.get(ghcn_url)
        response.raise_for_status() 

        with gzip.open(BytesIO(response.content), 'rt') as f:
            data = pd.read_csv(f, header=None)

        data.columns = ['ID', 'DATE', 'ELEMENT', 'VALUE', 'MFLAG', 'QFLAG', 'SFLAG', 'OBS_TIME']

        station_ids = [station[0] for station in stations]
        country_data = data[data['ID'].isin(station_ids) & data['ELEMENT'].isin(['TMAX', 'TMIN', 'TAVG'])].copy()

        country_data.loc[:, 'VALUE'] = country_data['VALUE'].astype(float) / 10.0 
        country_data.loc[:, 'DATE'] = pd.to_datetime(country_data['DATE'].astype(str), format='%Y%m%d')  

        avg_temp_data = country_data.groupby(['ID']).agg({'VALUE': 'mean'}).reset_index()

        station_df = pd.DataFrame(stations, columns=['ID', 'LAT', 'LON'])
        avg_temp_data = avg_temp_data.merge(station_df, on='ID')

        avg_temp_data['SIZE'] = avg_temp_data['VALUE'].abs()  
        max_temp = avg_temp_data['SIZE'].max()
        avg_temp_data['SIZE'] = avg_temp_data['SIZE'] / max_temp * 15 

     
        avg_temp_data.to_csv(output_file, index=False)

        print(f"Data processed and saved to {output_file}")
        return output_file

    except requests.RequestException as e:
        print(f"Error fetching data: {e}")
        return None
    except Exception as e:
        print(f"Error processing data: {e}")
        return None

def visualize_data(csv_file, country_code, year):
    if not os.path.exists(csv_file):
        print(f"CSV file {csv_file} does not exist. Cannot visualize.")
        return

    try:
        avg_temp_data = pd.read_csv(csv_file)
        fig = px.scatter_mapbox(
            avg_temp_data,
            lat='LAT',
            lon='LON',
            color='VALUE',
            color_continuous_scale='thermal',
            size='SIZE',
            size_max=15,
            zoom=4,
            mapbox_style='carto-positron',
            title=f'Average Temperature in {country_code} ({year})',
            labels={'VALUE': 'Temperature (°C)'}
        )

        fig.show()

    except Exception as e:
        print(f"Error visualizing data: {e}")

def main():
    parser = argparse.ArgumentParser(description='Fetch and visualize temperature data for a specified year and country.')
    parser.add_argument('--year', type=int, required=True, help='The year for which to fetch the data')
    parser.add_argument('--country', type=str, required=True, help='The country code for which to fetch the data (e.g., NO for Norway)')

    args = parser.parse_args()

    csv_file = fetch_and_process_data(args.year, args.country)

    if csv_file:
        visualize_data(csv_file, args.country, args.year)

if __name__ == "__main__":
    main()
