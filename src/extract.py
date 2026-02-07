import requests  # to make GET requests for data
import pandas as pd  # to create DataFrames from JSON
from datetime import datetime  # to get current timestamp
import time  # set delays between requests
import logging  # track progress of extraction
import sys

# Set up logger to monitor proccess of data extraction
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class ExtractWeather:

    # List of cities we want to gather the data
    CITIES=[
        "Moscow", "Saint Petersburg", "Novosibirsk", "Yekaterinburg", "Kazan",
        "Nizhny Novgorod", "Chelyabinsk", "Krasnoyarsk", "Samara", "Ufa",
        "Rostov-on-Don", "Omsk", "Krasnodar", "Voronezh", "Perm", "Volgograd",
        "Saratov", "Tyumen", "Tolyatti", "Izhevsk", "Barnaul", "Ulyanovsk",
        "Irkutsk", "Khabarovsk", "Yaroslavl", "Vladivostok", "Makhachkala",
        "Tomsk", "Orenburg", "Kemerovo", "Novokuznetsk", "Ryazan",
        "Naberezhnye Chelny", "Astrakhan", "Penza", "Kirov", "Lipetsk",
        "Balashikha", "Cheboksary", "Kaliningrad", "Tula", "Stavropol",
        "Kursk", "Ulan-Ude", "Tver", "Magnitogorsk", "Sochi", "Donetsk",
        "Belgorod", "Bryansk"
    ]
    
    def __init__(self, api_key):
        '''
        Class initialization function.

        Uses API key to initialize the class.
        '''

        # API key for authorization
        self.api_key = api_key
    
    def get_cities_chunked(self, chunk_size=5, delay=1.0):
        '''
        Function to get metadata about cities.
        
        Make a get request to API and return list of tuples with city_id, citiy_name, latitude and longitude.
        '''
        # Create a list for data
        data = []

        # Set up API endpoint
        url = 'https://api.openweathermap.org/data/2.5/forecast'

        # Divide cities into chunks
        for i in range(0, len(self.CITIES), chunk_size):
            chunk = self.CITIES[i:i + chunk_size]
            logger.info(f'Process chunk {i // chunk_size + 1}/{(len(self.CITIES) - 1) // chunk_size + 1}')

            for city in chunk:
                try:
                    # Set up request parameters
                    params = {
                        'q': f'{city},RU',
                        'appid': self.api_key,
                    }
            
                    # Make a get request
                    resp = requests.get(url, params=params, timeout=10)

                    # Check the status of the request
                    if resp.status_code == 200:  # if it's successfull
                        jsn = resp.json()  # get json
                    

                        # Create dictionary for data
                        row = {
                            'city_id': jsn['city'].get('id'),
                            'city_name': jsn['city'].get('name'),
                            'latitude': jsn['city']['coord'].get('lat'),
                            'longitude': jsn['city']['coord'].get('lon')
                        }

                        # Add it to our list
                        data.append(row)
                    else:
                        logger.info(f'Error for city: {city}: {resp.status_code}')
    

                except Exception as e:
                    logger.info(f"Unexpected error for {city}: {e}")
            
            # Pause between chunks
            if i + chunk_size < len(self.CITIES):
                time.sleep(delay)

        # Return the data frame
        return pd.DataFrame(data)
    
    def get_conditions(self):
        '''
        Function to get metadata about weather conditions.
        
        Uses pd.read_html to gather tables with weather conditions.
        Return list of tuples with weather conditions.
        '''
        # Set the link to gather weather conditions
        url = 'https://old.openweathermap.org/weather-conditions'

        # First table contains irrelevant data
        df = pd.concat(pd.read_html(url)[1:])

        # Drop irrelevant column
        df.drop(columns=[3], inplace=True)

        # Rename columns for clarity
        df.rename(columns={0: 'weather_id', 1: 'main', 2: 'description'}, inplace=True)

        # Reset and drop old index
        df.reset_index(inplace=True)
        df.drop(columns=['index'], inplace=True)

        # Return data frame
        return df
    
    def get_forecast_chunked(self, cnt=40, chunk_size=10, delay_between_chunks=5):
            """
            Fetching forecast data using chunks to make it fast and efficient.
            """
            logger.info(f"Start fetching data. One chunk={chunk_size} cities...")
            
            all_data = []
            total_chunks = (len(self.CITIES) + chunk_size - 1) // chunk_size
            
            for chunk_num in range(total_chunks):
                start_idx = chunk_num * chunk_size
                end_idx = min(start_idx + chunk_size, len(self.CITIES))
                chunk_cities = self.CITIES[start_idx:end_idx]
                
                logger.info(f"Chunk {chunk_num + 1}/{total_chunks}: city {start_idx + 1}-{end_idx}")
                
                # Fetch chunk sequently
                for city in chunk_cities:
                    try:
                        url = 'https://api.openweathermap.org/data/2.5/forecast'
                        params = {
                            'q': f'{city},RU',
                            'appid': self.api_key,
                            'cnt': cnt,
                        }
                        
                        resp = requests.get(url, params=params, timeout=30)
                        
                        if resp.status_code == 200:
                            jsn = resp.json()
                            
                            city_sunrise = jsn['city']['sunrise']
                            city_sunset = jsn['city']['sunset']
                            city_id = jsn['city'].get('id')
                            
                            for item in jsn['list']:
                                row = {
                                    'dt': item.get('dt'),
                                    'temp': item['main'].get('temp'),
                                    'feels_like': item['main'].get('feels_like'),
                                    'temp_min': item['main'].get('temp_min'),
                                    'temp_max': item['main'].get('temp_max'),
                                    'pressure': item['main'].get('pressure'),
                                    'humidity': item['main'].get('humidity'),
                                    'weather_id': item['weather'][0].get('id') if item.get('weather') else None,
                                    'clouds': item.get('clouds', {}).get('all'),
                                    'wind_speed': item.get('wind', {}).get('speed'),
                                    'wind_deg': item.get('wind', {}).get('deg'),
                                    'wind_gust': item.get('wind', {}).get('gust'),
                                    'visibility': item.get('visibility'),
                                    'city_id': city_id,
                                    'sunrise': city_sunrise,
                                    'sunset': city_sunset,
                                    'created_at': datetime.now()
                                }
                                all_data.append(row)
                            
                            logger.info(f"{city}: {len(jsn['list'])} records")
                            
                        elif resp.status_code == 429:
                            logger.warning(f"Limit! Wait for 60 sec...")
                            time.sleep(60)
                            continue
                            
                        else:
                            logger.error(f"Error {resp.status_code} for {city}")
                        
                        # Delay between cities in chunk
                        time.sleep(1.0)
                        
                    except Exception as e:
                        logger.error(f"Error for {city}: {str(e)}")
                        time.sleep(2)
                
                # Delay between chunks
                if chunk_num < total_chunks - 1:
                    logger.info(f"Pause between chunks: {delay_between_chunks} sec")
                    time.sleep(delay_between_chunks)
            
            logger.info(f"Collected {len(all_data)} records.")
            return pd.DataFrame(all_data)
