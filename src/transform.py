import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv, find_dotenv
import os
from clickhouse_driver import Client
import time

load_dotenv(find_dotenv())

def connect_to_gpdb():

    host = 'greenplum'
    port = 5432
    dbname = os.getenv('GREENPLUM_DATABASE_NAME')
    user = os.getenv('GREENPLUM_USER')
    password = os.getenv('GREENPLUM_PASSWORD')

    conn_string = f'postgresql://{user}:{password}@{host}:{port}/{dbname}'
    engine = create_engine(conn_string)

    forecast_query = '''
    SELECT *
    FROM raw_weather
    WHERE
    (to_timestamp(dt) AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Moscow')::timestamp >= date_trunc('day', (select current_date))
    AND (to_timestamp(dt) AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Moscow')::timestamp < (date_trunc('day', (select current_date)) + INTERVAL '6 days');
    '''

    cities_query = '''
    SELECT * FROM dim_cities;
    '''

    conditions_query = '''
    SELECT * FROM dim_weather;
    '''
    cities = pd.read_sql(cities_query, engine)
    conditions = pd.read_sql(conditions_query, engine)
    forecast = pd.read_sql(forecast_query, engine)

    return [cities, conditions, forecast]

def transform_data(df):

    # Convert UNIX to timestamp
    cols = ['dt', 'sunrise', 'sunset']
    for _ in cols:
        df[_] = pd.to_datetime(df[_], unit='s', utc=True)

        # Set timezone to Moscow
        df[_] = df[_].dt.tz_convert('Europe/Moscow')

    # Filter data
    today = pd.to_datetime('today')  # get today timestamp
    five_days_ahead = today + pd.Timedelta(days=5)  # calc delta for forecast

    # Filter data based on condition
    df = df[(df['dt'].dt.date >= today.date()) & (df['dt'].dt.date <= five_days_ahead.date())]

    # Convert Kelvin to Celsius
    cols = ['temp', 'feels_like', 'temp_min', 'temp_max']

    # Kelvin coef
    coef = 273.15

    for _ in cols:
        df[_] = df[_] - coef

    # Convert hPa to millimetres of mercury
    # hPa coef
    hPa_coef = 0.75
    df['pressure'] = df['pressure'] * hPa_coef

    # Convert humidity and clouds to percentages
    cols = ['humidity', 'clouds']
    for _ in cols:
        df[_] = (df[_] / 100).round(2)

    # Convert visibility from meters to kilometers
    df['visibility'] = (df['visibility'] / 1000).round(2)

    # Drop redundant columns (useless for column-based db like Clickhouse)
    df.drop(columns=['id', 'weather_id'], inplace=True)

    df['version'] = int(time.time())

    return df

def join_data(frames: list):

    # Unpack frames
    cities, conditions, forecast = [*frames]

    # Inner join for cities metadata and forecast
    first_join = pd.merge(forecast, cities, how='inner', on='city_id')

    # Inner join for weather conditions and forecast
    second_join = pd.merge(first_join, conditions, how='inner', on='weather_id')

    return second_join

def load_to_clickhouse(df):

    client = Client(
        host='clickhouse',
        port=9000,
        user=os.getenv('CLICKHOUSE_USER'),
        password=os.getenv('CLICKHOUSE_PASSWORD'),
        database=os.getenv('CLICKHOUSE_DB'),
        settings=dict(use_numpy=True)
    )
    client.insert_dataframe(
        'INSERT INTO forecast VALUES',
        df
    )

# Wraper functions
def extract_raw():
    return connect_to_gpdb()

def transform_forecast():
    raw_data = extract_raw()
    final_df = join_data(raw_data)
    final_df = transform_data(final_df)
    
    return final_df

def load_to_olap():
    load_to_clickhouse(transform_forecast())