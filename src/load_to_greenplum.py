import os
from dotenv import load_dotenv, find_dotenv
from extract import ExtractWeather
import psycopg2
import io
import pandas as pd

load_dotenv(find_dotenv())

def get_connection():
    
    return psycopg2.connect(
        dbname=os.getenv('GREENPLUM_DATABASE_NAME'),
        user=os.getenv('GREENPLUM_USER'),
        password=os.getenv('GREENPLUM_PASSWORD'),
        port=5432,
        host='greenplum'
    )

def get_extractions():
    return ExtractWeather(os.getenv('API_KEY'))

def insert_cities(conn, cursor):
    c = get_extractions()
    meta_cities = c.get_cities_chunked()
    
    meta_cities = meta_cities.where(pd.notnull(meta_cities), None)
    lst_of_tuples = list(meta_cities.itertuples(index=False, name=None))

    insert_query = '''
        INSERT INTO dim_cities (city_id, city_name, latitude, longitude)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (city_id) DO NOTHING;
    '''

    cursor.executemany(insert_query, lst_of_tuples)
    conn.commit()

def insert_conditions(conn, cursor):

    c = get_extractions()
    meta_weather = c.get_conditions()
    meta_weather = meta_weather.where(pd.notnull(meta_weather), None)
    lst_of_tuples = list(meta_weather.itertuples(index=False, name=None))

    insert_query = '''
        INSERT INTO dim_weather (weather_id, main, description)
        VALUES (%s, %s, %s)
        ON CONFLICT (weather_id) DO NOTHING;
    '''

    cursor.executemany(insert_query, lst_of_tuples)
    conn.commit()

def insert_forecast(conn, cursor):

    c = get_extractions()
    forecast = c.get_forecast_chunked()
    forecast = forecast.where(pd.notnull(forecast), None)
    lst_of_tuples = list(forecast.itertuples(index=False, name=None))

    insert_query = '''
        INSERT INTO raw_weather (dt, temp, feels_like, temp_min, temp_max, pressure, humidity, weather_id, clouds, wind_speed, wind_deg, wind_gust, visibility, city_id, sunrise, sunset, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (city_id, dt) DO NOTHING;
    '''

    cursor.executemany(insert_query, lst_of_tuples)
    conn.commit()

def load_cities():
    conn = get_connection()
    cursor = conn.cursor()

    insert_cities(conn, cursor)

    cursor.close()
    conn.close()

def load_conditions():
    conn = get_connection()
    cursor = conn.cursor()

    insert_conditions(conn, cursor)

    cursor.close()
    conn.close()

def load_forecast():
    conn = get_connection()
    cursor = conn.cursor()

    insert_forecast(conn, cursor)

    cursor.close()
    conn.close()

if __name__ == "__main__":
    load_conditions()
