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
    
    output = io.StringIO()
    meta_cities.to_csv(output, sep='\t', header=False, index=False)
    bytes_buffer = io.BytesIO(output.getvalue().encode('utf-8'))

    cursor.copy_expert("COPY dim_cities FROM STDIN WITH (FORMAT csv, DELIMITER '\t')", bytes_buffer)
    conn.commit()

def insert_conditions(conn, cursor):

    c = get_extractions()
    meta_weather = c.get_conditions()

    output = io.StringIO()
    meta_weather.to_csv(output, sep='\t', header=False, index=False)
    bytes_buffer = io.BytesIO(output.getvalue().encode('utf-8'))

    cursor.copy_expert("COPY dim_weather FROM STDIN WITH (FORMAT csv, DELIMITER '\t')", bytes_buffer)
    conn.commit()

def insert_forecast(conn, cursor):

    c = get_extractions()
    forecast = c.get_forecast_chunked()
    cols = forecast.columns

    forecast = forecast.where(pd.notnull(forecast), None)

    csv_io = io.StringIO()
    forecast.to_csv(csv_io, sep='\t', header=False, index=False, na_rep='\\N')
    csv_io.seek(0)

    cursor.copy_from(
        csv_io, 
        'raw_weather',
        columns=cols
    )
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