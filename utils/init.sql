CREATE TABLE IF NOT EXISTS dim_cities (
  city_id INTEGER PRIMARY KEY,
  city_name VARCHAR(50),
  latitude DECIMAL(9, 6),
  longitude DECIMAL(9, 6)
);

CREATE TABLE IF NOT EXISTS dim_weather (
  weather_id INTEGER PRIMARY KEY,
  main VARCHAR(25),
  description VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS raw_weather (
  id SERIAL,
  dt BIGINT,
  temp DECIMAL(7, 2),
  feels_like DECIMAL(7, 2),
  temp_min DECIMAL(7, 2),
  temp_max DECIMAL(7, 2),
  pressure INT,
  humidity INT,
  weather_id INT,
  clouds INT,
  wind_speed DECIMAL(7, 2),
  wind_deg INT,
  wind_gust DECIMAL(7, 2),
  visibility DECIMAL(7, 2),
  city_id INT,
  sunrise BIGINT,
  sunset BIGINT,
  created_at TIMESTAMP
) 
DISTRIBUTED BY (city_id, dt);

CREATE UNIQUE INDEX city_dt_idx 
ON raw_weather (city_id, dt);