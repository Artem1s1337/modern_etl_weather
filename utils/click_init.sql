CREATE TABLE IF NOT EXISTS forecast_db.forecast (
  dt DateTime('Europe/Moscow'),
  temp Float64,
  feels_like Float64,
  temp_min Float64,
  temp_max Float64,
  pressure Float64,
  humidity Float64,
  clouds Float64,
  wind_speed Float64,
  wind_deg Int64,
  wind_gust Float64,
  visibility Float64,
  city_id UInt64,
  sunrise DateTime('Europe/Moscow'),
  sunset DateTime('Europe/Moscow'),
  created_at DateTime('Europe/Moscow'),
  city_name String,
  latitude Float64,
  longitude Float64,
  main String,
  description String,
  version UInt64 DEFAULT 1
)
-- Upsert mechanism based on ReplacingMergeTree technique
ENGINE = ReplacingMergeTree(version)
PRIMARY KEY (city_id, dt)
ORDER BY (city_id, dt);