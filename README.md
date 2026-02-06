# Contemporary weather ETL-pipeline
You can get more info about project going throught this readme:
1. [Architecture](#Architecture)
2. [Features](#Features)
3. [How to use](#How-to-use)
4. [Troubleshooting](#Troubleshooting)

## Architecture
<p align="center">
  <img src="https://github.com/user-attachments/assets/82082bf3-e4f3-4b8d-bbfc-8d3d524c2b54" alt="ETL">
  <br>
  <em>ETL process schematic view (drawn in draw.io)</em>
</p>

### How it works?
As an example, I used 50 Russian cities with the highest population. Data is extracted from OpenWeatherMap using official API. Extracted data is stored in a raw layer (Greenplum). Data is taken from raw database, transformed (change data types, unify scale, change units to metrics system) and combined. Then transformed and combined data is ingested to analytical layer (Clickhouse) where we can group and aggregate data. It will help us to get insights on further step. After that we connect Metabase BI system to Clickhouse to build dashboards based on analytical layer. Whole ETL process is simplified and automated regard to Airflow DAGs. You can run such processes as extraction, transformation and loading using trigger buttons. All services run in an isolated environment using Docker.

## Features
### The project has some key features:
- modern stack (Greenplum, Clickhouse, Airflow) that is easy to scale on big data
- working with OOP (custom class to work with API)
- data is extracted using chunks to optimize performance (spend less time & get data quicker)
- Greenplum is deployed on a single-node configuration (master and segment are deployed on the only one machine)
- Fact and dimension tables are created using DISTRIBUTION BY complex key (combination of city_id and dt - UNIX time for forecast) to prevent duplicated records inside database
- Data loading (to raw layer and to analytical layer) is implemented by the fastest methods: COPY FROM for Greenplum and clickhouse_driver for Clickhouse
- Data is transformed with pandas
- Clickhouse table based on ReplacingMergeTree (upsert mechanism) to get rid of duplicated records (if you need it)
- Airflow orchestrated whole process with four DAGs: insert dimensions (cities, weather conditions), insert fact table and load data to Clickhouse (transform + join + load)
- You can view data by using Cloud Dbeaver UI
- Whole pipline work in isolated environment and network supported by Docker and Docker Compose
- Project dependencies installed with Python package manager called Poetry

## How to use

## Troubleshooting
If you ran all services, sign in Airflow service and saw no DAGs, try out the following commands:
Simple method
```bash
docker compose restart airflow
```
Alternative methods: without and with volumes deletion
```bash
docker compose down airflow
```
```bash
docker compose down -v airflow
```
If commands above didn't help, turn off and then turn on again all containters
```bash
docker compose down -v
```
