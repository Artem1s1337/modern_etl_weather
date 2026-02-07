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
- Airflow orchestrated whole process with four DAGs: insert dimensions (cities, weather conditions), insert fact table and load data to Clickhouse (transform + join + load). Meta data is collected and loaded only once. Forecast data extracted, transformed and loaded daily
- You can view data by using Cloud Dbeaver UI
- Whole pipline work in isolated environment and network supported by Docker and Docker Compose
- Project dependencies installed with Python package manager called Poetry

## How to use
Before you launch the whole pipeline, make sure you have WSL and Docker installed. Also go to the [OpenWeatherMap](https://old.openweathermap.org) website and register you personal API key to get the data.
### Set up environment
Clone repository to your local folder and open it
```bash
git clone https://github.com/Artem1s1337/modern_etl_weather.git
cd path/to_your_folder
```
Copy `.env.example` as `.env` file with the following command
```bash
cp .env.example .env
```
Fill in variable values. DO NOT CHANGE THE VARIABLE NAMES!
```bash
# API key for authorization
API_KEY=  <-- put here your generated API key

# Greenplum credentials
GREENPLUM_USER=
GREENPLUM_PASSWORD=
GREENPLUM_DATABASE_NAME=

# Clickhouse credentials
CLICKHOUSE_USER=
CLICKHOUSE_PASSWORD=
CLICKHOUSE_DB=
```
Then install Poetry package manager using `pip` or `sudo` or `pipx` [(more details)](https://python-poetry.org/docs/#installation) and upgrade `pip`
```bash
pip install poetry
python.exe -m pip install --upgrade pip
```
Configure your virtual environment and install necessary packages
```bash
poetry config virtualenvs.in-project true
poetry env activate
poetry env list  # to check activation status
poetry install --no-root
```
Enter the WSL2 by using `wsl` in terminal and run the porject by the following line
```bash
docker compose up -d
```
Open three services in Docker by clicking on links: `Cloud DBeaver`, `Airflow` and `Metabase`:
1. Sign in Cloud Dbeaver, set up connection parameters and connect to `Greenplum` and `Clickhouse` databases using credentials from the `.env` file
2. Open Airflow link, fill in username as `admin` and password (you can find it in logs using CTRL + F)
3. Run the DAGs: `extract_and_insert_conditions`, `extract_and_insert_cities`, `extract_and_insert_forecast` and `transform_and_load_to_olap`
4. Open Metabase, register and you're done! Feel free to create an analytical dashboard :)

In the end you can get a beautiful and informative dashboard. For instance, I got something like this:
<img width="1132" height="871" alt="{813A182B-A396-4F40-BC0F-CF747103F1AA}" src="https://github.com/user-attachments/assets/19ccd194-3dda-4761-8a17-089f232a4bc4" />

To turn the services off use the commands below
```bash
docker compose down
docker compose down -v  # with the volumes deletion
```
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
