# Contemporary ETL-pipeline for the weather data of top-50 Russian cities by population
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
Data is extracted from OpenWeatherMap using official API. Extracted data is stored in a raw layer (Greenplum). Data is taken from raw database, transformed (change data types, unify scale, change units to metrics system) and combined. Then transformed and combined data is ingested to analytical layer (Clickhouse) where we can group and aggregate data. It will help us to get insights on further step. After that we connect Metabase BI system to Clickhouse to build dashboards based on analytical layer. Whole ETL process is simplified and automated regard to Airflow DAGs. You can run such processes as extraction, transformation and loading using trigger buttons. All services run in an isolated environment using Docker.

## Features
The project has some key features:
-
-
-

## How to use

## Troubleshooting
