# Data-Warehouse-Weather-ETL-Incremental-Update-HW6
Incremental ETL pipeline for weather data using Apache Airflow and Snowflake - Data Warehouse Homework 6

# Incremental Weather ETL Pipeline - Homework 6

## What This Project Does
This project builds a daily weather data pipeline using Apache Airflow and Snowflake.
Every day at 3:30 AM, it automatically fetches weather data for New York City 
and stores it in Snowflake database.

## How It Works
1. **Extract** - Downloads today's weather data from Open-Meteo API
2. **Load** - Saves the data into Snowflake table

## Incremental Logic
Unlike a full refresh, this pipeline only updates one day at a time:
- Deletes only today's existing record
- Inserts fresh weather data for today
- All historical data stays untouched

## Tools Used
- Apache Airflow (workflow scheduling)
- Snowflake (data storage)
- Open-Meteo API (weather data source)
- Python (programming language)
- Docker (local development)

## Database Details
- Database: USER_DB_FERRET
- Schema: RAW
- Table: WEATHER_DATA_HW6

## Author
Kshitija Shinde
DATA226 - Data Warehouse
San Jose State University
Spring 2026
