from datetime import datetime, timedelta
import os
import requests
import pandas as pd

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


def get_logical_date():
    # Retrieves the logical date from the current Airflow context
    context = get_current_context()
    return str(context['logical_date'])[:10]


def get_next_day(date_str):
    # Returns the next day given a date string in YYYY-MM-DD format
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    next_day = date_obj + timedelta(days=1)
    return next_day.strftime("%Y-%m-%d")


def return_snowflake_conn(con_id):
    # Initializes and returns a Snowflake cursor using the given connection ID
    hook = SnowflakeHook(snowflake_conn_id=con_id)
    conn = hook.get_conn()
    return conn.cursor()


def get_past_weather(start_date, end_date, latitude, longitude):
    # Fetches daily weather data from Open-Meteo API for a given date range and location
    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "weather_code"
        ],
        "timezone": "America/Los_Angeles"
    }

    response = requests.get(url, params=params)
    data = response.json()

    df = pd.DataFrame({
        "date": data["daily"]["time"],
        "temp_max": data["daily"]["temperature_2m_max"],
        "temp_min": data["daily"]["temperature_2m_min"],
        "precipitation": data["daily"]["precipitation_sum"],
        "weather_code": data["daily"]["weather_code"]
    })

    df["date"] = pd.to_datetime(df["date"])
    return df


def save_weather_data(city, latitude, longitude, start_date, end_date, file_path):
    # Fetches weather data and saves it as a CSV file with city column added
    data = get_past_weather(start_date, end_date, latitude, longitude)
    data['city'] = city
    data.to_csv(file_path, index=False)
    return


def populate_table_via_stage(cur, database, schema, table, file_path):
    # Loads data from a CSV file into Snowflake using a temporary stage and COPY INTO command
    stage_name = f"TEMP_STAGE_{table}"
    file_name = os.path.basename(file_path)

    cur.execute(f"USE SCHEMA {database}.{schema}")
    cur.execute(f"CREATE TEMPORARY STAGE {stage_name}")
    cur.execute(f"PUT file://{file_path} @{stage_name}")

    copy_query = f"""
        COPY INTO {schema}.{table}
        FROM @{stage_name}/{file_name}
        FILE_FORMAT = (
                TYPE = 'CSV'
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                SKIP_HEADER = 1
        )
    """
    cur.execute(copy_query)

# ===================== TASK 1: EXTRACT =====================
@task
def extract(city, longitude, latitude):
    # Gets the logical date from Airflow context for incremental processing
    date_to_fetch = get_logical_date()

    # Calculates the next day to use as end_date for the API call
    next_day_of_date_to_fetch = get_next_day(date_to_fetch)

    file_path = f"/tmp/{city}_{date_to_fetch}.csv"

    save_weather_data(city, latitude, longitude, date_to_fetch, next_day_of_date_to_fetch, file_path)

    return file_path

# ===================== TASK 2: LOAD =====================
@task
def load(file_path, database, schema, target_table):
    date_to_fetch = get_logical_date()
    next_day_of_date_to_fetch = get_next_day(date_to_fetch)

    print(f"========= Updating {date_to_fetch}'s data ===========")
    cur = return_snowflake_conn("snowflake_conn")

    try:
        cur.execute("BEGIN;")

        # Create the target table if it does not already exist
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {database}.{schema}.{target_table} (
            date date, temp_max float, temp_min float, precipitation float, weather_code varchar, city varchar
        )""")

        # Incremental update: delete only the records for the current date range
        # This preserves all historical data while allowing today's data to be refreshed
        cur.execute(f"""
            DELETE FROM {database}.{schema}.{target_table}
            WHERE date >= '{date_to_fetch}' AND date <= '{next_day_of_date_to_fetch}'
        """)

        # Load fresh data from the staged CSV file into the target table
        populate_table_via_stage(cur, database, schema, target_table, file_path)

        cur.execute("COMMIT;")

    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e

# ===================== DAG DEFINITION =====================
with DAG(
    dag_id='weather_ETL_incremental',
    start_date=datetime(2026, 2, 28),
    catchup=False,
    tags=['ETL'],
    schedule='30 3 * * *',
    max_active_runs=1
) as dag:

    LATITUDE = Variable.get("LATITUDE")
    LONGITUDE = Variable.get("LONGITUDE")
    CITY = "New_York"

    database = "USER_DB_FERRET"
    schema = "raw"
    target_table = "WEATHER_DATA_HW6"

    file_path = extract(CITY, LONGITUDE, LATITUDE)
    load(file_path, database, schema, target_table)
