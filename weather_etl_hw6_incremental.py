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
    # Get the current context
    context = get_current_context()
    return str(context['logical_date'])[:10]


def get_next_day(date_str):
    """
    Given a date string in 'YYYY-MM-DD' format, returns the next day as a string in the same format.
    """
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    next_day = date_obj + timedelta(days=1)
    return next_day.strftime("%Y-%m-%d")


def return_snowflake_conn(con_id):
    hook = SnowflakeHook(snowflake_conn_id=con_id)
    conn = hook.get_conn()
    return conn.cursor()


def get_past_weather(start_date, end_date, latitude, longitude):
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
    data = get_past_weather(start_date, end_date, latitude, longitude)
    data['city'] = city
    data.to_csv(file_path, index=False)
    return


def populate_table_via_stage(cur, database, schema, table, file_path):
    """
    Populate a table with data from a given CSV file using Snowflake's COPY INTO command.
    """
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
    #  FIX 1: Use get_logical_date() to get today's date from Airflow context
    date_to_fetch = get_logical_date()

    #  FIX 2: Use get_next_day() to get the day after date_to_fetch
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

        # Create table if it doesn't exist yet
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {database}.{schema}.{target_table} (
            date date, temp_max float, temp_min float, precipitation float, weather_code varchar, city varchar
        )""")

        # FIX 3: DELETE existing records for this date range to avoid duplicates
        # This is the KEY difference from HW5 (HW5 deleted ALL rows; HW6 only deletes the specific date)
        cur.execute(f"""
            DELETE FROM {database}.{schema}.{target_table}
            WHERE date >= '{date_to_fetch}' AND date <= '{next_day_of_date_to_fetch}'
        """)

        #  FIX 4: Call populate_table_via_stage with all required parameters
        populate_table_via_stage(cur, database, schema, target_table, file_path)

        cur.execute("COMMIT;")

    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e


# ===================== DAG DEFINITION =====================
with DAG(
    dag_id='weather_ETL_incremental',       #  FIX 5: Added _incremental suffix
    start_date=datetime(2026, 2, 28),        # FIX 6: Start date set to Feb 28, 2026
    catchup=False,                           #  FIX 7: catchup set to False
    tags=['ETL'],
    schedule='30 3 * * *',
    max_active_runs=1
) as dag:

    # FIX 8: Use Variable.get() to read LATITUDE and LONGITUDE from Airflow Variables
    LATITUDE = Variable.get("LATITUDE")
    LONGITUDE = Variable.get("LONGITUDE")
    CITY = "New_York"                        #  No space - avoids Snowflake file path error

    database = "USER_DB_FERRET"              #  Your Snowflake database from HW5
    schema = "raw"
    target_table = "WEATHER_DATA_HW6"        #  New table name for HW6

    file_path = extract(CITY, LONGITUDE, LATITUDE)
    load(file_path, database, schema, target_table)
