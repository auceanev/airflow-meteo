import logging
import random
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

DB_PATH = Path("/opt/airflow/dags/meteo.db")

TOTAL_MIN, TOTAL_MAX = 1000, 2000
LATENCY_MIN, LATENCY_MAX = 100.0, 250.0

default_args = {
    "owner": "auceanev",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}


def extract(**context) -> dict:
    """Appelle l'API Open Meteo et pousse les données brutes dans XCom."""
    end_date = datetime.today().strftime("%Y-%m-%d")
    start_date = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 48.85,
        "longitude": 2.35,
        "hourly": "temperature_2m,precipitation,windspeed_10m",
        "start_date": start_date,
        "end_date": end_date,
    }

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()

    logger.info("Extraction OK — %d lignes", len(data["hourly"]["time"]))
    context["ti"].xcom_push(key="raw_data", value=data)


def transform(**context) -> None:
    """Nettoie les données brutes et pousse le résultat dans XCom."""
    raw = context["ti"].xcom_pull(key="raw_data", task_ids="extract")
    hourly = raw["hourly"]

    df = pd.DataFrame({
        "timestamp": pd.to_datetime(hourly["time"]),
        "temperature_c": hourly["temperature_2m"],
        "precipitation_mm": hourly["precipitation"],
        "windspeed_kmh": hourly["windspeed_10m"],
    })

    df = df.dropna()
    df["date"] = df["timestamp"].dt.date.astype(str)
    df["heure"] = df["timestamp"].dt.hour
    df["timestamp"] = df["timestamp"].astype(str)

    df["ressenti"] = pd.cut(
        df["temperature_c"],
        bins=[-50, 0, 10, 20, 30, 60],
        labels=["Glacial", "Froid", "Frais", "Agréable", "Chaud"],
    ).astype(str)

    logger.info("Transformation OK — %d lignes", len(df))
    context["ti"].xcom_push(key="clean_data", value=df.to_dict(orient="records"))


def load(**context) -> None:
    """Charge les données transformées dans SQLite."""
    records = context["ti"].xcom_pull(key="clean_data", task_ids="transform")
    df = pd.DataFrame(records)

    with sqlite3.connect(DB_PATH) as conn:
        df.to_sql("meteo_paris", conn, if_exists="replace", index=False)

    logger.info("Chargement OK — %d lignes dans %s", len(df), DB_PATH)


with DAG(
    dag_id="etl_meteo_paris",
    description="Pipeline ETL météo Paris via Open Meteo → SQLite",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["meteo", "etl", "data-engineering"],
) as dag:

    t_extract = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    t_transform = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    t_load = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    t_extract >> t_transform >> t_load