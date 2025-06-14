from datetime import datetime, timedelta

import pytz
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator

from utils import get_data, transform_data, ingest_data


URL = "https://api.publibike.ch/v1/public/partner/stations"
BUCKET_NAME = "proj-etl-publibike-dev"
PROJECT_ID = "inspired-victor-442419-j3"

default_args = {
    "start_date": airflow.utils.dates.days_ago(0),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "etl_dag", schedule_interval="*/5 * * * *", default_args=default_args, catchup=False
) as dag:
    now = datetime.now(pytz.timezone("UTC"))
    current_full_date = now.strftime("%Y-%m-%d %H:%M:00")
    current_ymd = now.strftime("%Y-%m-%d")
    current_hour = now.strftime("%H:00:00")
    current_minute = now.strftime("%H:%M:00")

    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=get_data,
        op_kwargs={
            "url": URL,
            "bucket_name": BUCKET_NAME,
            "output_path": f"raw_data/{current_ymd}/{current_hour}/data.json",
        },
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform_data,
        op_kwargs={
            "input_path": f"raw_data/{current_ymd}/{current_hour}/data.json",
            "bucket_name": BUCKET_NAME,
            "output_path": f"transformed_data/{current_ymd}/{current_hour}/{current_minute}_transformed_data_bikes.csv",
            "mode": "capacity",
            "date_time": current_full_date,
        },
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=ingest_data,
        op_kwargs={
            "input_path": f"transformed_data/{current_ymd}/{current_hour}/{current_minute}_transformed_data_bikes.csv",
            "bucket_name": BUCKET_NAME,
            "project_id": PROJECT_ID,
            "dataset": "publibike_dev",
            "table": "capacity",
            "mode": "capacity",
        },
    )

extract_task >> transform_task >> load_task
