import os
from datetime import datetime
import json
import logging 
import pytz

from utils import get_data, transform_data, ingest_data

URL = "https://api.publibike.ch/v1/public/partner/stations"
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("DATASET")

def extract_transform_load(request):

    mode = json.loads(request.data.decode()).get("mode")
    logging.info(f" mode: {mode}")

    now = datetime.now(pytz.timezone('UTC'))
    current_full_date = now.strftime("%Y-%m-%d %H:%M:00")
    current_ymd = now.strftime("%Y-%m-%d")
    current_hour = now.strftime("%H:00:00")
    current_minute = now.strftime("%H:%M:00")

    get_data(
        url=URL, 
        bucket_name=GCS_BUCKET_NAME,
        output_path=f"raw_data/{current_ymd}/{current_hour}/data.json"
    )

    if mode == "stations":

        transform_data(
            input_path=f"raw_data/{current_ymd}/{current_hour}/data.json",
            bucket_name=GCS_BUCKET_NAME,
            output_path=f"transformed_data/{current_ymd}/{current_hour}/{current_minute}_transformed_data_stations.csv",
            mode="stations",
            date_time=current_full_date
        )

        ingest_data(
            input_path=f"transformed_data/{current_ymd}/{current_hour}/{current_minute}_transformed_data_stations.csv", 
            project_id=GCS_BUCKET_NAME,
            dataset=DATASET, 
            table="stations", 
            mode="stations"
        )

    else:

        transform_data(
            input_path=f"raw_data/{current_ymd}/{current_hour}/data.json",
            bucket_name=GCS_BUCKET_NAME,
            output_path=f"transformed_data/{current_ymd}/{current_hour}/{current_minute}_transformed_data_bikes.csv",
            mode="capacity",
            date_time=current_full_date
        )

        ingest_data(
            input_path=f"transformed_data/{current_ymd}/{current_hour}/{current_minute}_transformed_data_bikes.csv", 
            project_id=GCP_PROJECT_ID,
            dataset=DATASET, 
            table="capacity", 
            mode="capacity"
        )

    return "200"