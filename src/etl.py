from datetime import datetime
import pytz
import logging
from src.extract import Extract
from src.transform import Transform
from src.ingest import Ingest


class ETL:
    """
    Class to handle the ETL process for different modes.
    """

    def __init__(self, mode: str, url: str, bucket_name: str, project_id: str, dataset: str):
        """
        Initializes the ETL class.

        Args:
            mode (str): The mode of the ETL process ("stations" or "capacity").
            url (str): The API URL for data extraction.
            bucket_name (str): The name of the GCS bucket.
            project_id (str): The Google Cloud project ID.
            dataset (str): The BigQuery dataset name.
        """
        self.mode = mode
        self.url = url
        self.bucket_name = bucket_name
        self.project_id = project_id
        self.dataset = dataset
        self.initialize_date_attributes()

    def initialize_date_attributes(self):
        """
        Initializes the date-related attributes for the ETL process.
        """
        now = datetime.now(pytz.timezone('UTC'))
        self.current_full_date = now.strftime("%Y-%m-%d %H:%M:00")
        self.current_ymd = now.strftime("%Y-%m-%d")
        self.current_hour = now.strftime("%H:00:00")
        self.current_minute = now.strftime("%H:%M:00")

    def extract_data(self):
        """
        Extracts data from the API and saves it to GCS.
        """
        extract = Extract(
            url=self.url,
            bucket_name=self.bucket_name,
            output_path=f"test/raw_data/{self.current_ymd}/{self.current_hour}/data.json"
        )
        extract.get_data()

    def transform_data(self):
        """
        Transforms the extracted data based on the mode and saves it to GCS.
        """
        transform = Transform(
            bucket_name=self.bucket_name,
            input_path=f"test/raw_data/{self.current_ymd}/{self.current_hour}/data.json",
            output_path=f"test/transformed_data/{self.current_ymd}/{self.current_hour}/{self.current_minute}_transformed_data_{self.mode}.csv",
            mode=self.mode,
            date_time=self.current_full_date
        )
        transform.transform_data()

    def ingest_data(self):
        """
        Ingests the transformed data into BigQuery.
        """
        ingest = Ingest(
            project_id=self.project_id,
            dataset=self.dataset,
            table="stations_test" if self.mode == "stations" else "capacity_test",
            bucket_name=self.bucket_name
        )
        ingest.ingest_data(
            input_path=f"{self.bucket_name}/test/transformed_data/{self.current_ymd}/{self.current_hour}/{self.current_minute}_transformed_data_{self.mode}.csv",
            mode=self.mode
        )

    def run(self):
        """
        Runs the ETL process.
        """
        logging.info(f"Starting ETL process for mode: {self.mode}")
        self.extract_data()
       
        self.transform_data()
      
        self.ingest_data()
        logging.info(f"ETL process for mode: {self.mode} completed successfully.")