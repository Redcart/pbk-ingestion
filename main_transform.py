import os
import json
import logging
import base64
from google.cloud import pubsub_v1

from src.transform import Transform

# Environment variables
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
API_URL = os.getenv("API_URL", "https://api.publibike.ch/v1/public/partner/stations")
TOPIC_NAME = os.getenv("PUBSUB_TOPIC_TRANSFORM")


def transform(event, context) -> tuple[dict, int]:
    """
    Cloud Function entry point for the ETL process.

    Args:
        request (dict): The HTTP request object containing the mode.

    Returns:
        tuple[dict, int]: A response indicating the status of the ETL process and HTTP status code.
    """
    print(f"Event: {event}, Context: {context}")
    if "data" in event:
        message_data = base64.b64decode(event["data"]).decode("utf-8")
        logging.info(f"Received message: {message_data}")

    # Validate environment variables
    if not GCS_BUCKET_NAME or not GCP_PROJECT_ID:
        logging.error(
            "Missing required environment variables. You have to set GCS_BUCKET_NAME, GCP_PROJECT_ID."
        )
        return {
            "status": "error",
            "message": "Missing required environment variables.",
        }, 500

    mode = message_data.get("mode")
    timestamp = message_data.get("timestamp")
    current_ymd = timestamp.strftime("%Y-%m-%d")
    current_minute = timestamp.strftime("%H:%M:00")

    # Initialize the ETL process
    transformer = Transform(
        bucket_name=GCS_BUCKET_NAME,
        input_path=f"{mode}/raw_data/{current_ymd}/{current_minute}/data.json",
        output_path=f"{mode}/transformed_data/{current_ymd}/{current_minute}/data.csv",
        mode=mode,
        date_time=timestamp,
    )

    data = json.dumps({"mode": mode, "timestamp": timestamp}).encode("utf-8")

    # Initialize Pub/Sub client
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(GCP_PROJECT_ID, TOPIC_NAME)

    # Run the ETL process
    try:
        transformer.run()
        future = publisher.publish(topic=topic_path, data=data)
        message_id = future.result()

        logging.info(f"Extract process for mode {mode} completed successfully.")
        logging.info(f"Message published with ID: {message_id}")
        return {
            "status": "success",
            "message": f"Extract process for mode {mode} completed successfully.",
        }, 200
    except Exception as e:
        logging.error(f"Extract process failed: {str(e)}")
        return {"status": "error", "message": f"Extract process failed: {str(e)}"}, 500


if __name__ == "__main__":
    # Example request for local testing
    test_request = {"data": json.dumps({"mode": "stations"}).encode("utf-8")}
    response, status_code = transform(test_request)
    print(f"Response: {response}, Status Code: {status_code}")
