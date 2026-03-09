import pandas as pd
from google.cloud import storage
import os

BUCKET_NAME = "airflow-etl-raw-data"

# Use absolute paths inside the container
LOCAL_RAW_PATH = "/opt/airflow/data/raw_sales.csv"
LOCAL_PROCESSED_PATH = "/opt/airflow/data/processed_sales.csv"

def transform_data():

    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    # Download raw file from GCS
    blob = bucket.blob("raw/retail_sales.csv")
    blob.download_to_filename(LOCAL_RAW_PATH)
    print(f"Downloaded raw data to {LOCAL_RAW_PATH}")

    # Load data
    df = pd.read_csv(LOCAL_RAW_PATH)

    # Simple transformations
    df = df.drop_duplicates()
    df = df.dropna()

    # Create new column
    df["total_bill_with_tip"] = df["total_bill"] + df["tip"]

    # Save processed file
    df.to_csv(LOCAL_PROCESSED_PATH, index=False)
    print(f"Data transformed and saved to {LOCAL_PROCESSED_PATH}")

    # Upload processed file to GCS
    processed_blob = bucket.blob("processed/processed_sales.csv")
    processed_blob.upload_from_filename(LOCAL_PROCESSED_PATH)
    print("Processed data uploaded to GCS")

if __name__ == "__main__":
    transform_data()