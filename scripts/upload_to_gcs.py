from google.cloud import storage
import os

bucket_name = "airflow-etl-raw-data"
# Update this path to the container path
source_file = "/opt/airflow/data/retail_sales.csv"
destination_blob = "raw/retail_sales.csv"

def upload_file():
    client = storage.Client()
    bucket = client.bucket("airflow-etl-raw-data")
    blob = bucket.blob("processed/processed_sales.csv")

    blob.upload_from_filename(source_file)

    print(f"Uploaded {source_file} to GCS")

if __name__ == "__main__":
    upload_file()