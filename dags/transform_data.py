import pandas as pd
from google.cloud import storage

BUCKET_NAME = "airflow-etl-raw-data"

def transform_data():

    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    # Download raw file
    blob = bucket.blob("raw/retail_sales.csv")
    blob.download_to_filename("data/raw_sales.csv")

    print("Downloaded raw data")

    # Load data
    df = pd.read_csv("data/raw_sales.csv")

    # Simple transformations
    df = df.drop_duplicates()
    df = df.dropna()

    # Create new column
    df["total_bill_with_tip"] = df["total_bill"] + df["tip"]

    # Save processed file
    df.to_csv("data/processed_sales.csv", index=False)

    print("Data transformed")

    # Upload processed file
    processed_blob = bucket.blob("processed/processed_sales.csv")
    processed_blob.upload_from_filename("data/processed_sales.csv")

    print("Processed data uploaded")

if __name__ == "__main__":
    transform_data()