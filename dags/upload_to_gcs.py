from google.cloud import storage

bucket_name = "airflow-etl-raw-data"
source_file = "data/retail_sales.csv"
destination_blob = "raw/retail_sales.csv"

def upload_file():
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)

    blob.upload_from_filename(source_file)

    print("Upload successful!")

if __name__ == "__main__":
    upload_file()