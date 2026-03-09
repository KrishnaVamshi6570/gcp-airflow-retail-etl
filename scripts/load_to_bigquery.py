from google.cloud import bigquery

def load_to_bigquery():

    PROJECT_ID = "cdebatch52-486907"
    DATASET = "retail_sales_etl_dataset"
    TABLE = "retail_sales_cleaned"

    uri = "gs://airflow-etl-raw-data/processed/processed_sales.csv"

    client = bigquery.Client()

    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True
    )

    load_job = client.load_table_from_uri(
        uri,
        table_id,
        job_config=job_config
    )

    load_job.result()

    print("Loaded data into BigQuery")