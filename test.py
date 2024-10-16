from google.cloud import storage
from google.cloud import bigquery
import pandas as pd

prefix = "csvImport/"
target_bucket = "svg-dcc-raw-tst"
destination_file_path = "/Users/finnhertsch/PycharmProjects/sdd-csv-bigquery/download/test.csv"
project_id = "svg-dcc-shr-storage-aa07"


def get_csv(target_bucket, source_blob_name, destination_file_path, storage_client):
    bucket = storage_client.bucket(target_bucket)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_path)
    print(f"Downloaded {source_blob_name} to {destination_file_path}")


def get_csv_path(target_bucket, prefix):
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(target_bucket)

    print("Blobs:")

    for blob in blobs:
        print(blob.name)  # Print all blob names for debugging

        if blob.name.startswith(prefix) and not blob.name.endswith('/'):
            path_to_csv = f"{target_bucket}/{blob.name}"
            print(f"Pfad zur CSV gefunden:\n gs://{path_to_csv}")

            # Get dataset and table names
            path_parts = blob.name.split('/')
            dataset_name = path_parts[1]  # or adjust based on actual structure
            table_name = path_parts[2]  # or adjust based on actual structure
            print(f"Dataset: {dataset_name}, Table: {table_name}")

            # Call function to download the CSV file
            get_csv(target_bucket, blob.name, destination_file_path, storage_client)
            project_id="svg-dcc-tst-datawarehouse-aa07"
            dataset_name="test_finn"
            return dataset_name, table_name, project_id
        else:
            print(f"Skipping: {blob.name}")

    return None, None


def create_dataset_if_not_exists(bigquery_client, dataset_id):
    try:
        bigquery_client.get_dataset(dataset_id)  # Make an API request.
        print(f"Dataset {dataset_id} already exists")
    except:
        print(f"Creating dataset {dataset_id}")
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        bigquery_client.create_dataset(dataset, exists_ok=True)


def write_to_bigquery(project_id, dataset_name, table_name, destination_file_path):
    bigquery_client = bigquery.Client(project=project_id)

    dataset_id = f"{project_id}.{dataset_name}"
    table_name = table_name.split(".")[0]
    table_id = f"{dataset_id}.{table_name}"
    print(table_id)

    # Create dataset if it does not exist
    create_dataset_if_not_exists(bigquery_client, dataset_id)

    # Load CSV file into DataFrame
    df = pd.read_csv(destination_file_path)

    # Write DataFrame to BigQuery table
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.CSV
    )

    load_job = bigquery_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    load_job.result()  # Waits for the job to complete.
    print(f"Loaded {len(df)} rows into {table_id}.")


# Call the function to find and download the CSV file

dataset_name, table_name, project_id = get_csv_path(target_bucket, prefix)

# If a table name was found, create the table_id and write to BigQuery
if dataset_name and table_name:
    write_to_bigquery(project_id, dataset_name, table_name, destination_file_path)
else:
    print("No table found.")