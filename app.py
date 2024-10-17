from google.cloud import storage
from google.cloud import bigquery
from datetime import datetime
from dotenv import load_dotenv
import os as os
import pandas as pd
from flask import Flask, render_template

load_dotenv()

prefix = os.getenv('PREFIX')
target_bucket = os.getenv('TARGET_BUCKET')
destination_file_path = os.getenv('DESTINATION_FILE_PATH')
source_project_id = os.getenv('SOURCE_PROJECT_ID')
destination_project_id = os.getenv('DESTINATION_PROJECT_ID')
source_blob_name = ""
storage_client = storage.Client()


app = Flask(__name__)
name = "Finn"
@app.route('/')
def user():
    return render_template('index.html', name=name)

@app.route('/script/', methods=['POST'])
def check():

    runScript(prefix, target_bucket, source_blob_name, destination_file_path, storage_client)
    message = "finished!"
        
    return render_template('script.html', message=message)
    
  
def runScript(prefix, target_bucket, source_blob_name, destination_file_path, storage_client):
    
    def get_csv(target_bucket, source_blob_name, destination_file_path, storage_client):
        bucket = storage_client.bucket(target_bucket)
        blob = bucket.blob(source_blob_name)
        blob.download_to_filename(destination_file_path)
        print(f"Downloaded {source_blob_name} to {destination_file_path}")

    blobs = storage_client.list_blobs(target_bucket)

    def get_csv_paths(target_bucket, prefix):
        dataset_table_pairs = []

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

                project_id=destination_project_id
                dataset_name= os.getenv('DATASET_NAME')
                current_blob = blob.name
                dataset_table_pairs.append((dataset_name, table_name, project_id, current_blob))

            else:
                print(f"Skipping: {blob.name}")

        return dataset_table_pairs



    def create_dataset_if_not_exists(bigquery_client, dataset_id):
        try:
            bigquery_client.get_dataset(dataset_id)  # Make an API request.
            print(f"Dataset {dataset_id} already exists")
        except:
            print(f"Creating dataset {dataset_id}")
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = os.getenv('DATASET_LOCATION')
            bigquery_client.create_dataset(dataset, exists_ok=True)



    def write_to_bigquery(project_id, dataset_name, table_name, destination_file_path):
        time_of_import = datetime.now().strftime("%w %b %Y : %H:%M:%S")
        file_name = table_name

        bigquery_client = bigquery.Client(project=project_id)

        dataset_id = f"{project_id}.{dataset_name}"
        table_name = table_name.split(".")[0]
        table_id = f"{dataset_id}.{table_name}"
        print(table_id)

        # Create dataset if it does not exist
        create_dataset_if_not_exists(bigquery_client, dataset_id)

        time_of_import_arr = []
        file_origin_arr = []

        # Load CSV into DataFrame
        df = pd.read_csv(destination_file_path)

        for row in df.itertuples():
            time_of_import_arr.append(f"{time_of_import}")
            file_origin_arr.append(f"{file_name}")

        df.insert(loc=len(df.columns), column="timeOfImport", value=time_of_import_arr)
        df.insert(loc=len(df.columns), column="nameOfOriginFile", value=file_origin_arr)
        df.to_parquet("df.parquet.gzip", compression="gzip")
        print(pd.read_parquet("df.parquet.gzip"))

        # Write DataFrame to BigQuery table
        job_config = bigquery.LoadJobConfig(
            # Detect Schema from PARQUET-File
            autodetect=True,
            # Set format of source file
            source_format=bigquery.SourceFormat.PARQUET,
            # If Table already exists, data is deleted and rewritten
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )


        uri = "df.parquet.gzip"

        with open(uri, "rb") as f:
            load_job = bigquery_client.load_table_from_file(f,
                                                            table_id,
                                                            job_config=job_config,
            )
            load_job.result()  # Waits for the job to complete.
        return print(f"Loaded {len(df)} rows into {table_id}.")


    # Call the function to find and download CSV files
    dataset_table_pairs = get_csv_paths(target_bucket, prefix)
    print(dataset_table_pairs)


    # Process each dataset-table pair and write to BigQuery
    for dataset_name, table_name, source_project_id, current_blob in dataset_table_pairs:
        if dataset_name and table_name and source_project_id and current_blob:
            get_csv(target_bucket, current_blob, destination_file_path, storage_client)
            write_to_bigquery(source_project_id, dataset_name, table_name, destination_file_path)
        else:
            print("Invalid dataset or table name.")
     
        

if __name__ == '__main__':
    app.run(debug=True)