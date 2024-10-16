from google.cloud import storage

prefix = "csvImport/"
target_bucket = "svg-dcc-raw-tst"
destination_file_path = "/Users/finnhertsch/PycharmProjects/sdd-csv-bigquery/download/test.csv"

project_id = "svg-dcc-shr-storage-aa07"


def getCSVPath(target_bucket, prefix):
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(target_bucket)

    print("Blobs:")

    for blob in blobs:
        print(blob.name)  # Print all blob names for debugging

        if blob.name.startswith(prefix) and not blob.name.endswith('/'):
            path_to_csv = f"{target_bucket}/{blob.name}"
            print(f"Pfad zur CSV gefunden:\n gs://{path_to_csv}")

            # Get dataset and table names
            datasetName = path_to_csv.split("/")[1]
            tableName = path_to_csv.split("/")[2]
            print(datasetName)
            print(tableName)

            # Call Function to download the CSV file
            getCSV(target_bucket, blob.name, destination_file_path, storage_client)
            break  # Exit the loop after finding and downloading the first matching blob
        else:
            print(f"Skipping: {blob.name}")


def getCSV(target_bucket, source_blob_name, destination_file_path, storage_client):
    bucket = storage_client.bucket(target_bucket)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_path)
    print(f"Downloaded {source_blob_name} to {destination_file_path}")


# Call the function to find and download the CSV file
getCSVPath(target_bucket, prefix)