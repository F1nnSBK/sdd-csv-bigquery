from sys import prefix

from google.cloud import storage



prefix = "csvImport/"
target_bucket="svg-dcc-raw-tst"
bucket_path="svg-dcc-raw-tst/"
blob_path="svg-dcc-raw-tst/csvImport/testData/"

project_id = "svg-dcc-shr-storage-aa07"

def getCSV(target_bucket, prefix, delimiter=None):

    storage_client = storage.Client()
    blobs = storage_client.list_blobs(target_bucket, delimiter=delimiter)

    print("Blobs:")
    for blob in blobs:
        if blob.name.startswith(prefix):
            rawPathToCSV = blob.path_helper(bucket_path, blob_path)
            print(rawPathToCSV)
            pathToCSV = str(rawPathToCSV).replace("%2F", "/").removeprefix("svg-dcc-raw-tst//o/")
            bucketName = pathToCSV.split("/")[0]
            datasetName = pathToCSV.split("/")[1]
            tableName = pathToCSV.split("/")[2]

            if pathToCSV != None:
                print(f"Pfad zur CSV gefunden:\n gs://{pathToCSV}")
                print(f"Name des Buckets: {bucketName}")
                print(f"Name des Datasets: {datasetName}")
                print(f"Name des Tables: {tableName}")
                break




getCSV(target_bucket, prefix)

