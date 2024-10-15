from wsgiref.util import request_uri

from flask import Flask
from google.cloud import storage

app = Flask(__name__)

bucketNames = []
blobsInProd = []
target_bucket="svg-dcc-raw-tst"

@app.route('/')
def displayBuckets():
    project_id = "svg-dcc-shr-storage-aa07"

    def getCSV(target_bucket):

        storage_client = storage.Client()
        blobs = storage_client.list_blobs(target_bucket)

        print("Buckets:")
        for bucket in blobs:
            print(bucket.name)
            print(bucket.time_created)
            bucketNames.append(bucket.name)
            bucketNames.append(bucket.time_created)
        print("Alle Buckets aufgelistet")



    getCSV(target_bucket)

    return bucketNames, blobsInProd

