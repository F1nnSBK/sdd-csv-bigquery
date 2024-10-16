from statistics import variance
from wsgiref.util import request_uri

from flask import Flask, render_template
from google.cloud import storage

app = Flask(__name__)

bucketNames = []
blobsInProd = []
target_bucket="svg-dcc-raw-tst"

@app.route('/')
def displayBuckets():
    project_id = "svg-dcc-shr-storage-aa07"

    return render_template("templates/index.html", bucketNames="Hallo")

if __name__ == '__main__':
    app.run(debug=True)