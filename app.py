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

app = Flask(__name__)
name = "Finn"
@app.route('/')
def user():
    return render_template('index.html', name=name)

@app.route('/script/', methods=['POST'])
def check():
    process = "active"
    if process == "active":
        message = "processing..."
    else:
        message = "finished"
        
    return render_template('script.html', message=message)
    
  
        
        

if __name__ == '__main__':
    app.run(debug=True)