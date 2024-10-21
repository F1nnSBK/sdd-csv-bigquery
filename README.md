
Für das lokale Hosten muss folgendes eingerichtet sein:
- 
+ .env muss korrekt konfiguriert sein.
    Beispiel für eine .env ist unten in der README zu finden.  
     
+ Die Credentials für die google cloud müssen eingerichtet sein.
+ Im Python environment müssen die nötigen requirements installiert sein.
+ pyarrow Installation macht manchmal Probleme



##### Name des Ordners, in dem sich der Unterordner mit den csv Dateien befindet
PREFIX = "csvImport/"

##### In welchem Bucket befinden sich die Daten
TARGET_BUCKET = "svg-dcc-raw-tst"

##### Wohin werden die csv Daten heruntergeladen (temp file, wird von der .gitignore erfasst)
DESTINATION_FILE_PATH = "/download/temp.csv"

##### Aus welchem GCS Project kommen die csv Daten?
SOURCE_PROJECT_ID = "svg-dcc-shr-storage-aa07"

##### In welches BigQuery Projekt sollen die Daten geladen werden?
DESTINATION_PROJECT_ID = "svg-dcc-tst-datawarehouse-aa07"

##### Location des Dataset Servers
##### Mehr Informationen unter:
##### https://cloud.google.com/bigquery/docs/locations?hl=en
DATASET_LOCATION = "europe-west3" # Frankfurt = europe-west3

##### Sollte nur für Testzwecke verwendet werden (Name des Unterordners wird überschrieben)
##### Ansonsten wird der DATASET_NAME aus dem Pfad zur .csv abgelesen und ein neues Dataset erstellt.
DATASET_NAME = "test_finn"