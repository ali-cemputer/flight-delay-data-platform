#  GCP
GCP_PROJECT_ID = "bd-project-cemputer"                  # GCP project ID
GCP_CREDENTIALS_PATH = "/opt/spark/keys/gcp-key.json"

#  GCS 
GCS_BUCKET = "bts-airline-bronze"    # bucket name (gs://)

BRONZE_PREFIX = "bronze/carrier_report"                  # raw parquets prefix
SILVER_PREFIX = "silver/carrier_report"                  # prefix of cleaned parquet

# glob pattern — use for SparkSession.read
BRONZE_READ_PATH = f"gs://{GCS_BUCKET}/{BRONZE_PREFIX}/year=*/month=*/data.parquet"

# ── BigQuery 
BQ_DATASET = "bts_airline"                               # BigQuery dataset name
BQ_TABLE = "carrier_performance"                         # target table name
BQ_TABLE_FULL = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

BQ_PARTITION_COLUMN = "FlightDate"                       # DATE type partition
BQ_CLUSTER_COLUMNS = ["Reporting_Airline", "Origin"]     # cluster order

# ── Lookup files (container-internal path) 
LOOKUP_DIR = "/opt/spark/data/lookups"
LOOKUP_CARRIERS = f"{LOOKUP_DIR}/L_UNIQUE_CARRIERS.csv"
LOOKUP_AIRPORT = f"{LOOKUP_DIR}/L_AIRPORT.csv"
LOOKUP_CANCELLATION = f"{LOOKUP_DIR}/L_CANCELLATION.csv"

# ── Colons
SELECTED_COLUMNS = [
    "Year", "Month", "DayOfWeek", "FlightDate",
    "Reporting_Airline", "Origin", "OriginCityName", "Dest", "DestCityName",
    "CRSDepTime", "DepTime", "DepDelay", "TaxiOut", "TaxiIn",
    "CRSArrTime", "ArrTime", "ArrDelay", "Cancelled", "CancellationCode",
    "Diverted", "ActualElapsedTime", "AirTime", "Distance",
    "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay",
]
