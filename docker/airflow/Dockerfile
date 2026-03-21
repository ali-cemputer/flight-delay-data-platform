# Base image: Apache Airflow 2.11.2 — stable version
FROM apache/airflow:2.11.2

USER airflow

# We embed the extra packages into the image:
# - apache-airflow-providers-google : Adds GCS/BigQuery operators to Airflow
# - google-cloud-storage            : Directly access to GCS with Python
# - google-cloud-bigquery           : Directly access to BigQuery with Python
# - pandas                          : CSV/DataFrame jobs (ETL branch)
# - pyarrow                         : Parquet read/write (ELT branch)
RUN pip install --no-cache-dir \
    apache-airflow-providers-google \
    google-cloud-storage \
    google-cloud-bigquery \
    pandas \
    pyarrow

# GCP JSON key cannot be copied here — it's a security risk.