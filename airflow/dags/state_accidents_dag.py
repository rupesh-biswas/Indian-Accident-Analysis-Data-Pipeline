from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from google.cloud import bigquery, storage
from datetime import datetime, timedelta
import pandas as pd
import os
import requests
from google.oauth2 import service_account
import google.auth.transport.requests

# Set GCS and BigQuery credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/keys/creds.json"

# Configurations
DATA_PATH = "/opt/airflow/data/state-wise-accidents.csv"
LOCAL_TRANSFORMED_PATH = "/opt/airflow/data/transformed/state_wise_road_accidents/"
BUCKET_NAME = "indian_road_accidents-sandbox-449108"
PROJECT_ID = "kestra-sandbox-449108"
DATASET = "indian_road_accidents"
TABLE_NAME = "state_wise_road_accidents"
DATAPROC_CLUSTER_NAME = "indian-road-accidents-cluster"
DATAPROC_REGION = "us-central1"

storage_client = storage.Client()
bq_client = bigquery.Client()

# Ensure local transformed folder exists
os.makedirs(LOCAL_TRANSFORMED_PATH, exist_ok=True)

def _get_access_token():
    """Generates an OAuth2 access token using the service account JSON."""
    credentials = service_account.Credentials.from_service_account_file(
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    request = google.auth.transport.requests.Request()
    credentials.refresh(request)
    return credentials.token

def check_missing_years(**kwargs):
    """Check which years in CSV are missing in BigQuery."""
    # Extract years from CSV
    df_sample = pd.read_csv(DATA_PATH, nrows=5)
    csv_years = {int(col) for col in df_sample.columns if str(col).isdigit()}
    
    # Get years from BigQuery
    query = f"SELECT DISTINCT year FROM `{PROJECT_ID}.{DATASET}.{TABLE_NAME}`"
    query_result = bq_client.query(query).result()
    bq_years = {row.year for row in query_result}
    
    # Find missing years
    print(f"Checking missing years. CSV Years: {csv_years}, BigQuery Years: {bq_years}")
    missing_years = sorted(csv_years - bq_years)

    if not missing_years:
        print("No missing years found. Stopping DAG.")
        return False  # Stop DAG execution
    
    print(f"Missing years found: {missing_years}")
    kwargs['ti'].xcom_push(key="missing_years", value=missing_years)
    return True  # Proceed with DAG execution

def transform_state_data(**kwargs):
    """Transform data for missing years."""
    ti = kwargs['ti']
    missing_years = ti.xcom_pull(task_ids='check_missing_years', key='missing_years')

    df = pd.read_csv(DATA_PATH, index_col=0)

    for year in missing_years:
        df_year = df[["States/UTs", str(year)]].rename(columns={
            str(year): "accident",
            "States/UTs": "state_ut"
        })
        
        df_year["year"] = year
        df_year = df_year[["year", "state_ut", "accident"]]

        file_path = os.path.join(LOCAL_TRANSFORMED_PATH, f"{year}.csv")
        df_year.to_csv(file_path, index=False)
        print(f"Transformed data saved: {file_path}")

def upload_state_data(**kwargs):
    """Upload transformed CSV files to GCS."""
    ti = kwargs['ti']
    missing_years = ti.xcom_pull(task_ids='check_missing_years', key='missing_years')

    for year in missing_years:
        file_name = f"{year}.csv"
        local_file = os.path.join(LOCAL_TRANSFORMED_PATH, file_name)

        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(f"state_wise_road_accidents/raw/{year}/{file_name}")
        blob.upload_from_filename(local_file, content_type="text/csv")

        print(f"Uploaded {file_name} to GCS under state_wise_road_accidents/raw/{year}/")

def trigger_spark_jobs(**kwargs):
    """Trigger Dataproc Spark job for each missing year."""
    ti = kwargs['ti']
    missing_years = ti.xcom_pull(task_ids='check_missing_years', key='missing_years')

    access_token = _get_access_token()
    headers = {"Authorization": f"Bearer {access_token}"}
    job_ids = []

    for year in missing_years:
        payload = {
            "projectId": PROJECT_ID,
            "job": {
                "placement": {
                    "clusterName": DATAPROC_CLUSTER_NAME
                },
                "pysparkJob": {
                    "mainPythonFileUri": f"gs://{BUCKET_NAME}/code/big-query-spark.py",
                    "args": [
                        f"--input-year={year}",
                        "--input-data-type=state"
                    ]
                }
            }
        }
        
        url = f"https://dataproc.googleapis.com/v1/projects/{PROJECT_ID}/regions/{DATAPROC_REGION}/jobs:submit"
        
        response = requests.post(url, json=payload, headers=headers)
        print(f"Triggered Spark job for {year}: {response.status_code}")
        if response.status_code == 200:
            job_id = response.json().get("reference", {}).get("jobId")
            job_ids.append(job_id)
            print(f"Triggered Spark job for {year}: Job ID = {job_id}")
        else:
            raise Exception(f"Failed to trigger Spark job for {year}: {response.text}")

        ti.xcom_push(key="spark_job_ids", value=job_ids)

def check_spark_jobs_status(**kwargs):
    """Poll Dataproc Spark job statuses and fail the DAG if any job fails."""
    ti = kwargs['ti']
    job_ids = ti.xcom_pull(task_ids='trigger_spark_jobs', key='spark_job_ids')

    if not job_ids:
        raise Exception("No Spark job IDs found. Cannot proceed with status check.")

    access_token = _get_access_token()
    headers = {"Authorization": f"Bearer {access_token}"}
    
    failed_jobs = []
    job_statuses = {}

    for job_id in job_ids:
        url = f"https://dataproc.googleapis.com/v1/projects/{PROJECT_ID}/regions/{DATAPROC_REGION}/jobs/{job_id}"
        
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            state = response.json().get("status", {}).get("state", "UNKNOWN")
            job_statuses[job_id] = state
            
            if state in ["ERROR", "CANCELLED", "FAILED"]:
                failed_jobs.append(job_id)
        
        else:
            raise Exception(f"Failed to fetch status for job {job_id}: {response.text}")

    print(f"Spark Job Statuses: {job_statuses}")

    if failed_jobs:
        raise Exception(f"Some Spark jobs failed: {failed_jobs}")

    print("All Spark jobs completed successfully!")

def cleanup_transformed_files():
    """Delete all transformed CSV files."""
    for file_name in os.listdir(LOCAL_TRANSFORMED_PATH):
        file_path = os.path.join(LOCAL_TRANSFORMED_PATH, file_name)
        os.remove(file_path)
        print(f"Deleted: {file_path}")

# Define DAG
with DAG(
    dag_id="state_accidents_dag",
    schedule_interval="@monthly",
    start_date=datetime(2025, 1, 1),
    catchup=False
) as dag:

    check_missing_years_task = ShortCircuitOperator(
        task_id="check_missing_years",
        python_callable=check_missing_years
    )

    transform_task = PythonOperator(
        task_id="transform_state_data",
        python_callable=transform_state_data
    )

    upload_task = PythonOperator(
        task_id="upload_state_data",
        python_callable=upload_state_data
    )

    trigger_spark_task = PythonOperator(
        task_id="trigger_spark_jobs",
        python_callable=trigger_spark_jobs
    )

    check_spark_status_task = PythonOperator(
        task_id="check_spark_jobs_status",
        python_callable=check_spark_jobs_status,
        retries=3,  # Retry polling in case of API failure
        retry_delay=timedelta(minutes=2)
    )

    cleanup_task = PythonOperator(
        task_id="cleanup_transformed_files",
        python_callable=cleanup_transformed_files
    )

    # DAG execution order
    check_missing_years_task >> transform_task >> upload_task
    upload_task >> trigger_spark_task >> check_spark_status_task
    upload_task >> cleanup_task
