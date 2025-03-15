from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from minio import Minio
from minio.error import S3Error

# Define the Python function that will be called by the DAG
def list_files_in_bucket(**kwargs):
    # Retrieve the MinIO bucket path (parameter passed during the DAG trigger)
    bucket_path = kwargs['dag_run'].conf.get('bucket_path', 'No bucket path provided')

    # Set up MinIO client - use your MinIO credentials
    client = Minio(
        "host.docker.internal:9000",  # MinIO endpoint
        access_key="minioadmin",  # Your MinIO access key
        secret_key="minioadmin",  # Your MinIO secret key
        secure=False,  # Set to False since you're using HTTP
    )

    try:
        # List the files in the specified bucket
        files = client.list_objects(bucket_path)

        # Print the files in the bucket
        print(f"Files in bucket '{bucket_path}':")
        for file in files:
            print(file.object_name)
    except S3Error as e:
        print(f"Error listing files in bucket {bucket_path}: {e}")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'list_minio_files_dag',
    default_args=default_args,
    description='A DAG that lists files in a MinIO bucket',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    params={
        # Define the bucket path parameter for the UI
        "bucket_path": Param(
            "document",  # Default bucket path (can be any of your provided bucket names)
            type="string",  # Type: string
            title="MinIO Bucket Path",
            description="Enter the MinIO bucket path to list files (e.g., document, embed-document, text-document)",
        ),
    },
)

# Define the task using PythonOperator
list_files_task = PythonOperator(
    task_id='list_files_in_bucket',
    python_callable=list_files_in_bucket,
    provide_context=True,  # To access kwargs and DAG run parameters
    dag=dag,
)

# Set the task execution order
list_files_task
