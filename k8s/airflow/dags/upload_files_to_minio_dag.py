from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from minio import Minio
from minio.error import S3Error
import os

def upload_to_minio(**kwargs):
    dag_run_conf = kwargs.get('dag_run', None)

    if dag_run_conf:
        bucket_name = dag_run_conf.conf.get('bucket_path', 'default-bucket')
        upload_path = dag_run_conf.conf.get('file_path', '/path/to/default/')
    else:
        bucket_name = 'default-bucket'
        upload_path = '/opt/airflow/uploads'

    # Ensure path exists
    if not os.path.exists(upload_path):
        print(f"Error: Path {upload_path} does not exist!")
        return

    # MinIO client setup
    client = Minio(
        "host.docker.internal:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False,
    )

    # Generate folder names
    today = datetime.now().strftime('%Y-%m-%d')  # e.g., "2025-03-15"
    run_timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')  # e.g., "2025-03-15_14-30-10"
    base_folder = f"{today}/"
    run_folder = f"{base_folder}{run_timestamp}/"

    # Ensure MinIO bucket exists
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    # Create an empty ".keep" file to simulate a directory
    keep_file = "/tmp/.keep"
    with open(keep_file, "wb") as f:
        f.write(b"")

    # Upload the ".keep" file to create the folder structure
    client.fput_object(bucket_name, f"{run_folder}.keep", keep_file)

    # If uploading a single file
    if os.path.isfile(upload_path):
        file_name = os.path.basename(upload_path)
        minio_path = f"{run_folder}{file_name}"
        client.fput_object(bucket_name, minio_path, upload_path)
        print(f"Uploaded: {upload_path} → {bucket_name}/{minio_path}")
    else:
        # If uploading a folder, recursively upload all files inside
        for root, _, files in os.walk(upload_path):
            for file in files:
                file_path = os.path.join(root, file)
                relative_path = os.path.relpath(file_path, upload_path)
                minio_path = f"{run_folder}{relative_path}"
                client.fput_object(bucket_name, minio_path, file_path)
                print(f"Uploaded: {file_path} → {bucket_name}/{minio_path}")

# DAG configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'upload_files_to_minio_dag',
    default_args=default_args,
    description='Uploads files/folders to MinIO with a structured date-time folder',
    schedule_interval=None,
    catchup=False,
    params={
        "bucket_path": Param("document", type="string", title="MinIO Bucket Name"),
        "file_path": Param("/opt/airflow/uploads", type="string", title="Local Path"),
    },
)

upload_task = PythonOperator(
    task_id='upload_to_minio',
    python_callable=upload_to_minio,
    dag=dag,
)

upload_task
