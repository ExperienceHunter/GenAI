from datetime import datetime, timedelta
import json
import pytz
import os
from uuid import uuid4
from minio import Minio
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.models.param import Param

# Local timezone
LOCAL_TIMEZONE = "Asia/Tokyo"

def get_local_time():
    return datetime.now(pytz.timezone(LOCAL_TIMEZONE))

def get_minio_client():
    return Minio(
        "host.docker.internal:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False,
    )

# Generate default UUID once at DAG parse time
DEFAULT_PIPELINE_ID = str(uuid4())

def check_file_exists(**kwargs):
    dag_params = kwargs["params"]
    upload_path = dag_params.get("file_path", "/opt/airflow/uploads")

    if not os.path.exists(upload_path):
        raise FileNotFoundError(f"Error: Path {upload_path} does not exist!")

    kwargs['ti'].xcom_push(key='upload_path', value=upload_path)
    print(f"[CHECK] Path {upload_path} exists.")

def generate_metadata(**kwargs):
    ti = kwargs['ti']
    dag_params = kwargs["params"]
    upload_path = ti.xcom_pull(task_ids='check_file_exists', key='upload_path')
    bucket_name = dag_params.get("bucket_path", "document")
    pipeline_id = dag_params.get("pipeline_id", DEFAULT_PIPELINE_ID)

    now = get_local_time()
    today = now.strftime('%Y-%m-%d')
    run_timestamp = now.strftime('%Y-%m-%d_%H-%M-%S')
    base_folder = f"{today}/"
    run_folder = f"{base_folder}{run_timestamp}/"

    metadata = {
        "pipeline_id": pipeline_id,
        "bucket_name": bucket_name,
        "upload_path": upload_path,
        "dag_run_timestamp": run_timestamp,
        "run_folder": run_folder,
        "uploaded_files": []
    }

    for root, _, files in os.walk(upload_path):
        for file in files:
            full_path = os.path.join(root, file)
            rel_path = os.path.relpath(full_path, upload_path)
            metadata["uploaded_files"].append({
                "file_name": file,
                "object_name": f"{run_folder}{rel_path}",
                "source_path": full_path,
            })

    tmp_metadata_path = "/tmp/upload_metadata.json"
    with open(tmp_metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)

    ti.xcom_push(key='metadata', value=metadata)
    ti.xcom_push(key='run_folder', value=run_folder)
    ti.xcom_push(key='pipeline_id', value=pipeline_id)

    print(f"[META] Metadata generated with pipeline_id={pipeline_id} and {len(metadata['uploaded_files'])} files.")

def upload_files_to_minio(**kwargs):
    ti = kwargs['ti']
    metadata = ti.xcom_pull(task_ids='generate_metadata', key='metadata')
    client = get_minio_client()

    bucket_name = metadata["bucket_name"]
    run_folder = metadata["run_folder"]

    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    for file_info in metadata["uploaded_files"]:
        client.fput_object(bucket_name, file_info["object_name"], file_info["source_path"])
        print(f"[UPLOAD] {file_info['source_path']} → {bucket_name}/{file_info['object_name']}")

    tmp_metadata_path = "/tmp/upload_metadata.json"
    metadata_object = f"{run_folder}metadata.json"
    client.fput_object(bucket_name, metadata_object, tmp_metadata_path)
    print(f"[UPLOAD] metadata.json → {bucket_name}/{metadata_object}")

# DAG definition
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 15),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "upload_files_to_minio",
    default_args=default_args,
    description="Upload files to MinIO and trigger text extraction",
    schedule_interval=None,
    catchup=False,
    params={
        "file_path": Param("/opt/airflow/uploads", type="string", title="Local path to files"),
        "bucket_path": Param("document", type="string", title="MinIO bucket name"),
        "pipeline_id": Param(DEFAULT_PIPELINE_ID, type="string", title="Pipeline ID", description="Unique pipeline run ID"),
    },
)

check = PythonOperator(
    task_id="check_file_exists",
    python_callable=check_file_exists,
    provide_context=True,
    dag=dag,
)

meta = PythonOperator(
    task_id="generate_metadata",
    python_callable=generate_metadata,
    provide_context=True,
    dag=dag,
)

upload = PythonOperator(
    task_id="upload_files_to_minio",
    python_callable=upload_files_to_minio,
    provide_context=True,
    dag=dag,
)

trigger = TriggerDagRunOperator(
    task_id="trigger_text_extraction_dag",
    trigger_dag_id="process_documents_and_upload_text",
    conf={
        "run_folder": "{{ task_instance.xcom_pull(task_ids='generate_metadata', key='run_folder') }}",
        "pipeline_id": "{{ task_instance.xcom_pull(task_ids='generate_metadata', key='pipeline_id') }}"
    },
    dag=dag,
)

check >> meta >> upload >> trigger
