from datetime import datetime, timedelta
import json
import pytz
import os
from minio import Minio
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.models.param import Param

# Define your local timezone
LOCAL_TIMEZONE = "Asia/Tokyo"

def get_local_time():
    """ Return current time in the specified local timezone. """
    return datetime.now(pytz.timezone(LOCAL_TIMEZONE))


# MinIO client setup function
def get_minio_client():
    """ Setup and return a MinIO client. """
    return Minio(
        "host.docker.internal:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False,
    )


def check_file_exists(**kwargs):
    """ Check if the file/folder exists before proceeding. """
    dag_run_conf = kwargs.get('dag_run', None)
    upload_path = dag_run_conf.conf.get('file_path', '/opt/airflow/uploads') if dag_run_conf else '/opt/airflow/uploads'

    if not os.path.exists(upload_path):
        raise FileNotFoundError(f"Error: Path {upload_path} does not exist!")

    kwargs['ti'].xcom_push(key='upload_path', value=upload_path)
    print(f"Path {upload_path} exists, proceeding...")


def generate_metadata(**kwargs):
    """ Generate metadata for upload. """
    ti = kwargs['ti']
    upload_path = ti.xcom_pull(task_ids='check_file_exists', key='upload_path')
    dag_run_conf = kwargs.get('dag_run', None)
    bucket_name = dag_run_conf.conf.get('bucket_path', 'default-bucket') if dag_run_conf else 'default-bucket'

    now = get_local_time()
    today = now.strftime('%Y-%m-%d')
    run_timestamp = now.strftime('%Y-%m-%d_%H-%M-%S')
    base_folder = f"{today}/"
    run_folder = f"{base_folder}{run_timestamp}/"

    metadata = {
        "bucket_name": bucket_name,
        "upload_path": upload_path,
        "dag_run_timestamp": run_timestamp,
        "run_folder": run_folder,
        "uploaded_files": []
    }
    ti.xcom_push(key='metadata', value=metadata)
    print(f"Generated metadata: {metadata}")


def upload_to_minio(**kwargs):
    """ Upload files and metadata to MinIO. """
    ti = kwargs['ti']
    metadata = ti.xcom_pull(task_ids='generate_metadata', key='metadata')

    bucket_name = metadata["bucket_name"]
    upload_path = metadata["upload_path"]
    run_folder = metadata["run_folder"]

    client = get_minio_client()

    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    # Upload placeholder file to simulate directory structure
    keep_file = "/tmp/.keep"
    with open(keep_file, "wb") as f:
        f.write(b"")

    client.fput_object(bucket_name, f"{run_folder}.keep", keep_file)

    uploaded_files = []
    if os.path.isfile(upload_path):
        file_name = os.path.basename(upload_path)
        minio_path = f"{run_folder}{file_name}"
        client.fput_object(bucket_name, minio_path, upload_path)
        uploaded_files.append(minio_path)
        print(f"Uploaded: {upload_path} → {bucket_name}/{minio_path}")
    else:
        for root, _, files in os.walk(upload_path):
            for file in files:
                file_path = os.path.join(root, file)
                relative_path = os.path.relpath(file_path, upload_path)
                minio_path = f"{run_folder}{relative_path}"
                client.fput_object(bucket_name, minio_path, file_path)
                uploaded_files.append(minio_path)
                print(f"Uploaded: {file_path} → {bucket_name}/{minio_path}")

    metadata["uploaded_files"] = uploaded_files
    metadata_file_path = "/tmp/upload_metadata.json"
    with open(metadata_file_path, "w") as metadata_file:
        json.dump(metadata, metadata_file, indent=4)

    metadata_minio_path = f"{run_folder}metadata.json"
    client.fput_object(bucket_name, metadata_minio_path, metadata_file_path)
    print(f"Metadata uploaded: {metadata_file_path} → {bucket_name}/{metadata_minio_path}")

    ti.xcom_push(key='metadata', value=metadata)


def copy_metadata_to_text_document_bucket(**kwargs):
    """ Copy metadata to the 'text-document' bucket. """
    ti = kwargs['ti']
    metadata = ti.xcom_pull(task_ids='upload_to_minio', key='metadata')

    print(f"Original metadata: {metadata}")

    client = get_minio_client()
    target_bucket_name = "text-document"
    metadata["bucket_name"] = target_bucket_name

    print(f"Modified metadata: {metadata}")

    if not client.bucket_exists(target_bucket_name):
        client.make_bucket(target_bucket_name)

    # Create placeholder file to simulate folder structure
    keep_file = "/tmp/.keep"
    with open(keep_file, "wb") as f:
        f.write(b"")

    client.fput_object(target_bucket_name, f"{metadata['run_folder']}.keep", keep_file)

    metadata_file_path = "/tmp/upload_metadata.json"
    with open(metadata_file_path, 'w') as f:
        json.dump(metadata, f, indent=4)

    target_metadata_path = f"{metadata['run_folder'].rstrip('/')}/metadata.json"
    client.fput_object(target_bucket_name, target_metadata_path, metadata_file_path)

    print(f"Metadata copied to: {target_bucket_name}/{target_metadata_path}")


def trigger_second_dag(**kwargs):
    """Trigger the second DAG after file upload. """
    dag_run_conf = kwargs.get('dag_run', None)
    run_id = dag_run_conf.run_id if dag_run_conf else 'default_run_id'

    metadata = kwargs['ti'].xcom_pull(task_ids='generate_metadata', key='metadata')
    run_folder = metadata.get("run_folder", None)

    if not run_folder:
        raise ValueError("run_folder not found in metadata.")

    conf = {
        "document_folder": run_folder,
        "text_document_folder": run_folder,
    }

    kwargs['ti'].xcom_push(key='run_id', value=run_id)

    return {
        'dag_id': 'process_documents_and_upload_text',
        'conf': conf,
    }


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
    description='Uploads files to MinIO with structured folders and metadata',
    schedule_interval=None,
    catchup=False,
    params={
        "bucket_path": Param("document", type="string", title="MinIO Bucket Name"),
        "file_path": Param("/opt/airflow/uploads", type="string", title="Local Path"),
    },
)

# Task definitions
check_task = PythonOperator(task_id='check_file_exists', python_callable=check_file_exists, dag=dag)
metadata_task = PythonOperator(task_id='generate_metadata', python_callable=generate_metadata, dag=dag)
upload_task = PythonOperator(task_id='upload_to_minio', python_callable=upload_to_minio, dag=dag)
copy_metadata_task = PythonOperator(task_id='copy_metadata_to_text_document', python_callable=copy_metadata_to_text_document_bucket, dag=dag)
trigger_second_dag_task = TriggerDagRunOperator(
    task_id='trigger_second_dag',
    trigger_dag_id='process_documents_and_upload_text',
    conf={
        "document_folder": "{{ ti.xcom_pull(task_ids='generate_metadata', key='metadata')['run_folder'] }}",
        "text_document_folder": "{{ ti.xcom_pull(task_ids='generate_metadata', key='metadata')['run_folder'] }}",
    },
    dag=dag,
)

# Task dependencies
check_task >> metadata_task >> upload_task >> copy_metadata_task >> trigger_second_dag_task
