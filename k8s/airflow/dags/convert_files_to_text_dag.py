# process_documents_and_upload_text.py

import sys
import os
import json
import logging
from datetime import datetime, timedelta
from io import BytesIO

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.minio_utils import get_minio_client, TEXT_DOCUMENT_BUCKET
from utils.document_utils import extract_text_from_stream

logger = logging.getLogger(__name__)
METADATA_FILE_NAME = "metadata.json"

def receive_folder_paths(**kwargs):
    conf = kwargs['dag_run'].conf or {}
    base_folder = conf.get("run_folder")
    if not base_folder:
        raise ValueError("run_folder must be provided in conf")

    document_folder = base_folder
    text_document_folder = base_folder.replace("document", "text", 1)

    kwargs['ti'].xcom_push(key='document_folder', value=document_folder)
    kwargs['ti'].xcom_push(key='text_document_folder', value=text_document_folder)

def process_documents(**kwargs):
    client = get_minio_client()
    ti = kwargs['ti']
    document_folder = ti.xcom_pull(task_ids='receive_folder_paths', key='document_folder')
    text_document_folder = ti.xcom_pull(task_ids='receive_folder_paths', key='text_document_folder')

    metadata_path = os.path.join(document_folder, METADATA_FILE_NAME)
    tmp_metadata_path = "/tmp/convert_metadata.json"

    logger.info(f"[INFO] Downloading metadata.json from: {metadata_path}")
    client.fget_object("document", metadata_path, tmp_metadata_path)

    with open(tmp_metadata_path, 'r') as f:
        metadata = json.load(f)

    uploaded_files = metadata.get("uploaded_files", [])
    pipeline_id = metadata.get("pipeline_id")  # ✅ Extract pipeline_id if exists

    logger.info(f"[INFO] Found {len(uploaded_files)} files in metadata.")
    logger.info(f"[INFO] Pipeline ID: {pipeline_id}")

    for file in uploaded_files:
        object_name = file["object_name"]
        file_name = file["file_name"]
        logger.info(f"[INFO] Attempting to extract: {file_name} ({object_name})")

        try:
            obj = client.get_object("document", object_name)
            stream_bytes = obj.read()
            stream = BytesIO(stream_bytes)
            logger.info(f"[DEBUG] Stream size for {file_name}: {len(stream_bytes)} bytes")

            text = extract_text_from_stream(stream, file_name) or ""
            logger.info(f"[DEBUG] Extracted text length: {len(text)}")

            extracted_file_name = f"text_{os.path.splitext(file_name)[0]}.txt"
            local_text_path = f"/tmp/{extracted_file_name}"
            with open(local_text_path, "w", encoding="utf-8") as f_out:
                f_out.write(text)

            object_path = os.path.join(text_document_folder, extracted_file_name)
            client.fput_object(TEXT_DOCUMENT_BUCKET, object_path, local_text_path)
            file["extracted_text_file"] = object_path

        except Exception as e:
            logger.warning(f"[WARN] Failed to extract text from {file_name}: {str(e)}")
            file["extracted_text_file"] = None

    # ✅ Include pipeline_id in updated metadata
    new_metadata = {
        "pipeline_id": pipeline_id,
        "text_documents": uploaded_files
    }

    updated_path = "/tmp/converted_text_metadata.json"
    with open(updated_path, "w") as f:
        json.dump(new_metadata, f, indent=2)

    text_metadata_path = os.path.join(text_document_folder, METADATA_FILE_NAME)
    logger.info(f"[INFO] Uploading new metadata to {text_metadata_path} in text-document")
    client.fput_object(TEXT_DOCUMENT_BUCKET, text_metadata_path, updated_path)

# Airflow boilerplate
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 16),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'process_documents_and_upload_text',
    default_args=default_args,
    description='Extract text from uploaded documents and save as separate files in MinIO',
    schedule_interval=None,
    catchup=False,
)

receive_task = PythonOperator(
    task_id='receive_folder_paths',
    python_callable=receive_folder_paths,
    provide_context=True,
    dag=dag,
)

process_task = PythonOperator(
    task_id='process_documents',
    python_callable=process_documents,
    provide_context=True,
    dag=dag,
)

trigger_task = TriggerDagRunOperator(
    task_id='trigger_embedding_dag',
    trigger_dag_id='generate_embeddings',
    conf={
        "text_document_folder": "{{ task_instance.xcom_pull(task_ids='receive_folder_paths', key='text_document_folder') }}",
        "embed_document_folder": "{{ task_instance.xcom_pull(task_ids='receive_folder_paths', key='text_document_folder').replace('text', 'embed') }}"
    },
    dag=dag,
)

receive_task >> process_task >> trigger_task
