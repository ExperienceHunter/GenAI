from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from minio import Minio
from io import BytesIO
import os
import json
import pdfplumber
from datetime import datetime, timedelta
import logging

# Constants
MINIO_ENDPOINT = "host.docker.internal:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
SECURE = False
DOCUMENT_BUCKET = "document"
TEXT_DOCUMENT_BUCKET = "text-document"
EMBED_DOCUMENT_BUCKET = "embed-document"
METADATA_FILE_NAME = "metadata.json"

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize MinIO client
def get_minio_client():
    logger.info("Initializing MinIO client...")
    return Minio(MINIO_ENDPOINT, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=SECURE)

# Ensure embed-document bucket exists
minio_client = get_minio_client()
if not minio_client.bucket_exists(EMBED_DOCUMENT_BUCKET):
    logger.info(f"Bucket {EMBED_DOCUMENT_BUCKET} does not exist, creating it.")
    minio_client.make_bucket(EMBED_DOCUMENT_BUCKET)
else:
    logger.info(f"Bucket {EMBED_DOCUMENT_BUCKET} already exists.")

def extract_text_from_stream(file_stream, file_path):
    """Extracts text from PDF or plain text files."""
    logger.info(f"Extracting text from file: {file_path}")
    file_stream.seek(0)
    if file_path.lower().endswith('.pdf'):
        with pdfplumber.open(file_stream) as pdf:
            extracted_text = "\n".join([page.extract_text() for page in pdf.pages if page.extract_text()])
            logger.info(f"Text extracted from PDF file: {file_path}")
            return extracted_text
    extracted_text = file_stream.read().decode('utf-8', errors='ignore')
    logger.info(f"Text extracted from plain text file: {file_path}")
    return extracted_text

def receive_folder_paths(**kwargs):
    """Retrieves folder paths from the triggering DAG."""
    logger.info("Receiving folder paths from the triggering DAG...")
    conf = kwargs['dag_run'].conf or {}
    document_folder = conf.get('document_folder')
    text_document_folder = conf.get('text_document_folder')
    if not document_folder or not text_document_folder:
        logger.error("Both 'document_folder' and 'text_document_folder' must be provided.")
        raise ValueError("Both 'document_folder' and 'text_document_folder' must be provided.")
    logger.info(f"Received folders: document_folder={document_folder}, text_document_folder={text_document_folder}")
    kwargs['ti'].xcom_push(key='text_document_folder', value=text_document_folder)

def process_documents(**kwargs):
    """Processes uploaded documents and stores extracted text in MinIO."""
    logger.info("Starting document processing...")
    client = get_minio_client()
    ti = kwargs['ti']
    text_document_folder = ti.xcom_pull(task_ids='receive_folder_paths', key='text_document_folder')
    if not text_document_folder:
        logger.error("text_document_folder not found in XCom.")
        raise ValueError("text_document_folder not found in XCom.")

    metadata_file_path = os.path.join(text_document_folder, METADATA_FILE_NAME)
    local_metadata_path = "/tmp/upload_metadata.json"

    try:
        logger.info(f"Fetching metadata file from MinIO: {metadata_file_path}")
        client.fget_object(TEXT_DOCUMENT_BUCKET, metadata_file_path, local_metadata_path)
        with open(local_metadata_path, 'r') as f:
            metadata = json.load(f)
        uploaded_files = metadata.get('uploaded_files', [])
        logger.info(f"Found {len(uploaded_files)} files to process.")
    except Exception as e:
        logger.error(f"Error fetching metadata: {e}")
        return

    text_documents = []
    for file_name in uploaded_files:
        try:
            logger.info(f"Processing file: {file_name}")
            file_stream = client.get_object(DOCUMENT_BUCKET, file_name)
            extracted_text = extract_text_from_stream(BytesIO(file_stream.read()), file_name)
            text_file_minio_path = os.path.join(text_document_folder, f"text_{os.path.basename(file_name)}.txt")
            logger.info(f"Uploading extracted text for {file_name} to {text_file_minio_path}")
            client.put_object(TEXT_DOCUMENT_BUCKET, text_file_minio_path, BytesIO(extracted_text.encode('utf-8')),
                              length=len(extracted_text.encode('utf-8')))
            text_documents.append(text_file_minio_path)
        except Exception as e:
            logger.error(f"Error processing {file_name}: {e}")

    metadata["text_documents"] = text_documents
    try:
        with open(local_metadata_path, 'w') as f:
            json.dump(metadata, f, indent=4)
        logger.info(f"Uploading updated metadata to {metadata_file_path}")
        client.fput_object(TEXT_DOCUMENT_BUCKET, metadata_file_path, local_metadata_path)
    except Exception as e:
        logger.error(f"Error uploading metadata: {e}")

    ti.xcom_push(key='metadata', value=metadata)
    logger.info("Document processing completed.")

def copy_metadata_to_embed_document_bucket(**kwargs):
    """Copies metadata from 'text-document' to 'embed-document'."""
    logger.info("Copying metadata to embed-document bucket...")
    client = get_minio_client()
    ti = kwargs['ti']
    text_document_folder = ti.xcom_pull(task_ids='receive_folder_paths', key='text_document_folder')
    if not text_document_folder:
        logger.error("text_document_folder not found in XCom.")
        raise ValueError("text_document_folder not found in XCom.")

    metadata_file_path = os.path.join(text_document_folder, METADATA_FILE_NAME)
    local_metadata_path = "/tmp/upload_metadata.json"
    try:
        logger.info(f"Fetching metadata file from {TEXT_DOCUMENT_BUCKET} to {local_metadata_path}")
        client.fget_object(TEXT_DOCUMENT_BUCKET, metadata_file_path, local_metadata_path)
        with open(local_metadata_path, 'r') as f:
            metadata = json.load(f)
        metadata["bucket_name"] = EMBED_DOCUMENT_BUCKET
        with open(local_metadata_path, 'w') as f:
            json.dump(metadata, f, indent=4)
        logger.info(f"Uploading metadata to {EMBED_DOCUMENT_BUCKET}")
        client.fput_object(EMBED_DOCUMENT_BUCKET, metadata_file_path, local_metadata_path)
    except Exception as e:
        logger.error(f"Failed to copy metadata: {e}")

# DAG Configuration
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
    description='Process documents from MinIO and upload extracted text',
    schedule_interval=None,
    catchup=False,
)

# Tasks
receive_folder_paths_task = PythonOperator(
    task_id='receive_folder_paths',
    python_callable=receive_folder_paths,
    provide_context=True,
    dag=dag,
)

process_documents_task = PythonOperator(
    task_id='process_documents',
    python_callable=process_documents,
    provide_context=True,
    dag=dag,
)

copy_metadata_task = PythonOperator(
    task_id='copy_metadata_to_embed_document',
    python_callable=copy_metadata_to_embed_document_bucket,
    provide_context=True,
    dag=dag,
)

trigger_embed_documents_dag_task = TriggerDagRunOperator(
    task_id='trigger_embed_documents_dag',
    trigger_dag_id='generate_embeddings',
    conf={
        "text_document_folder": "{{ ti.xcom_pull(task_ids='receive_folder_paths', key='text_document_folder') }}",
        "embed_document_folder": "{{ ti.xcom_pull(task_ids='receive_folder_paths', key='text_document_folder') | replace('text-document', 'embed-document') }}"
    },
    dag=dag,
)

# Task Dependencies
receive_folder_paths_task >> process_documents_task >> copy_metadata_task >> trigger_embed_documents_dag_task
