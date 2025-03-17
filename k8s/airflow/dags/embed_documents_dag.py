from airflow import DAG
from airflow.operators.python import PythonOperator
from minio import Minio
from datetime import datetime, timedelta
from io import BytesIO
import os
import json
import numpy as np
from sentence_transformers import SentenceTransformer

# MinIO Configuration
MINIO_CONFIG = {
    "endpoint": "host.docker.internal:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
    "secure": False
}

# Bucket Names
TEXT_DOCUMENT_BUCKET = "text-document"
EMBED_DOCUMENT_BUCKET = "embed-document"

# Initialize MinIO Client
minio_client = Minio(**MINIO_CONFIG)

# Ensure embed-document bucket exists
if not minio_client.bucket_exists(EMBED_DOCUMENT_BUCKET):
    minio_client.make_bucket(EMBED_DOCUMENT_BUCKET)


class EmbeddingUtils:
    def __init__(self, model_name='all-MiniLM-L6-v2'):
        self.model = SentenceTransformer(model_name)

    def generate_embedding(self, text):
        return self.model.encode(text)

    def save_embedding(self, embedding, bucket, object_name):
        embedding_json = json.dumps(embedding.tolist())
        minio_client.put_object(
            bucket, object_name,
            data=BytesIO(embedding_json.encode('utf-8')),
            length=len(embedding_json.encode('utf-8')),
            content_type='application/json'
        )


def receive_folder_paths(**kwargs):
    conf = kwargs['dag_run'].conf or {}
    text_folder = conf.get('text_document_folder')
    embed_folder = conf.get('embed_document_folder')

    if not text_folder or not embed_folder:
        raise ValueError("Both 'text_document_folder' and 'embed_document_folder' must be provided.")

    print(f"Received: text_folder={text_folder}, embed_folder={embed_folder}")
    kwargs['ti'].xcom_push(key='text_folder', value=text_folder)
    kwargs['ti'].xcom_push(key='embed_folder', value=embed_folder)


def process_embeddings(**kwargs):
    embedding_utils = EmbeddingUtils()
    ti = kwargs['ti']
    text_folder = ti.xcom_pull(task_ids='receive_folder_paths', key='text_folder').rstrip('/')
    embed_folder = ti.xcom_pull(task_ids='receive_folder_paths', key='embed_folder').rstrip('/')
    metadata_path = f"{embed_folder}/metadata.json".replace("//", "/")

    try:
        metadata_obj = minio_client.get_object(EMBED_DOCUMENT_BUCKET, metadata_path)
        metadata = json.loads(metadata_obj.read().decode('utf-8'))
    except Exception as e:
        print(f"Failed to retrieve metadata: {e}")
        return

    text_files = metadata.get("text_documents", [])
    embedded_files = metadata.get("embedded_documents", [])

    if not text_files:
        print("No files to process.")
        return

    for file_path in text_files:
        try:
            text_obj = minio_client.get_object(TEXT_DOCUMENT_BUCKET, file_path)
            document_text = text_obj.read().decode('utf-8')
            embedding = embedding_utils.generate_embedding(document_text)
            file_name = os.path.basename(file_path)
            embed_object = f"{embed_folder}/embedded_{file_name}.json"
            embedding_utils.save_embedding(embedding, EMBED_DOCUMENT_BUCKET, embed_object)
            embedded_files.append(embed_object)
        except Exception as e:
            print(f"Error processing {file_path}: {e}")

    metadata['embedded_documents'] = embedded_files
    try:
        metadata_json = json.dumps(metadata, indent=4)
        minio_client.put_object(
            EMBED_DOCUMENT_BUCKET, metadata_path,
            data=BytesIO(metadata_json.encode('utf-8')),
            length=len(metadata_json.encode('utf-8')),
            content_type='application/json'
        )
    except Exception as e:
        print(f"Failed to update metadata: {e}")


# DAG Configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 16),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'generate_embeddings',
    default_args=default_args,
    description='Generate embeddings for processed documents and store in MinIO',
    schedule_interval=None,
    catchup=False,
)

# Define Tasks
receive_folder_paths_task = PythonOperator(
    task_id='receive_folder_paths',
    python_callable=receive_folder_paths,
    provide_context=True,
    dag=dag,
)

process_embeddings_task = PythonOperator(
    task_id='process_embeddings',
    python_callable=process_embeddings,
    provide_context=True,
    dag=dag,
)

# Task Dependencies
receive_folder_paths_task >> process_embeddings_task
