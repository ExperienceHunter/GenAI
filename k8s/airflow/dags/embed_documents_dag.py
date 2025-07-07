import os
import sys
import json
import logging
from datetime import datetime, timedelta

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.minio_utils import (
    get_minio_client,
    load_metadata,
    save_metadata,
    get_text_from_minio
)
from utils.embedding_utils import EmbeddingUtils
from utils.elasticsearch_utils import get_vectorstore
from langchain.schema import Document

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

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
    description='Generate embeddings, upload each file into embed-document bucket, and store in Elasticsearch',
    schedule_interval=None,
    catchup=False,
)

def receive_folder_paths(**kwargs):
    conf = kwargs['dag_run'].conf or {}
    text_folder = conf.get('text_document_folder')
    embed_folder = conf.get('embed_document_folder')

    if not text_folder or not embed_folder:
        raise ValueError("Both 'text_document_folder' and 'embed_document_folder' must be provided.")

    logger.info(f"[INFO] DAG triggered with:")
    logger.info(f"       ├── text_folder: {text_folder}")
    logger.info(f"       └── embed_folder: {embed_folder}")

    kwargs['ti'].xcom_push(key='text_folder', value=text_folder)
    kwargs['ti'].xcom_push(key='embed_folder', value=embed_folder)

def embed_and_store_documents(**kwargs):
    ti = kwargs['ti']
    text_folder = ti.xcom_pull(task_ids='receive_folder_paths', key='text_folder').rstrip('/')
    embed_folder = ti.xcom_pull(task_ids='receive_folder_paths', key='embed_folder').rstrip('/')
    text_metadata_path = f"{text_folder}/metadata.json"
    embed_metadata_path = f"{embed_folder}/metadata.json"

    logger.info(f"[STEP 1] Initializing MinIO and embedding clients...")
    client = get_minio_client()
    embedding_utils = EmbeddingUtils()
    vectorstore = get_vectorstore()

    # ✅ Step: Copy metadata from text-document to embed-document
    logger.info(f"[STEP 2] Copying metadata from text-document to embed-document...")
    tmp_metadata = "/tmp/tmp_text_metadata.json"
    client.fget_object("text-document", text_metadata_path, tmp_metadata)
    client.fput_object("embed-document", embed_metadata_path, tmp_metadata)

    logger.info(f"[STEP 3] Reading metadata from embed-document: {embed_metadata_path}")
    metadata = load_metadata(client, embed_metadata_path)
    logger.info(f"[STEP 4] Metadata content:")
    logger.info(json.dumps(metadata, indent=4))

    text_files = metadata.get("text_documents", [])
    embedded_files = []

    logger.info(f"[STEP 5] Found {len(text_files)} text files to embed.")

    for doc in text_files:
        file_name = doc.get("file_name") or os.path.basename(doc.get("object_name", ""))
        object_name = doc.get("extracted_text_file")

        if not object_name:
            logger.warning(f"[SKIP] No extracted_text_file found for {file_name}")
            continue

        try:
            logger.info(f"[STEP 6] Fetching content from MinIO: {object_name}")
            text = get_text_from_minio(client, object_name)
            logger.info(f"[INFO] Retrieved text length: {len(text)} characters")

            logger.info(f"[STEP 7] Generating embedding for {file_name}")
            embedding = embedding_utils.get_embedding(text)
            logger.info(f"[INFO] Embedding vector length: {len(embedding)}")

            embedded_filename = f"embedded_{file_name}.json"
            local_path = f"/tmp/{embedded_filename}"
            with open(local_path, "w") as f:
                json.dump({
                    "file_name": file_name,
                    "embedding": embedding
                }, f)

            embedded_object_path = f"{embed_folder}/{embedded_filename}"
            logger.info(f"[STEP 8] Uploading embedding to embed-document: {embedded_object_path}")
            client.fput_object("embed-document", embedded_object_path, local_path)

            logger.info(f"[STEP 9] Inserting {file_name} into vectorstore")
            pipeline_id = metadata.get("pipeline_id", "unknown")
            vectorstore.add_documents([
                Document(
                    page_content=text,
                    metadata={
                        "source": file_name,
                        "pipeline_id": pipeline_id  # ✅ Include pipeline_id
                    }
                )
            ])

            embedded_files.append({
                "file_name": file_name,
                "embedding_file": embedded_object_path,
                "embedding_model": "sentence-transformers/all-MiniLM-L6-v2"
            })

        except Exception as e:
            logger.warning(f"[WARN] Failed to embed {file_name}: {str(e)}")

    # ✅ Final metadata save
    metadata["embedded_documents"] = embedded_files
    logger.info(f"[STEP 10] Saving updated metadata to: {embed_metadata_path}")
    save_metadata(client, embed_metadata_path, metadata)

# DAG tasks
receive_task = PythonOperator(
    task_id='receive_folder_paths',
    python_callable=receive_folder_paths,
    provide_context=True,
    dag=dag,
)

embed_task = PythonOperator(
    task_id='embed_and_store_documents',
    python_callable=embed_and_store_documents,
    provide_context=True,
    dag=dag,
)

receive_task >> embed_task
