from airflow import DAG
from airflow.operators.python import PythonOperator
from minio import Minio
from datetime import datetime, timedelta
from io import BytesIO
import os
import json
import numpy as np
from sentence_transformers import SentenceTransformer

# MinIO client setup
minio_client = Minio(
    "host.docker.internal:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

# Define bucket names
TEXT_DOCUMENT_BUCKET = "text-document"
EMBED_DOCUMENT_BUCKET = "embed-document"

# Ensure the embed-document bucket exists
if not minio_client.bucket_exists(EMBED_DOCUMENT_BUCKET):
    minio_client.make_bucket(EMBED_DOCUMENT_BUCKET)

# Embedding Utility Class
class EmbeddingUtils:
    def __init__(self, model_name='all-MiniLM-L6-v2'):
        self.model = SentenceTransformer(model_name)

    def generate_embedding(self, text):
        """Generate embeddings for a given text."""
        return self.model.encode(text)

    def save_embedding(self, embedding, bucket_name, object_name, minio_client):
        """Save embeddings as JSON to MinIO."""
        embedding_json = json.dumps(embedding.tolist())  # Convert numpy array to list
        minio_client.put_object(
            bucket_name, object_name,
            data=BytesIO(embedding_json.encode('utf-8')),
            length=len(embedding_json.encode('utf-8')),
            content_type='application/json'
        )

# Function to receive folder paths from the previous DAG run
def receive_folder_paths(**kwargs):
    """Retrieve the folder paths passed from the previous DAG."""
    # Accessing the 'conf' dictionary passed from the previous DAG run
    text_document_folder = kwargs['dag_run'].conf.get('text_document_folder')
    embed_document_folder = kwargs['dag_run'].conf.get('embed_document_folder')

    if not text_document_folder or not embed_document_folder:
        raise ValueError("Both 'text_document_folder' and 'embed_document_folder' must be provided.")

    print(f"Received folder paths: text_document_folder={text_document_folder}, embed_document_folder={embed_document_folder}")

    kwargs['ti'].xcom_push(key='text_document_folder', value=text_document_folder)
    kwargs['ti'].xcom_push(key='embed_document_folder', value=embed_document_folder)


def process_embeddings(**kwargs):
    """Fetch text files, generate embeddings, and store them in MinIO."""
    embedding_utils = EmbeddingUtils()
    ti = kwargs['ti']

    # Retrieve folder paths from XCom
    text_document_folder = ti.xcom_pull(task_ids='receive_folder_paths', key='text_document_folder')
    embed_document_folder = ti.xcom_pull(task_ids='receive_folder_paths', key='embed_document_folder')

    # Ensure paths are sanitized
    text_document_folder = text_document_folder.rstrip('/')
    embed_document_folder = embed_document_folder.rstrip('/')

    # Debugging logs to confirm path retrieval
    print(f"Processing text files from folder: {text_document_folder}")
    print(f"Embedding results will be stored in folder: {embed_document_folder}")

    # Construct the path to metadata.json based on the received embed_document_folder
    metadata_path = f"{embed_document_folder}/metadata.json".replace("//", "/")
    try:
        print(f"Attempting to retrieve metadata from: {metadata_path}")
        metadata_obj = minio_client.get_object(EMBED_DOCUMENT_BUCKET, metadata_path)
        metadata_content = json.loads(metadata_obj.read().decode('utf-8'))
        print(f"Successfully retrieved metadata: {metadata_content}")
    except Exception as e:
        print(f"Failed to retrieve metadata from {metadata_path}: {e}")
        return

    # List of uploaded files from the metadata
    text_documents = metadata_content.get("text_documents", [])
    embedded_documents = metadata_content.get("embedded_documents", [])

    # Debugging: Check if text_documents list is empty or not
    if not text_documents:
        print(f"No files found in metadata: {metadata_path}")
        return

    print(f"Found {len(text_documents)} files to process.")

    # Process each file in the text_documents list
    for file_path in text_documents:
        try:
            print(f"Processing file: {file_path}")

            # Retrieve the text content of the file from the TEXT_DOCUMENT_BUCKET
            try:
                print(f"Attempting to retrieve file from TEXT_DOCUMENT_BUCKET: {file_path}")
                text_obj = minio_client.get_object(TEXT_DOCUMENT_BUCKET, file_path)
                document_text = text_obj.read().decode('utf-8')
                print(f"Successfully retrieved content for file: {file_path}")
            except Exception as e:
                print(f"Failed to retrieve file {file_path} from {TEXT_DOCUMENT_BUCKET}: {e}")
                continue

            # Generate embedding
            print(f"Generating embedding for file: {file_path}")
            embedding = embedding_utils.generate_embedding(document_text)

            # Define embedding file path in embed_document_folder
            original_file_name = os.path.basename(file_path)  # Extracts just the filename
            embedding_object_name = f"{embed_document_folder}/embedded_{original_file_name}.json"
            print(f"Saving embedding to: {embedding_object_name}")

            # Save embedding to MinIO
            embedding_utils.save_embedding(embedding, EMBED_DOCUMENT_BUCKET, embedding_object_name, minio_client)
            print(f"Uploaded embedding for {file_path} → {EMBED_DOCUMENT_BUCKET}/{embedding_object_name}")

            # Add the file to the embedded_documents list
            embedded_documents.append(embedding_object_name)

        except Exception as e:
            print(f"Error processing embedding for {file_path}: {e}")

    # Update the metadata.json with the new embedded_documents list
    try:
        metadata_content['embedded_documents'] = embedded_documents
        metadata_json = json.dumps(metadata_content, indent=4)  # Properly formatted JSON

        # Save the updated metadata back to MinIO
        minio_client.put_object(
            EMBED_DOCUMENT_BUCKET, metadata_path,
            data=BytesIO(metadata_json.encode('utf-8')),
            length=len(metadata_json.encode('utf-8')),
            content_type='application/json'
        )
        print(f"Successfully updated metadata: {metadata_path}")
    except Exception as e:
        print(f"Failed to update metadata: {e}")


# DAG configuration
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

# Define the tasks
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

# Task execution order
receive_folder_paths_task >> process_embeddings_task
