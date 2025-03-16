from airflow import DAG
from airflow.operators.python import PythonOperator
from minio import Minio
from io import BytesIO
import os
import json
import pdfplumber
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator

# MinIO client setup with direct values
minio_client = Minio(
    "host.docker.internal:9000",  # MinIO endpoint
    access_key="minioadmin",  # MinIO access key
    secret_key="minioadmin",  # MinIO secret key
    secure=False  # Whether to use SSL
)

# Define bucket names
DOCUMENT_BUCKET = "document"
TEXT_DOCUMENT_BUCKET = "text-document"
EMBED_DOCUMENT_BUCKET = "embed-document"

# Ensure the embed-document bucket exists
if not minio_client.bucket_exists(EMBED_DOCUMENT_BUCKET):
    minio_client.make_bucket(EMBED_DOCUMENT_BUCKET)


# Function to extract text from PDF or plain text
def extract_text_from_stream(file_stream, file_path):
    """Extract text from PDF or plain text files."""
    text = ""
    file_stream.seek(0)  # Reset stream position to the beginning

    if file_path.lower().endswith('.pdf'):
        with pdfplumber.open(file_stream) as pdf:
            text = "\n".join([page.extract_text() for page in pdf.pages if page.extract_text()])
    else:
        # For non-PDF files, assume it's a text file
        text = file_stream.read().decode('utf-8', errors='ignore')
    return text


def receive_folder_paths(**kwargs):
    """Retrieve the folder paths passed from the first DAG."""
    # Accessing the 'conf' dictionary passed from the first DAG
    document_folder = kwargs['dag_run'].conf.get('document_folder')
    text_document_folder = kwargs['dag_run'].conf.get('text_document_folder')

    if not document_folder or not text_document_folder:
        raise ValueError("Both 'document_folder' and 'text_document_folder' must be provided.")

    print(f"Received folder paths: document_folder={document_folder}, text_document_folder={text_document_folder}")

    kwargs['ti'].xcom_push(key='document_folder', value=document_folder)
    kwargs['ti'].xcom_push(key='text_document_folder', value=text_document_folder)


def process_documents(**kwargs):
    """Retrieve metadata, process documents listed in the metadata, and upload extracted text to MinIO."""
    ti = kwargs['ti']

    # Retrieve the folder paths from XCom
    text_document_folder = ti.xcom_pull(task_ids='receive_folder_paths', key='text_document_folder')

    if not text_document_folder:
        raise ValueError("text_document_folder not found in XCom.")

    # MinIO client setup
    client = Minio(
        "host.docker.internal:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False,
    )

    # Define the 'document' and 'text-document' buckets
    document_bucket_name = "document"
    text_document_bucket_name = "text-document"

    # Ensure the 'document' and 'text-document' buckets exist
    if not client.bucket_exists(document_bucket_name):
        raise ValueError(f"Bucket {document_bucket_name} does not exist.")
    if not client.bucket_exists(text_document_bucket_name):
        raise ValueError(f"Bucket {text_document_bucket_name} does not exist.")

    # Strip out any extra leading slashes and ensure no double slashes in the text_document_folder path
    text_document_folder = text_document_folder.rstrip('/')

    # Construct the path to the metadata file in 'text-document' bucket
    metadata_file_name = "metadata.json"
    metadata_file_path = f"{text_document_folder}/{metadata_file_name}".replace("\\", "/")

    # Log the path being used for debugging
    print(f"Attempting to fetch metadata from: {text_document_bucket_name}/{metadata_file_path}")

    try:
        # Fetch the metadata.json file from the 'text-document' bucket
        local_metadata_path = "/tmp/upload_metadata.json"
        client.fget_object(text_document_bucket_name, metadata_file_path, local_metadata_path)

        # Load the metadata.json file
        with open(local_metadata_path, 'r') as f:
            metadata = json.load(f)

        # Extract the list of uploaded files from the metadata
        uploaded_files = metadata.get('uploaded_files', [])
        if not uploaded_files:
            print("No files to process in uploaded_files.")
            return

    except Exception as e:
        print(f"Error fetching metadata or reading metadata.json: {e}")
        return

    # Now process the files listed in 'uploaded_files' (from the 'document' bucket)
    extracted_texts = {}
    text_documents = []  # New list to store processed text documents

    for file_name in uploaded_files:
        try:
            print(f"Processing file: {file_name}")

            # Fetch the file from the 'document' bucket
            file_stream = client.get_object(document_bucket_name, file_name)
            file_content = file_stream.read()  # Read the file content into memory
            file_stream.close()  # Close the file stream after reading

            # Wrap the content in a BytesIO stream (needed for pdfplumber or other extractors)
            file_content_stream = BytesIO(file_content)

            # Extract text (Assuming extract_text_from_stream handles the extraction based on file type)
            extracted_text = extract_text_from_stream(file_content_stream, file_name)
            extracted_texts[file_name] = extracted_text

            # Save the extracted text to a new file in memory (BytesIO)
            text_file_stream = BytesIO(extracted_text.encode('utf-8'))

            # Fix the text file path construction to avoid extra slashes
            text_file_minio_path = os.path.join(text_document_folder,
                                                f"text_{os.path.basename(file_name)}.txt").replace("\\", "/")

            # Upload the extracted text directly to the 'text-document' bucket
            client.put_object(text_document_bucket_name, text_file_minio_path, text_file_stream,
                              length=len(extracted_text.encode('utf-8')))
            print(f"Uploaded extracted text for {file_name} → {text_document_bucket_name}/{text_file_minio_path}")

            # Add the processed text file to the new text_documents list
            text_documents.append(text_file_minio_path)

        except Exception as e:
            print(f"Error processing {file_name}: {e}")

    # Update the metadata by creating a new text_documents list with processed files
    metadata["text_documents"] = text_documents  # New list to hold processed text files

    # Save the updated metadata back to the 'text-document' bucket
    try:
        with open(local_metadata_path, 'w') as f:
            json.dump(metadata, f, indent=4)

        client.fput_object(text_document_bucket_name, metadata_file_path, local_metadata_path)
        print(f"Updated metadata uploaded to: {text_document_bucket_name}/{metadata_file_path}")
    except Exception as e:
        print(f"Failed to update metadata: {e}")

    # Push metadata to XCom
    ti.xcom_push(key='metadata', value=metadata)
    ti.xcom_push(key='text_document_folder', value=text_document_folder)
    print(f"Metadata pushed to XCom: {metadata}")


def copy_metadata_to_embed_document_bucket(**kwargs):
    """Copy the metadata file from the 'text-document' bucket to the 'embed-document' bucket and modify the metadata."""
    ti = kwargs['ti']
    print("Fetching metadata from 'text-document' bucket...")

    # Retrieve the text_document_folder from XCom
    text_document_folder = ti.xcom_pull(task_ids='receive_folder_paths', key='text_document_folder')

    if not text_document_folder:
        raise ValueError("text_document_folder not found in XCom.")

    # MinIO client setup
    client = Minio(
        "host.docker.internal:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False,
    )

    # Define the 'text-document' and 'embed-document' buckets
    source_bucket_name = "text-document"
    target_bucket_name = "embed-document"

    # Ensure the 'embed-document' bucket exists
    if not client.bucket_exists(target_bucket_name):
        client.make_bucket(target_bucket_name)

    # Strip out any extra leading slashes and ensure no double slashes
    text_document_folder = text_document_folder.rstrip('/')

    # Construct the path to the metadata file
    metadata_file_name = "metadata.json"
    metadata_file_path = f"{text_document_folder}/{metadata_file_name}"

    # Log the path being used for debugging
    print(f"Attempting to fetch metadata from: {source_bucket_name}/{metadata_file_path}")

    try:
        # Check if the metadata.json file exists in the 'text-document' bucket
        objects = client.list_objects(source_bucket_name, prefix=metadata_file_path, recursive=False)
        files = [obj.object_name for obj in objects]

        if not files:
            raise FileNotFoundError(f"{metadata_file_name} not found in {source_bucket_name}/{text_document_folder}.")

        # Fetch the metadata.json file
        local_metadata_path = "/tmp/upload_metadata.json"
        client.fget_object(source_bucket_name, metadata_file_path, local_metadata_path)

        # Load the metadata.json file
        with open(local_metadata_path, 'r') as f:
            metadata = json.load(f)

        # Modify the 'bucket_name' to 'embed-document'
        metadata["bucket_name"] = target_bucket_name

        # Fetch a list of text files in the specified folder
        text_files = []
        objects = client.list_objects(source_bucket_name, prefix=text_document_folder, recursive=True)
        for obj in objects:
            if obj.object_name.endswith('.txt'):
                text_files.append(obj.object_name)

        # Add the text files list to the metadata
        metadata["text_files"] = text_files

        # Log the modified metadata for debugging
        print(f"Modified metadata: {metadata}")

        # Save the modified metadata back to a file
        with open(local_metadata_path, 'w') as f:
            json.dump(metadata, f, indent=4)

        # Define the target path in the 'embed-document' bucket
        target_metadata_path = f"{text_document_folder}/{metadata_file_name}"

        # Log the target path
        print(f"Uploading modified metadata to: {target_bucket_name}/{target_metadata_path}")

        # Upload the modified metadata file to the 'embed-document' bucket
        client.fput_object(target_bucket_name, target_metadata_path, local_metadata_path)
        print(f"Modified metadata copied to: {target_bucket_name}/{target_metadata_path}")

    except Exception as e:
        print(f"Failed to copy and modify metadata: {e}")

# DAG configuration
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

# Define the tasks
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

# Define the new task to trigger the 'embed_documents_dag'
trigger_embed_documents_dag_task = TriggerDagRunOperator(
    task_id='trigger_embed_documents_dag',
    trigger_dag_id='generate_embeddings',  # The DAG to be triggered
    conf={
        "text_document_folder": "{{ ti.xcom_pull(task_ids='receive_folder_paths', key='text_document_folder') }}",
        "embed_document_folder": "{{ ti.xcom_pull(task_ids='receive_folder_paths', key='text_document_folder') | replace('text-document', 'embed-document') }}"
    },
    dag=dag,
)

# Set task execution order
receive_folder_paths_task >> process_documents_task >> copy_metadata_task >> trigger_embed_documents_dag_task


