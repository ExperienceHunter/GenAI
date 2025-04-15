from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# -------------------------------
# ⚙️ DAG Configuration
# -------------------------------
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
    description='Generate embeddings and store in MinIO + Elasticsearch via LangChain',
    schedule_interval=None,
    catchup=False,
)


# -------------------------------
# 📥 Receive Folder Paths
# -------------------------------
def receive_folder_paths(**kwargs):
    conf = kwargs['dag_run'].conf or {}
    text_folder = conf.get('text_document_folder')
    embed_folder = conf.get('embed_document_folder')

    if not text_folder or not embed_folder:
        raise ValueError("Both 'text_document_folder' and 'embed_document_folder' must be provided.")

    print(f"Received: text_folder={text_folder}, embed_folder={embed_folder}")
    kwargs['ti'].xcom_push(key='text_folder', value=text_folder)
    kwargs['ti'].xcom_push(key='embed_folder', value=embed_folder)


# -------------------------------
# 🧩 Main Task: Process Embeddings
# -------------------------------
def process_embeddings(**kwargs):
    import os
    import json
    from io import BytesIO
    from minio import Minio
    from sentence_transformers import SentenceTransformer
    from elasticsearch import Elasticsearch
    from langchain.vectorstores import ElasticsearchStore
    from langchain.schema import Document
    from langchain.embeddings import HuggingFaceEmbeddings

    # 🧾 MinIO Configuration
    MINIO_CONFIG = {
        "endpoint": "host.docker.internal:9000",
        "access_key": "minioadmin",
        "secret_key": "minioadmin",
        "secure": False
    }

    TEXT_DOCUMENT_BUCKET = "text-document"
    EMBED_DOCUMENT_BUCKET = "embed-document"

    # 🧠 Embedding Utility
    class EmbeddingUtils:
        def __init__(self, model_name='all-MiniLM-L6-v2'):
            self.model = SentenceTransformer(model_name)

        def generate_embedding(self, text):
            return self.model.encode(text)

        def save_embedding(self, embedding, bucket, object_name, minio_client):
            embedding_json = json.dumps(embedding.tolist())
            minio_client.put_object(
                bucket, object_name,
                data=BytesIO(embedding_json.encode('utf-8')),
                length=len(embedding_json.encode('utf-8')),
                content_type='application/json'
            )

    # Step 1: Load input from XComs
    ti = kwargs['ti']
    text_folder = ti.xcom_pull(task_ids='receive_folder_paths', key='text_folder').rstrip('/')
    embed_folder = ti.xcom_pull(task_ids='receive_folder_paths', key='embed_folder').rstrip('/')
    metadata_path = f"{embed_folder}/metadata.json".replace("//", "/")

    # Step 2: Initialize MinIO client
    minio_client = Minio(**MINIO_CONFIG)
    if not minio_client.bucket_exists(EMBED_DOCUMENT_BUCKET):
        minio_client.make_bucket(EMBED_DOCUMENT_BUCKET)

    # Step 3: Initialize Elasticsearch and LangChain
    try:
        es_client = Elasticsearch("http://elasticsearch:9200")
        if not es_client.indices.exists(index="document-embeddings"):
            es_client.indices.create(index="document-embeddings")
            print("[INFO] Created 'document-embeddings' index.")

        embedding_model = HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2")
        vectorstore = ElasticsearchStore(
            es_connection=es_client,
            index_name="document-embeddings",
            embedding=embedding_model
        )
    except Exception as e:
        print(f"[ERROR] Connecting to Elasticsearch: {e}")
        return

    # Step 4: Load metadata
    try:
        metadata_obj = minio_client.get_object(EMBED_DOCUMENT_BUCKET, metadata_path)
        metadata = json.loads(metadata_obj.read().decode('utf-8'))
    except Exception as e:
        print(f"[ERROR] Failed to retrieve metadata: {e}")
        return

    text_files = metadata.get("text_documents", [])
    embedded_files = metadata.get("embedded_documents", [])

    if not text_files:
        print("[INFO] No files to process.")
        return

    # Step 5: Process and embed
    embedding_utils = EmbeddingUtils()
    for file_path in text_files:
        try:
            text_obj = minio_client.get_object(TEXT_DOCUMENT_BUCKET, file_path)
            document_text = text_obj.read().decode('utf-8')
            embedding = embedding_utils.generate_embedding(document_text)

            file_name = os.path.basename(file_path)
            embed_object = f"{embed_folder}/embedded_{file_name}.json"
            embedding_utils.save_embedding(embedding, EMBED_DOCUMENT_BUCKET, embed_object, minio_client)
            embedded_files.append(embed_object)

            # Store in vector DB
            vectorstore.add_documents([
                Document(page_content=document_text, metadata={"source": file_path})
            ])

            print(f"[SUCCESS] Processed: {file_path}")
        except Exception as e:
            print(f"[ERROR] Processing {file_path}: {e}")

    # Step 6: Update metadata
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
        print(f"[ERROR] Failed to update metadata: {e}")


# -------------------------------
# 🧱 Task Definitions
# -------------------------------
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

receive_folder_paths_task >> process_embeddings_task
