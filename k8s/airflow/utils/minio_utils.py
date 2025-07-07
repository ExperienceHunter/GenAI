import json
from io import BytesIO
from minio import Minio

MINIO_CONFIG = {
    "endpoint": "host.docker.internal:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
    "secure": False
}

TEXT_DOCUMENT_BUCKET = "text-document"
EMBED_DOCUMENT_BUCKET = "embed-document"

def get_minio_client():
    client = Minio(**MINIO_CONFIG)
    if not client.bucket_exists(EMBED_DOCUMENT_BUCKET):
        client.make_bucket(EMBED_DOCUMENT_BUCKET)
    return client

def get_text_from_minio(client, path):
    obj = client.get_object(TEXT_DOCUMENT_BUCKET, path)
    return obj.read().decode('utf-8')

def save_embedding_to_minio(client, embedding, object_path):
    data = json.dumps(embedding.tolist()).encode('utf-8')
    client.put_object(
        EMBED_DOCUMENT_BUCKET, object_path,
        data=BytesIO(data),
        length=len(data),
        content_type='application/json'
    )

def load_metadata(client, path):
    try:
        obj = client.get_object(EMBED_DOCUMENT_BUCKET, path)
        return json.loads(obj.read().decode('utf-8'))
    except:
        return {"text_documents": [], "embedded_documents": []}

def save_metadata(client, path, metadata):
    metadata_json = json.dumps(metadata, indent=4).encode('utf-8')
    client.put_object(
        EMBED_DOCUMENT_BUCKET, path,
        data=BytesIO(metadata_json),
        length=len(metadata_json),
        content_type='application/json'
    )
