import os
import json
import logging
from io import BytesIO
from .minio_utils import get_minio_client, TEXT_DOCUMENT_BUCKET

logger = logging.getLogger(__name__)
METADATA_FILE_NAME = "metadata.json"

def extract_text_from_stream(file_stream, file_path):
    """Extracts text from PDF or plain text files."""
    import pdfplumber

    logger.info(f"Extracting text from file: {file_path}")
    file_stream.seek(0)

    if file_path.lower().endswith('.pdf'):
        with pdfplumber.open(file_stream) as pdf:
            return "\n".join([page.extract_text() for page in pdf.pages if page.extract_text()])
    else:
        return file_stream.read().decode("utf-8", errors="ignore")

def copy_metadata_between_buckets(source_bucket, target_bucket, folder_path):
    """Copies metadata.json from source to target bucket and updates the bucket name."""
    client = get_minio_client()
    metadata_path = os.path.join(folder_path, METADATA_FILE_NAME)
    local_path = "/tmp/upload_metadata.json"

    try:
        client.fget_object(source_bucket, metadata_path, local_path)
        with open(local_path, "r") as f:
            metadata = json.load(f)
        metadata["bucket_name"] = target_bucket
        with open(local_path, "w") as f:
            json.dump(metadata, f, indent=4)
        client.fput_object(target_bucket, metadata_path, local_path)
        logger.info(f"Copied metadata from '{source_bucket}' to '{target_bucket}'.")
    except Exception as e:
        logger.error(f"Failed to copy metadata: {e}")
        raise
