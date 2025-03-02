import os
from apps.knowledge_base_function.minio_client import MinioClient
from apps.knowledge_base_function.pdf_utils import extract_text_from_pdf
from apps.knowledge_base_function.embedding_utils import EmbeddingUtils
from dotenv import load_dotenv

def process_documents():
    # Load environment variables from .env file
    load_dotenv()

    # Initialize MinIO client with environment variables
    minio_client = MinioClient(
        os.getenv("MINIO_HOST"),  # MinIO is running on the host from the .env file
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False
    )

    # Initialize embedding model
    embedding_utils = EmbeddingUtils()

    # Define buckets from the environment variables
    document_bucket = os.getenv("DOCUMENT_BUCKET")
    embed_bucket = os.getenv("EMBED_BUCKET")
    text_document_bucket = os.getenv("TEXT_DOCUMENT_BUCKET")

    # Ensure the 'downloads' directory exists
    os.makedirs("downloads", exist_ok=True)

    # Create required buckets if they don't exist
    for bucket in [document_bucket, embed_bucket, text_document_bucket]:
        try:
            # Check if the bucket exists before creating it
            if not minio_client.bucket_exists(bucket):
                minio_client.create_bucket(bucket)
        except Exception as e:
            print(f"Error ensuring bucket '{bucket}': {e}")

    # List and retrieve documents from the 'document' bucket
    objects = minio_client.list_objects(document_bucket)
    documents = []
    document_names = []

    for obj in objects:
        file_path = f"downloads/{obj.object_name}"
        try:
            minio_client.download_object(document_bucket, obj.object_name, file_path)

            # Check if the document is a PDF (you can add more file types as needed)
            if file_path.lower().endswith('.pdf'):
                # Extract text from the PDF
                document_text = extract_text_from_pdf(file_path)
                documents.append(document_text)
            else:
                # Handle non-PDF files as plain text or other methods
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
                    documents.append(file.read())

            document_names.append(obj.object_name)
        except Exception as e:
            print(f"Error processing {obj.object_name}: {e}")

    # Store extracted text documents in the 'text-document' bucket
    for idx, document_text in enumerate(documents):
        try:
            object_name = f"text_{document_names[idx]}.txt"  # Naming text files based on the document name

            # Convert the text document into bytes
            document_bytes = document_text.encode('utf-8')

            # Upload the text document to MinIO as a bytes-like object
            minio_client.upload_object(
                text_document_bucket, object_name,
                data=document_bytes,
                content_type='text/plain'
            )
            print(f"Uploaded text document for {document_names[idx]} to {object_name}")

            # Generate the embeddings for the extracted text
            embedding = embedding_utils.generate_embedding(document_text)

            # Save the embeddings in the 'embed-document' bucket
            embedding_object_name = f"embedding_{document_names[idx]}.json"
            embedding_utils.save_embedding(embedding, embed_bucket, embedding_object_name, minio_client)
            print(f"Uploaded embedding for {document_names[idx]}")

        except Exception as e:
            print(f"Error processing document {document_names[idx]}: {e}")

    print("All text documents and embeddings have been stored.")

# To run the process, simply call the function:
# process_documents()
