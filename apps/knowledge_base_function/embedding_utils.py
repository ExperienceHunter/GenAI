# embedding_utils.py
from sentence_transformers import SentenceTransformer
import json

class EmbeddingUtils:
    def __init__(self, model_name='all-MiniLM-L6-v2'):
        self.model = SentenceTransformer(model_name)

    def generate_embedding(self, text):
        """Generate embeddings for a given text."""
        return self.model.encode(text)

    def save_embedding(self, embedding, bucket_name, object_name, minio_client):
        """Save embeddings as JSON to MinIO."""
        embedding_json = json.dumps(embedding.tolist())  # Convert numpy array to list
        minio_client.upload_object(
            bucket_name, object_name,
            data=embedding_json.encode('utf-8'),
            content_type='application/json'
        )
