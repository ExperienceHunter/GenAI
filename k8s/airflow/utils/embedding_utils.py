from sentence_transformers import SentenceTransformer

class EmbeddingUtils:
    def __init__(self, model_name='all-MiniLM-L6-v2'):
        self.model = SentenceTransformer(model_name)

    def get_embedding(self, text):  # ✅ Rename here
        return self.model.encode(text).tolist()  # ✅ .tolist() for JSON serialization
