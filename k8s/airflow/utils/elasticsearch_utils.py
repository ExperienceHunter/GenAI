def get_vectorstore():
    from elasticsearch import Elasticsearch
    from langchain.vectorstores import ElasticsearchStore
    from langchain.embeddings import HuggingFaceEmbeddings

    print("[DEBUG] get_vectorstore() called")

    es_url = "http://host.docker.internal:9200"  # ✅ Use Docker host gateway on Mac/Windows
    index_name = "document-embeddings"

    es_client = Elasticsearch(es_url)

    # Optional: Connection test
    if not es_client.ping():
        raise ConnectionError(f"Could not connect to Elasticsearch at {es_url}")

    # Check if the index exists
    if not es_client.indices.exists(index=index_name):
        es_client.indices.create(index=index_name)
        print(f"[INFO] Created index: {index_name}")
    else:
        print(f"[INFO] Index {index_name} already exists.")

    # Initialize the embedding model
    embedding_model = HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2")

    # Create vector store using the Elasticsearch client (not es_url)
    vectorstore = ElasticsearchStore(
        embedding=embedding_model,
        index_name=index_name,
        es_connection=es_client  # ✅ This is key
    )

    return vectorstore

