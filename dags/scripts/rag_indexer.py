"""
Indexador de documentos y datos para la memoria semántica (RAG).
Actualiza la base vectorial Qdrant con nuevos embeddings.
"""
import logging
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams
from langchain_qdrant import QdrantVectorStore
from langchain_openai import OpenAIEmbeddings
from langchain_community.document_loaders import PyPDFLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter

logger = logging.getLogger(__name__)


def actualizar_vector_store():
    """
    Actualiza la base vectorial con nuevos documentos técnicos.
    """
    client = QdrantClient(url="http://qdrant:6333")
    
    # Crear colección si no existe
    collections = client.get_collections().collections
    if not any(c.name == "documentos_amazonia" for c in collections):
        client.create_collection(
            collection_name="documentos_amazonia",
            vectors_config=VectorParams(size=3072, distance=Distance.COSINE),
        )
        logger.info("Colección 'documentos_amazonia' creada")
    
    # Cargar e indexar documentos
    embedding_model = OpenAIEmbeddings(model="text-embedding-3-large")
    
    documentos = [
        "manuales/agricultura_caroni.pdf",
        "manuales/gestion_riesgos.pdf",
        "guias/calendario_siembra.pdf",
    ]
    
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=200)
    
    for doc_path in documentos:
        try:
            loader = PyPDFLoader(doc_path)
            docs = loader.load()
            chunks = text_splitter.split_documents(docs)
            
            QdrantVectorStore.from_documents(
                documents=chunks,
                embedding=embedding_model,
                url="http://qdrant:6333",
                collection_name="documentos_amazonia",
            )
            logger.info(f"Indexado: {doc_path} ({len(chunks)} chunks)")
        except Exception as e:
            logger.error(f"Error indexando {doc_path}: {e}")
    
    return {"documentos_indexados": len(documentos)}