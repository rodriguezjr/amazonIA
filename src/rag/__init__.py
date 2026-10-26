"""
Módulo RAG (Retrieval-Augmented Generation) para AmazonIA 4.0.
Combina telemetría en tiempo real con memoria semántica vectorial de la cuenca del Río Caroní.
"""
from src.rag.vector_store import AmazonIAVectorStore
from src.rag.engine import responder_consulta_rag

__all__ = ["AmazonIAVectorStore", "responder_consulta_rag"]
