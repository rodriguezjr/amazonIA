"""
Indexador RAG para AmazonIA 4.0.
Usa AmazonIAVectorStore (Qdrant offline) ya implementado en src/rag/.
Sin dependencias de LangChain ni OpenAI — embeddings deterministas locales.

Lógica:
  1. Importa AmazonIAVectorStore desde src/rag/vector_store.
  2. Ejecuta indexar_documentos(forzar=True) para re-indexar toda la knowledge_base.
  3. Retorna el conteo de documentos indexados.
"""
import sys
import os
import logging
from pathlib import Path

# Agregar la raíz del proyecto al PYTHONPATH para importar src/
_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_ROOT))

logger = logging.getLogger("amazonia.pipeline.rag_indexer")


def main() -> dict:
    """
    Actualiza la base vectorial Qdrant con los documentos de knowledge_base/.
    Retorna un diccionario con el resumen de la indexación.
    """
    logger.info("═══ Iniciando indexación RAG (Qdrant) ═══")

    from src.rag.vector_store import AmazonIAVectorStore

    try:
        vs = AmazonIAVectorStore()
        total = vs.indexar_documentos(forzar=True)

        resultado = {
            "documentos_indexados": total,
            "coleccion": vs.coleccion,
            "estado": "exitoso",
        }
        logger.info("═══ Indexación RAG completada: %s ═══", resultado)
        return resultado

    except Exception as e:
        logger.error("Error durante la indexación RAG: %s", e)
        return {
            "documentos_indexados": 0,
            "estado": "error",
            "detalle": str(e),
        }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
    resultado = main()
    print(f"Resultado: {resultado}")
