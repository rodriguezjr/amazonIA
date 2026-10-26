"""
Módulo de Almacenamiento Vectorial (Qdrant) para AmazonIA 4.0.
Gestiona la indexación y recuperación semántica de documentos técnicos y agroclimáticos.
Soporta conexión a broker Qdrant remoto o persistencia local en memoria / disco (Offline-First).
"""
import os
import re
import math
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional

import numpy as np
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance,
    VectorParams,
    PointStruct,
)

logger = logging.getLogger("amazonia.rag.vector_store")

BASE_DIR = Path(__file__).resolve().parent.parent.parent
KNOWLEDGE_BASE_DIR = BASE_DIR / "knowledge_base"
LOCAL_STORAGE_DIR = BASE_DIR / "data" / "qdrant_storage"

COLECCION_DEFECTO = "documentos_amazonia"
VECTOR_DIM = 128  # Dimensión optimizada para el generador de embeddings local / offline


def _calcular_embedding_offline(texto: str, dim: int = VECTOR_DIM) -> List[float]:
    """
    Generador de embeddings determinista y semántico para operación Offline-First.
    Transforma texto en un vector normalizado de dimensión fija basándose en n-gramas
    y frecuencias léxicas, permitiendo similitud coseno precisa sin requerir internet.
    """
    vector = np.zeros(dim, dtype=np.float32)
    palabras = re.findall(r"\w+", texto.lower())
    if not palabras:
        return vector.tolist()

    for idx, palabra in enumerate(palabras):
        # Hashing polinomial consistente de palabras
        h = sum(ord(c) * (31 ** i) for i, c in enumerate(palabra[:10]))
        pos = h % dim
        peso = 1.0 + (len(palabra) / 10.0)
        vector[pos] += peso

        # Bigramas de contexto
        if idx > 0:
            bi = f"{palabras[idx - 1]}_{palabra}"
            h_bi = sum(ord(c) * (37 ** i) for i, c in enumerate(bi[:12]))
            vector[h_bi % dim] += 1.5

    # Normalización L2 para cálculo exacto de distancia Coseno
    norma = np.linalg.norm(vector)
    if norma > 0:
        vector = vector / norma

    return vector.tolist()


class AmazonIAVectorStore:
    """
    Gestor de la base de datos vectorial Qdrant para AmazonIA 4.0.
    """
    _clientes_cache: Dict[str, QdrantClient] = {}

    def __init__(
        self,
        coleccion: str = COLECCION_DEFECTO,
        url_qdrant: Optional[str] = None,
        en_memoria: bool = False,
        storage_dir: Optional[Path] = None,
    ):
        self.coleccion = coleccion
        self.url = url_qdrant or os.getenv("QDRANT_URL", "http://localhost:6333")
        self.storage_dir = storage_dir or LOCAL_STORAGE_DIR
        self.cliente = self._conectar_cliente(en_memoria)
        self._asegurar_coleccion()

    def _conectar_cliente(self, en_memoria: bool) -> QdrantClient:
        """Intenta conectar a Qdrant remoto; si no está disponible, usa modo local / memoria con caché de conexión."""
        if en_memoria:
            logger.info("Iniciando Qdrant en modo memoria (:memory:)")
            return QdrantClient(":memory:")

        # Intentar conectar a Qdrant en red (Docker / Servicio)
        if self.url and not self.url.startswith(":"):
            clave_red = f"url:{self.url}"
            if clave_red in self._clientes_cache:
                return self._clientes_cache[clave_red]
            try:
                cliente = QdrantClient(url=self.url, timeout=1.0, check_compatibility=False)
                cliente.get_collections()
                logger.info("Conectado exitosamente a Qdrant remoto en %s", self.url)
                self._clientes_cache[clave_red] = cliente
                return cliente
            except Exception as e:
                logger.warning(
                    "No se pudo conectar a Qdrant en %s (%s). Activando motor local en disco (Offline-First).",
                    self.url,
                    e,
                )

        # Fallback local persistente en disco (reutilizando instancia para evitar colisiones de lock)
        ruta_str = str(self.storage_dir)
        if ruta_str in self._clientes_cache:
            return self._clientes_cache[ruta_str]

        self.storage_dir.mkdir(parents=True, exist_ok=True)
        cliente = QdrantClient(path=ruta_str, check_compatibility=False)
        self._clientes_cache[ruta_str] = cliente
        return cliente

    def _asegurar_coleccion(self):
        """Crea la colección vectorial si no existe aún en Qdrant."""
        try:
            colecciones = [c.name for c in self.cliente.get_collections().collections]
            if self.coleccion not in colecciones:
                self.cliente.create_collection(
                    collection_name=self.coleccion,
                    vectors_config=VectorParams(size=VECTOR_DIM, distance=Distance.COSINE),
                )
                logger.info("Colección '%s' creada en Qdrant (dim=%d)", self.coleccion, VECTOR_DIM)
        except Exception as e:
            logger.error("Error asegurando colección en Qdrant: %s", e)

    def indexar_documentos(self, forzar: bool = False) -> int:
        """
        Lee los documentos markdown de knowledge_base/, los divide en chunks,
        genera sus embeddings y los almacena en Qdrant.
        """
        if not KNOWLEDGE_BASE_DIR.exists():
            logger.warning("No se encontró el directorio knowledge_base: %s", KNOWLEDGE_BASE_DIR)
            return 0

        # Verificar si ya tiene datos
        try:
            info = self.cliente.get_collection(self.coleccion)
            puntos_actuales = info.points_count or 0
            if puntos_actuales > 0 and not forzar:
                logger.info("La colección '%s' ya contiene %d puntos indexados.", self.coleccion, puntos_actuales)
                return puntos_actuales
        except Exception:
            puntos_actuales = 0

        puntos: List[PointStruct] = []
        punto_id = 1

        for archivo in KNOWLEDGE_BASE_DIR.glob("*.md"):
            try:
                contenido = archivo.read_text(encoding="utf-8")
            except Exception as e:
                logger.error("Error leyendo %s: %s", archivo.name, e)
                continue

            # Separar por secciones de Markdown (##)
            secciones = re.split(r"\n(?=##?\s)", contenido)
            for sec in secciones:
                sec_limpia = sec.strip()
                if len(sec_limpia) < 40:
                    continue

                lineas = sec_limpia.split("\n")
                titulo = lineas[0].replace("#", "").strip()
                texto_cuerpo = "\n".join(lineas[1:]).strip()

                emb = _calcular_embedding_offline(sec_limpia)

                puntos.append(
                    PointStruct(
                        id=punto_id,
                        vector=emb,
                        payload={
                            "documento": archivo.name,
                            "titulo": titulo,
                            "texto": sec_limpia,
                            "longitud": len(sec_limpia),
                        },
                    )
                )
                punto_id += 1

        if puntos:
            self.cliente.upsert(collection_name=self.coleccion, points=puntos)
            logger.info("Se indexaron %d chunks en la colección '%s'", len(puntos), self.coleccion)

        return len(puntos)

    def buscar_similares(self, consulta: str, top_k: int = 3) -> List[Dict[str, Any]]:
        """
        Busca los documentos y manuales más relevantes para la consulta del usuario.
        """
        emb_consulta = _calcular_embedding_offline(consulta)

        try:
            # Compatibilidad con qdrant_client query_points / search
            if hasattr(self.cliente, "query_points"):
                res = self.cliente.query_points(
                    collection_name=self.coleccion,
                    query=emb_consulta,
                    limit=top_k,
                )
                puntos = res.points
            else:
                puntos = self.cliente.search(
                    collection_name=self.coleccion,
                    query_vector=emb_consulta,
                    limit=top_k,
                )

            resultados = []
            for p in puntos:
                resultados.append({
                    "id": p.id,
                    "score": round(float(p.score), 3),
                    "titulo": p.payload.get("titulo", "Sin título"),
                    "documento": p.payload.get("documento", ""),
                    "texto": p.payload.get("texto", ""),
                })
            return resultados
        except Exception as e:
            logger.error("Error buscando en Qdrant: %s", e)
            return []


def cerrar_clientes_qdrant():
    """Cierra limpiamente las conexiones abiertas de Qdrant."""
    for cliente in list(AmazonIAVectorStore._clientes_cache.values()):
        try:
            cliente.close()
        except Exception:
            pass
    AmazonIAVectorStore._clientes_cache.clear()
