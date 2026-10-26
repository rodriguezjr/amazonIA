"""
Módulo Lakehouse Medallion (Bronze / Silver / Gold) para AmazonIA 4.0.
Motor analítico columnar Offline-First basado en DuckDB y Apache Parquet.
"""
from src.lakehouse.manager import AmazonIALakehouse, obtener_lakehouse

__all__ = ["AmazonIALakehouse", "obtener_lakehouse"]
