"""
Script de Spark para el procesamiento Bronze → Silver.
En producción, este script se ejecutaría con spark-submit.
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, current_timestamp
from delta import configure_spark_with_delta_pip

def main():
    # Configurar Spark con Delta Lake
    builder = SparkSession.builder \
        .appName("AmazonIA4.0-BronzeToSilver") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    # Leer datos de Bronze
    df_bronze = spark.read.format("delta").load("s3a://lakehouse/delta/bronze/telemetria")
    
    # Limpieza y validación
    df_silver = df_bronze \
        .filter(col("estado_calidad") == "valid") \
        .withColumn("temperatura_celsius", 
                    when(col("temperatura") > 100, None).otherwise(col("temperatura"))) \
        .withColumn("timestamp_procesado", current_timestamp()) \
        .dropDuplicates(["estacion_id", "timestamp"])
    
    # Escribir en Silver
    df_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save("s3a://lakehouse/delta/silver/telemetria_limpia")
    
    print(f"Registros procesados: {df_silver.count()}")
    spark.stop()


if __name__ == "__main__":
    main()