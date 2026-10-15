"""
Script de Spark para el procesamiento Silver → Gold.
Calcula agregaciones e indicadores climáticos.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, max, min, sum, count
from delta import configure_spark_with_delta_pip

def main():
    builder = SparkSession.builder \
        .appName("AmazonIA4.0-SilverToGold") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    # Leer datos de Silver
    df_silver = spark.read.format("delta").load("s3a://lakehouse/delta/silver/telemetria_limpia")
    
    # Agregaciones horarias
    df_gold = df_silver \
        .groupBy(
            window(col("timestamp"), "1 hour"),
            col("estacion_id")
        ) \
        .agg(
            avg("temperatura_celsius").alias("temp_promedio"),
            max("temperatura_celsius").alias("temp_maxima"),
            min("temperatura_celsius").alias("temp_minima"),
            sum("precipitacion").alias("precipitacion_acumulada"),
            avg("humedad").alias("humedad_promedio"),
            avg("viento").alias("viento_promedio"),
            count("*").alias("total_lecturas")
        )
    
    # Escribir en Gold
    df_gold.write \
        .format("delta") \
        .mode("overwrite") \
        .save("s3a://lakehouse/delta/gold/indicadores_horarios")
    
    print(f"Agregaciones calculadas: {df_gold.count()}")
    spark.stop()


if __name__ == "__main__":
    main()