"""
etl_orders_emr.py
=================
Aplicación PySpark para el Proyecto Final — Parte I (Batch Processing en AWS EMR).

Lee el dataset crudo (>= 30 GB en JSONL) y el catálogo de tiendas (CSV) desde S3,
aplica transformaciones lazy (limpieza, derivación de columnas, join con broadcast,
filtrado/ordenamiento y agregaciones), y persiste el resultado en S3 en formato
Parquet particionado por `country_region`.

Subida a S3:
    aws s3 cp spark_utils_emr.py s3://<tu-bucket>/src/spark_utils_emr.py
    aws s3 cp etl_orders_emr.py  s3://<tu-bucket>/src/etl_orders_emr.py

Ejecución en EMR (spark-submit / EMR Step):
    spark-submit \
      --deploy-mode cluster \
      --py-files s3://<tu-bucket>/src/spark_utils_emr.py \
      s3://<tu-bucket>/src/etl_orders_emr.py \
      s3://<tu-bucket>/raw/orders/ \
      s3://<tu-bucket>/raw/stores_catalog.csv \
      s3://<tu-bucket>/output/orders_parquet/
"""

import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    avg,
    broadcast,
    col,
    count,
    date_format,
    round as sround,
    sum as ssum,
    to_date,
    to_timestamp,
    trim,
    when,
)

from spark_utils_emr import SparkUtils

# Rutas por defecto (reemplazar con tu bucket real si no se pasan argumentos)
DEFAULT_ORDERS_PATH = "s3://<tu-bucket>/raw/orders/"
DEFAULT_STORES_PATH = "s3://<tu-bucket>/raw/stores_catalog.csv"
DEFAULT_OUTPUT_PATH = "s3://<tu-bucket>/output/orders_parquet/"


def build_spark_session(app_name: str = "final-project-part1-batch-etl") -> SparkSession:
    """Inicializa SparkSession sin fijar .master() para que YARN lo gestione en EMR."""
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    orders_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_ORDERS_PATH
    stores_path = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_STORES_PATH
    output_path = sys.argv[3] if len(sys.argv) > 3 else DEFAULT_OUTPUT_PATH
    if not output_path.endswith("/"):
        output_path += "/"

    spark = build_spark_session()

    # =========================================================================
    # 1. INGESTA CON ESQUEMA EXPLÍCITO (SparkUtils)
    # =========================================================================
    orders_schema = SparkUtils.generate_schema([
        ("order_id", "StringType"),
        ("event_timestamp", "StringType"),
        ("customer_id", "StringType"),
        ("customer_name", "StringType"),
        ("customer_email", "StringType"),
        ("customer_age", "IntegerType"),
        ("store_id", "StringType"),
        ("city", "StringType"),
        ("country_region", "StringType"),
        ("product_category", "StringType"),
        ("payment_method", "StringType"),
        ("items_count", "IntegerType"),
        ("unit_price", "DoubleType"),
        ("discount_pct", "DoubleType"),
        ("delivery_time_min", "DoubleType"),
        ("distance_km", "DoubleType"),
        ("customer_rating", "DoubleType"),
        ("order_status", "StringType"),
        ("notes", "StringType"),
    ])

    stores_schema = SparkUtils.generate_schema([
        ("store_id", "StringType"),
        ("store_name", "StringType"),
        ("store_tier", "StringType"),
        ("commission_rate", "DoubleType"),
        ("manager_name", "StringType"),
        ("opened_year", "IntegerType"),
    ])

    raw_orders_df = (
        spark.read
        .schema(orders_schema)
        .option("mode", "PERMISSIVE")
        .json(orders_path)
    )

    stores_df = (
        spark.read
        .schema(stores_schema)
        .option("header", "true")
        .csv(stores_path)
    )

    # =========================================================================
    # 2. TRANSFORMACIONES LAZY (sin acciones de escritura/persistencia aquí)
    # =========================================================================

    # 2.1 Data Cleaning: eliminar duplicados, limpiar espacios, imputar nulos y
    # filtrar registros con precios o cantidades inválidas.
    clean_orders_df = (
        raw_orders_df
        .dropDuplicates(["order_id"])
        .withColumn("city", trim(col("city")))
        .fillna({
            "city": "Unknown",
            "customer_rating": 3.0,
            "customer_age": 30,
        })
        .filter((col("unit_price") > 0) & (col("items_count") > 0))
    )

    # 2.2 Column Derivation: fechas, montos bruto/neto, SLA de entrega y segmento de edad.
    derived_df = (
        clean_orders_df
        .withColumn("event_ts", to_timestamp(col("event_timestamp")))
        .withColumn("order_date", to_date(col("event_ts")))
        .withColumn("order_month", date_format(col("event_ts"), "yyyy-MM"))
        .withColumn("gross_amount", sround(col("items_count") * col("unit_price"), 2))
        .withColumn(
            "net_amount",
            sround(col("gross_amount") * (1.0 - col("discount_pct") / 100.0), 2),
        )
        .withColumn(
            "delivery_sla",
            when(col("delivery_time_min") < 20.0, "FAST")
            .when(col("delivery_time_min") <= 40.0, "ON_TIME")
            .otherwise("LATE"),
        )
        .withColumn(
            "age_group",
            when(col("customer_age") < 25, "YOUNG")
            .when(col("customer_age") <= 50, "ADULT")
            .otherwise("SENIOR"),
        )
    )

    # 2.3 Joins: enriquecer órdenes con el catálogo de tiendas (broadcast join).
    joined_df = (
        derived_df
        .join(broadcast(stores_df), on="store_id", how="left")
        .withColumn(
            "platform_fee",
            sround(col("net_amount") * col("commission_rate"), 2),
        )
    )

    # 2.4 Filtering & Sorting: conservar órdenes válidas (no canceladas) y ordenar
    # dentro de cada partición por fecha y monto neto descendente.
    filtered_df = (
        joined_df
        .filter(col("order_status") != "CANCELLED")
        .sortWithinPartitions(col("order_date").desc(), col("net_amount").desc())
    )

    # 2.5 Aggregations: métricas de resumen por región, categoría y nivel de tienda
    # (tanto mediante Window sobre el dataset enriquecido como en tabla agregada).
    region_cat_window = Window.partitionBy("country_region", "product_category")
    final_enriched_df = (
        filtered_df
        .withColumn(
            "region_cat_avg_ticket",
            sround(avg("net_amount").over(region_cat_window), 2),
        )
        .withColumn(
            "region_cat_order_count",
            count("order_id").over(region_cat_window),
        )
    )

    summary_df = (
        filtered_df
        .groupBy("country_region", "product_category", "store_tier", "order_month")
        .agg(
            count("order_id").alias("total_orders"),
            sround(ssum("net_amount"), 2).alias("total_revenue"),
            sround(ssum("platform_fee"), 2).alias("total_platform_fees"),
            sround(avg("delivery_time_min"), 2).alias("avg_delivery_min"),
            sround(avg("customer_rating"), 2).alias("avg_rating"),
        )
        .orderBy(col("country_region"), col("total_revenue").desc())
    )

    # =========================================================================
    # 3. PERSISTENCIA EN AMAZON S3 (PARQUET PARTICIONADO)
    # =========================================================================
    (
        final_enriched_df
        .write
        .mode("overwrite")
        .partitionBy("country_region")
        .parquet(f"{output_path}orders_enriched/")
    )

    (
        summary_df
        .write
        .mode("overwrite")
        .partitionBy("country_region")
        .parquet(f"{output_path}summary_by_region/")
    )

    spark.stop()


if __name__ == "__main__":
    main()
