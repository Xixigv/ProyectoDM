"""
streaming_consumer.py
=====================
Proyecto Final — Parte II (Structured Streaming): Aplicación CONSUMIDORA.

Lee los eventos de órdenes que publica `producer/kafka_producer.py` en Kafka,
re-aplica en streaming la misma lógica de la Parte I (`etl_orders_emr.py`) y
persiste los resultados en MongoDB con `mongo-spark-connector` + `foreachBatch`
(upserts con write concern "majority").

Pipeline (un solo DataFrame que crece continuamente):
    Kafka (orders-stream)
      └─ readStream.format("kafka")
          └─ from_json con esquema explícito (SparkUtils, el mismo de la Parte I)
              ├─ registros malformados / inválidos ─────────────► orders_rejected   (append)
              └─ limpieza + watermark + dropDuplicatesWithinWatermark
                  └─ columnas derivadas (montos, SLA, grupo de edad)
                      └─ stream-static join (broadcast) con stores_catalog.csv
                          ├─ orden enriquecida (documento anidado) ─► orders            (upsert por order_id)
                          ├─ ventana de 1 min por región/categoría ─► metrics_by_window (update + upsert)
                          └─ KPIs acumulados por tienda ───────────► store_kpis        (update + upsert)

Ejecución local (Kafka + MongoDB levantados con docker compose):
    python consumer/streaming_consumer.py
    python consumer/streaming_consumer.py --run-seconds 120 --reset

Ejecución con spark-submit (local o EMR):
    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.mongodb.spark:mongo-spark-connector_2.12:10.4.0 \
      --py-files spark_utils_emr.py \
      consumer/streaming_consumer.py --stores-path s3://<bucket>/raw/stores_catalog.csv

Credenciales: NUNCA en el código. Se leen de variables de entorno (o del archivo
.env, que está en .gitignore):
    MONGODB_URI, MONGODB_DATABASE, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
"""

import argparse
import os
import shutil
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

# --- Ajustes para correr PySpark en Windows (winutils / python del venv) -----
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.pop("JAVA_TOOL_OPTIONS", None)
if os.name == "nt" and not os.environ.get("HADOOP_HOME") and Path(r"C:\hadoop\bin").exists():
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["PATH"] += r";C:\hadoop\bin"

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from spark_utils_emr import SparkUtils  # noqa: E402  (mismo módulo de la Parte I)

KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
MONGO_PKG = "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0"

# Esquema del evento: idéntico a `orders_schema` de etl_orders_emr.py
ORDERS_SCHEMA = SparkUtils.generate_schema([
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

STORES_SCHEMA = SparkUtils.generate_schema([
    ("store_id", "StringType"),
    ("store_name", "StringType"),
    ("store_tier", "StringType"),
    ("commission_rate", "DoubleType"),
    ("manager_name", "StringType"),
    ("opened_year", "IntegerType"),
])


# =============================================================================
# Configuración
# =============================================================================
def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def parse_args() -> argparse.Namespace:
    load_dotenv(REPO_ROOT / ".env")
    p = argparse.ArgumentParser(description="Consumidor Structured Streaming — Proyecto Final Parte II")
    p.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    p.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "orders-stream"))
    p.add_argument("--starting-offsets", default="earliest", choices=["earliest", "latest"])
    p.add_argument("--max-offsets-per-trigger", type=int, default=2000)
    p.add_argument("--trigger", default="10 seconds", help="processingTime del trigger")
    p.add_argument("--window", default="1 minute", help="Duración de la ventana de métricas")
    p.add_argument("--watermark", default="2 minutes", help="Retraso tolerado de eventos tardíos")
    p.add_argument("--stores-path", default=str(REPO_ROOT / "data" / "stores_catalog.csv"))
    p.add_argument("--mongo-uri", default=os.getenv("MONGODB_URI"))
    p.add_argument("--mongo-db", default=os.getenv("MONGODB_DATABASE", "qcommerce"))
    p.add_argument("--checkpoint-dir", default=str(REPO_ROOT / "checkpoints"))
    p.add_argument("--master", default=os.getenv("SPARK_MASTER", "local[4]"),
                   help="'yarn' o vacío en EMR; local[4] en laptop")
    p.add_argument("--run-seconds", type=int, default=0, help="0 = correr hasta Ctrl+C")
    p.add_argument("--reset", action="store_true", help="Borra checkpoints antes de iniciar")
    args = p.parse_args()
    if not args.mongo_uri:
        raise SystemExit("Falta MONGODB_URI (defínelo en .env o como variable de entorno).")
    return args


def build_spark(master: str) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("final-project-part2-streaming-consumer")
        .config("spark.jars.packages", f"{KAFKA_PKG},{MONGO_PKG}")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.streaming.stateStore.stateSchemaCheck", "false")
    )
    if master:
        builder = builder.master(master)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def ensure_stores_catalog(path: str) -> None:
    """Si el catálogo local no existe, se genera con la MISMA semilla que la Parte I."""
    if path.startswith("s3") or Path(path).exists():
        return
    from generate_dataset import build_stores_catalog
    build_stores_catalog(Path(path))
    print(f"[consumer] catálogo de tiendas generado en {path}")


# =============================================================================
# Transformaciones (todas sobre DataFrames de streaming)
# =============================================================================
def read_kafka(spark: SparkSession, args) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_events(raw: DataFrame) -> DataFrame:
    """Kafka value (bytes) -> string -> struct tipado. Nunca truena con JSON malformado."""
    return (
        raw.select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("raw_json"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_ts"),
        )
        .withColumn("data", F.from_json("raw_json", ORDERS_SCHEMA, {"mode": "PERMISSIVE"}))
        .select("kafka_key", "raw_json", "kafka_partition", "kafka_offset", "kafka_ts", "data.*")
        .withColumn("event_ts", F.to_timestamp("event_timestamp"))
    )


def split_valid_rejected(parsed: DataFrame):
    """Separa registros procesables de los que se mandan a cuarentena (orders_rejected)."""
    reason = (
        F.when(F.col("order_id").isNull(), "MALFORMED_JSON")
        .when(F.col("event_ts").isNull(), "INVALID_TIMESTAMP")
        .when(F.col("unit_price").isNull() | (F.col("unit_price") <= 0), "NON_POSITIVE_PRICE")
        .when(F.col("items_count").isNull() | (F.col("items_count") <= 0), "NON_POSITIVE_ITEMS")
    )
    tagged = parsed.withColumn("reject_reason", reason)
    rejected = tagged.filter(F.col("reject_reason").isNotNull()).select(
        "kafka_key", "raw_json", "kafka_partition", "kafka_offset", "kafka_ts",
        "order_id", "unit_price", "items_count", "reject_reason",
        F.current_timestamp().alias("rejected_at"),
    )
    valid = tagged.filter(F.col("reject_reason").isNull()).drop("reject_reason")
    return valid, rejected


def clean_and_derive(valid: DataFrame, watermark: str) -> DataFrame:
    """Data cleaning + column derivation (misma lógica que la Parte I, ahora en streaming)."""
    return (
        valid
        # Watermark: límite de cuánto estado se guarda para eventos tardíos
        .withWatermark("event_ts", watermark)
        # Duplicados: el productor repite ~0.5% de los order_id
        .dropDuplicatesWithinWatermark(["order_id"])
        .withColumn("city", F.trim("city"))
        .fillna({"city": "Unknown", "customer_rating": 3.0, "customer_age": 30})
        .withColumn("order_date", F.to_date("event_ts"))
        .withColumn("gross_amount", F.round(F.col("items_count") * F.col("unit_price"), 2))
        .withColumn("net_amount",
                    F.round(F.col("gross_amount") * (1.0 - F.col("discount_pct") / 100.0), 2))
        .withColumn("discount_amount", F.round(F.col("gross_amount") - F.col("net_amount"), 2))
        .withColumn(
            "delivery_sla",
            F.when(F.col("delivery_time_min") < 20.0, "FAST")
            .when(F.col("delivery_time_min") <= 40.0, "ON_TIME")
            .otherwise("LATE"),
        )
        .withColumn(
            "age_group",
            F.when(F.col("customer_age") < 25, "YOUNG")
            .when(F.col("customer_age") <= 50, "ADULT")
            .otherwise("SENIOR"),
        )
        .withColumn("speed_kmh",
                    F.round(F.col("distance_km") / (F.col("delivery_time_min") / 60.0), 2))
        .withColumn("processing_lag_sec",
                    F.col("kafka_ts").cast("double") - F.col("event_ts").cast("double"))
    )


def enrich_with_stores(stream_df: DataFrame, stores_df: DataFrame) -> DataFrame:
    """Stream-static join: cada orden se enriquece con su tienda (broadcast)."""
    return (
        stream_df.join(F.broadcast(stores_df), on="store_id", how="left")
        .withColumn("platform_fee", F.round(F.col("net_amount") * F.col("commission_rate"), 2))
        .withColumn("is_cancelled", (F.col("order_status") == "CANCELLED").cast("int"))
        .withColumn("is_returned", (F.col("order_status") == "RETURNED").cast("int"))
        .withColumn("is_late", (F.col("delivery_sla") == "LATE").cast("int"))
    )


def to_order_documents(enriched: DataFrame) -> DataFrame:
    """Modelo documental: una orden = un documento con sub-documentos anidados."""
    return enriched.select(
        F.col("order_id").alias("_id"),
        "order_id",
        "event_ts",
        "order_date",
        "order_status",
        "country_region",
        "product_category",
        "payment_method",
        F.struct(
            F.col("customer_id").alias("id"),
            F.col("customer_name").alias("name"),
            F.col("customer_email").alias("email"),
            F.col("customer_age").alias("age"),
            "age_group",
            F.col("customer_rating").alias("rating"),
        ).alias("customer"),
        F.struct(
            F.col("store_id").alias("id"),
            F.col("store_name").alias("name"),
            F.col("store_tier").alias("tier"),
            "commission_rate",
            "city",
        ).alias("store"),
        F.struct(
            "items_count", "unit_price", "discount_pct", "gross_amount",
            "discount_amount", "net_amount", "platform_fee",
        ).alias("amounts"),
        F.struct(
            F.col("delivery_time_min").alias("time_min"),
            "distance_km", "speed_kmh",
            F.col("delivery_sla").alias("sla"),
        ).alias("delivery"),
        F.struct(
            F.col("kafka_partition").alias("partition"),
            F.col("kafka_offset").alias("offset"),
            F.col("kafka_ts").alias("ingested_at"),
            "processing_lag_sec",
        ).alias("kafka"),
        "notes",
    )


def windowed_metrics(enriched: DataFrame, window: str) -> DataFrame:
    """Agregación con ventana de tiempo (tumbling) por región y categoría."""
    w = F.window("event_ts", window)
    return (
        enriched.groupBy(w, "country_region", "product_category")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.round(F.sum("net_amount"), 2).alias("total_revenue"),
            F.round(F.avg("net_amount"), 2).alias("avg_ticket"),
            F.round(F.sum("platform_fee"), 2).alias("total_platform_fees"),
            F.round(F.avg("delivery_time_min"), 2).alias("avg_delivery_min"),
            F.round(F.avg("customer_rating"), 2).alias("avg_rating"),
            F.round(F.avg("is_cancelled") * 100, 2).alias("cancel_rate_pct"),
            F.round(F.avg("is_late") * 100, 2).alias("late_rate_pct"),
            F.approx_count_distinct("customer_id").alias("unique_customers"),
        )
        .select(
            F.concat_ws("|", F.date_format("window.start", "yyyy-MM-dd'T'HH:mm"),
                        "country_region", "product_category").alias("_id"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "country_region", "product_category", "total_orders", "total_revenue",
            "avg_ticket", "total_platform_fees", "avg_delivery_min", "avg_rating",
            "cancel_rate_pct", "late_rate_pct", "unique_customers",
            F.current_timestamp().alias("updated_at"),
        )
    )


def store_kpis(enriched: DataFrame) -> DataFrame:
    """Agregación acumulada (sin ventana) por tienda: estado limitado a 1,000 llaves."""
    return (
        enriched.groupBy("store_id", "store_name", "store_tier")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.round(F.sum("net_amount"), 2).alias("total_revenue"),
            F.round(F.sum("platform_fee"), 2).alias("total_platform_fees"),
            F.round(F.avg("customer_rating"), 2).alias("avg_rating"),
            F.sum("is_cancelled").alias("cancelled_orders"),
            F.sum("is_returned").alias("returned_orders"),
            F.max("event_ts").alias("last_order_at"),
        )
        .withColumn("_id", F.col("store_id"))
        .withColumn("updated_at", F.current_timestamp())
    )


# =============================================================================
# Sink MongoDB (foreachBatch + mongo-spark-connector)
# =============================================================================
def mongo_writer(args, collection: str, operation: str = "replace"):
    """
    Devuelve una función para foreachBatch que escribe cada micro-batch en MongoDB.
    operation="replace" + upsertDocument=true  => UPSERT por _id (idempotente si
    Spark reprocesa un micro-batch después de una falla).
    """
    def _write(batch_df: DataFrame, batch_id: int) -> None:
        batch_df = batch_df.persist()
        n = batch_df.count()
        if n:
            (
                batch_df.write.format("mongodb")
                .mode("append")
                .option("connection.uri", args.mongo_uri)
                .option("database", args.mongo_db)
                .option("collection", collection)
                .option("operationType", operation)
                .option("upsertDocument", "true")
                .option("idFieldList", "_id")
                .option("writeConcern.w", "majority")
                .option("writeConcern.journal", "true")
                .option("writeConcern.wTimeoutMS", "10000")
                .save()
            )
        print(f"[consumer] batch {batch_id:>4} -> {args.mongo_db}.{collection}: {n:,} documentos")
        batch_df.unpersist()
    return _write


def main() -> None:
    args = parse_args()
    if args.reset:
        shutil.rmtree(args.checkpoint_dir, ignore_errors=True)
        print(f"[consumer] checkpoints borrados: {args.checkpoint_dir}")

    ensure_stores_catalog(args.stores_path)
    spark = build_spark(args.master)
    print(f"[consumer] Spark {spark.version} | Kafka {args.bootstrap}/{args.topic} | "
          f"Mongo db={args.mongo_db}")

    stores_df = (
        spark.read.schema(STORES_SCHEMA).option("header", "true").csv(args.stores_path).cache()
    )

    parsed = parse_events(read_kafka(spark, args))
    valid, rejected = split_valid_rejected(parsed)
    enriched = enrich_with_stores(clean_and_derive(valid, args.watermark), stores_df)

    ckpt = lambda name: str(Path(args.checkpoint_dir) / name)  # noqa: E731

    queries = [
        to_order_documents(enriched).writeStream
        .queryName("orders_upsert")
        .outputMode("append")
        .foreachBatch(mongo_writer(args, "orders"))
        .option("checkpointLocation", ckpt("orders"))
        .trigger(processingTime=args.trigger)
        .start(),

        windowed_metrics(enriched, args.window).writeStream
        .queryName("metrics_by_window")
        .outputMode("update")
        .foreachBatch(mongo_writer(args, "metrics_by_window"))
        .option("checkpointLocation", ckpt("metrics_by_window"))
        .trigger(processingTime=args.trigger)
        .start(),

        store_kpis(enriched).writeStream
        .queryName("store_kpis")
        .outputMode("update")
        .foreachBatch(mongo_writer(args, "store_kpis"))
        .option("checkpointLocation", ckpt("store_kpis"))
        .trigger(processingTime=args.trigger)
        .start(),

        rejected.writeStream
        .queryName("orders_rejected")
        .outputMode("append")
        .foreachBatch(mongo_writer(args, "orders_rejected", operation="insert"))
        .option("checkpointLocation", ckpt("orders_rejected"))
        .trigger(processingTime=args.trigger)
        .start(),
    ]

    print("[consumer] queries activas:", ", ".join(q.name for q in queries))
    try:
        if args.run_seconds:
            spark.streams.awaitAnyTermination(args.run_seconds)
        else:
            spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        pass
    finally:
        for q in queries:
            if q.isActive:
                q.stop()
        for q in queries:
            progress = q.lastProgress
            if progress:
                print(f"[consumer] {q.name}: último batch {progress['batchId']}, "
                      f"filas de entrada {progress['numInputRows']}")
        spark.stop()
        print("[consumer] detenido")


if __name__ == "__main__":
    main()
