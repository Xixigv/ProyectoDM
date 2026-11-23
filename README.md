# Proyecto Final - Procesamiento de Datos Masivos

Repositorio con el pipeline de procesamiento batch (Parte I) y el generador de datos compartidos para streaming (Parte II).

## Contenido

- `generate_dataset.py`: Script con `Faker` para generar el dataset de órdenes en formato JSON Lines (>= 30 GB) y el catálogo de tiendas (`stores_catalog.csv`).
- `spark_utils_emr.py`: Módulo auxiliar con la definición de esquemas (`SparkUtils.generate_schema`).
- `etl_orders_emr.py`: Aplicación PySpark para correr en AWS EMR (limpieza, derivación de columnas, join con catálogo, filtros, agregaciones y escritura en Parquet particionado).

## Cómo replicar el proyecto (Parte I)

1. Instalar dependencias para generar los datos:
   ```bash
   pip install faker boto3
   ```

2. Generar el dataset y subirlo a S3 (recomendado correrlo desde una instancia EC2 con permisos hacia S3):
   ```bash
   python generate_dataset.py --size-gb 31 --s3-bucket <tu-bucket> --s3-prefix raw --workers 4
   ```
   *(Para hacer una prueba rápida en local sin subir a S3: `python generate_dataset.py --test --output-dir ./data_sample`)*

3. Subir los scripts de Spark a S3:
   ```bash
   aws s3 cp spark_utils_emr.py s3://<tu-bucket>/src/spark_utils_emr.py
   aws s3 cp etl_orders_emr.py  s3://<tu-bucket>/src/etl_orders_emr.py
   ```

4. Ejecutar el job en AWS EMR (como Step o con `spark-submit`):
   ```bash
   spark-submit \
     --deploy-mode cluster \
     --py-files s3://<tu-bucket>/src/spark_utils_emr.py \
     s3://<tu-bucket>/src/etl_orders_emr.py \
     s3://<tu-bucket>/raw/orders/ \
     s3://<tu-bucket>/raw/stores_catalog.csv \
     s3://<tu-bucket>/output/orders_parquet/
   ```

## Notas para la Parte II (Streaming)

- El productor de Kafka debe emitir mensajes JSON con exactamente la misma estructura de campos generada en `generate_dataset.py` (`order_id`, `event_timestamp`, `customer_id`, `store_id`, `country_region`, `product_category`, `items_count`, `unit_price`, etc.).
- El consumidor en PySpark Structured Streaming lee del tópico de Kafka y reutiliza la lógica de limpieza y transformaciones de `etl_orders_emr.py` antes de persistir en MongoDB o Neo4j.
