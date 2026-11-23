# Proyecto Final - Procesamiento de Datos Masivos

Repositorio con el pipeline de procesamiento batch en AWS EMR (Parte I) y el pipeline de streaming con Kafka, PySpark Structured Streaming y MongoDB (Parte II). Ambas partes comparten el mismo dataset de órdenes de quick-commerce generado con Faker.

**Equipo:** Ximena Gonzales Guerra (746199) · Renata Caballero Velazquez (741036)

## Parte I — Batch Processing (AWS EMR)

### Contenido

- `generate_dataset.py`: Script con `Faker` para generar el dataset de órdenes en formato JSON Lines (>= 30 GB) y el catálogo de tiendas (`stores_catalog.csv`).
- `spark_utils_emr.py`: Módulo auxiliar con la definición de esquemas (`SparkUtils.generate_schema`).
- `etl_orders_emr.py`: Aplicación PySpark para correr en AWS EMR (limpieza, derivación de columnas, join con catálogo, filtros, agregaciones y escritura en Parquet particionado).

### Cómo replicar la Parte I

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

## Parte II — Structured Streaming (Kafka → PySpark → MongoDB)

```
producer/kafka_producer.py ──JSON──► Kafka (orders-stream, 3 particiones)
                                          │
                                          ▼
                     consumer/streaming_consumer.py (Spark Structured Streaming)
                                          │ foreachBatch (upserts)
                                          ▼
       MongoDB  qcommerce: orders · metrics_by_window · store_kpis · orders_rejected
```

### Contenido

- `producer/kafka_producer.py`: Aplicación productora. Publica en Kafka, un evento a la vez, **la misma información de la Parte I**: usa `build_order_record()` de `generate_dataset.py`, la misma función que genera los archivos batch. Tiene dos modos: `synthetic`, que genera eventos nuevos con la hora actual, y `replay`, que re-emite los `.jsonl` de la Parte I. Con `--corrupt-every` se pueden inyectar mensajes corruptos.
- `consumer/streaming_consumer.py`: Aplicación consumidora en PySpark Structured Streaming:
  - lee de `readStream.format("kafka")` y parsea con `from_json` usando el mismo esquema de la Parte I;
  - manda los registros inválidos a cuarentena (`orders_rejected`);
  - aplica watermark + `dropDuplicatesWithinWatermark`, limpieza y columnas derivadas;
  - hace un join stream-static con el catálogo de tiendas;
  - calcula métricas por ventana de 1 minuto y KPIs por tienda;
  - escribe a MongoDB con `mongo-spark-connector` + `foreachBatch` (upsert por `_id`, write concern `majority`).
- `mongo/init_collections.py`: Esquema de las colecciones (validador `$jsonSchema`) e índices.
- `mongo/validate_queries.py` y `mongo/queries.mongodb.js`: Consultas de validación (aggregation pipelines).
- `docker-compose.yml`: Kafka 4.1 (KRaft), MongoDB 7 y Mongo Express (UI en http://localhost:8081).
- `.env.example`: Plantilla de configuración y credenciales. Se copia como `.env`, que no se sube al repo.
- `docs/Part2_Report.md` / `docs/Part2_Report.pdf`: Reporte de la Parte II. `docs/build_pdf.py` regenera el PDF.

### Requisitos

- Docker Desktop
- Java 17
- Python 3.10+ con:
  ```bash
  pip install pyspark==3.5.1 confluent-kafka pymongo faker
  ```
- En Windows: `winutils.exe` en `C:\hadoop\bin`. El consumidor configura `HADOOP_HOME` automáticamente.

### Cómo correr la Parte II

1. Configurar credenciales:
   ```bash
   cp .env.example .env
   ```
   Después edita `.env` y cambia la contraseña de MongoDB.
2. Levantar la infraestructura:
   ```bash
   docker compose up -d
   ```
3. Crear las colecciones e índices de MongoDB (solo la primera vez):
   ```bash
   python mongo/init_collections.py
   ```
4. **Terminal 1, consumidor.** La primera vez descarga los conectores de Kafka y Mongo. Si agregas `--reset`, se borran los checkpoints y vuelve a leer el tópico desde el inicio:
   ```bash
   python consumer/streaming_consumer.py
   ```
5. **Terminal 2, productor.** Envía 20 eventos/s y un mensaje corrupto cada 200:
   ```bash
   python producer/kafka_producer.py --rate 20 --corrupt-every 200
   ```
6. **Terminal 3, validación en tiempo real:**
   ```bash
   python mongo/validate_queries.py --watch 5
   ```
7. Ver los documentos en Mongo Express: http://localhost:8081. El progreso de Spark está en http://localhost:4040 (pestaña *Structured Streaming*).
8. Detener todo: `Ctrl+C` en cada terminal y después:
   ```bash
   docker compose down
   ```
   Agrega `-v` para borrar también los datos de Mongo.
