"""
kafka_producer.py
=================
Proyecto Final — Parte II (Structured Streaming): Aplicación PRODUCTORA.

Genera EXACTAMENTE la misma información que el generador batch de la Parte I
(`generate_dataset.py`) y la publica, un evento a la vez, en un tópico de Kafka
para simular una fuente de datos en streaming.

Reutiliza directamente `build_stores_catalog`, `build_faker_pools` y
`build_order_record` de `generate_dataset.py`, así que el esquema del mensaje
JSON es idéntico al de cada línea de los archivos .jsonl que se procesaron en EMR.

Dos modos de operación:
  * synthetic (default): genera eventos nuevos con Faker al vuelo; el campo
    `event_timestamp` se pone con la hora actual (UTC) para que las ventanas de
    tiempo del consumidor tengan sentido "en vivo".
  * replay: re-emite las líneas de uno o más archivos .jsonl generados en la
    Parte I (por ejemplo `data_sample/orders/orders_part_0000.jsonl`).

Ejemplos:
    # 5 eventos/segundo, indefinidamente (Ctrl+C para detener)
    python producer/kafka_producer.py --rate 5

    # 2,000 eventos lo más rápido posible, con 1 mensaje corrupto cada 500
    python producer/kafka_producer.py --rate 0 --max-events 2000 --corrupt-every 500

    # Reproducir un archivo de la Parte I
    python producer/kafka_producer.py --mode replay --replay-path data_sample/orders --rate 20

Variables de entorno (opcionales, también se leen de .env):
    KAFKA_BOOTSTRAP_SERVERS (default localhost:9092)
    KAFKA_TOPIC             (default orders-stream)
"""

import argparse
import json
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

# Permite importar generate_dataset.py (vive en la raíz del repo)
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from generate_dataset import (  # noqa: E402
    build_faker_pools,
    build_order_record,
    build_stores_catalog,
)


def load_dotenv(path: Path) -> None:
    """Carga variables KEY=VALUE de un archivo .env sin dependencias externas."""
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def ensure_topic(bootstrap: str, topic: str, partitions: int) -> None:
    """Crea el tópico si no existe (idempotente)."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = admin.list_topics(timeout=10).topics
    if topic in existing:
        print(f"[producer] tópico '{topic}' ya existe "
              f"({len(existing[topic].partitions)} particiones)")
        return
    futures = admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=1)])
    for name, fut in futures.items():
        try:
            fut.result()
            print(f"[producer] tópico '{name}' creado con {partitions} particiones")
        except KafkaException as exc:
            print(f"[producer] no se pudo crear '{name}' (¿ya existe?): {exc}")


def synthetic_events(seed: int):
    """Generador infinito de eventos sintéticos con el mismo esquema de la Parte I."""
    catalog_path = REPO_ROOT / "data" / "stores_catalog.csv"
    # Misma semilla (42) que la Parte I -> mismos store_id y mismo catálogo
    store_ids = build_stores_catalog(catalog_path)
    pools = build_faker_pools()
    print(f"[producer] catálogo de tiendas en {catalog_path} ({len(store_ids)} tiendas)")

    rng = random.Random(seed)
    last_order_id = None
    while True:
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        record = build_order_record(rng, pools, store_ids, last_order_id, event_timestamp=now_iso)
        last_order_id = record["order_id"]
        yield record


def replay_events(replay_path: Path):
    """Re-emite registros de archivos .jsonl generados en la Parte I."""
    files = sorted(replay_path.glob("*.jsonl")) if replay_path.is_dir() else [replay_path]
    if not files:
        raise SystemExit(f"No hay archivos .jsonl en {replay_path}")
    for fp in files:
        print(f"[producer] reproduciendo {fp}")
        with open(fp, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    yield json.loads(line)


def main() -> None:
    load_dotenv(REPO_ROOT / ".env")

    parser = argparse.ArgumentParser(description="Productor Kafka — Proyecto Final Parte II")
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "orders-stream"))
    parser.add_argument("--partitions", type=int, default=3)
    parser.add_argument("--mode", choices=["synthetic", "replay"], default="synthetic")
    parser.add_argument("--replay-path", type=Path, default=REPO_ROOT / "data_sample" / "orders")
    parser.add_argument("--rate", type=float, default=5.0,
                        help="Eventos por segundo (0 = sin límite)")
    parser.add_argument("--max-events", type=int, default=0, help="0 = infinito")
    parser.add_argument("--corrupt-every", type=int, default=0,
                        help="Inyecta un mensaje JSON inválido cada N eventos (0 = nunca)")
    parser.add_argument("--seed", type=int, default=int(time.time()))
    args = parser.parse_args()

    ensure_topic(args.bootstrap, args.topic, args.partitions)

    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "client.id": "orders-producer",
        "acks": "all",                 # durabilidad: esperar confirmación del líder + réplicas
        "enable.idempotence": True,    # sin duplicados por reintentos del productor
        "linger.ms": 20,               # micro-batching del lado del productor
        "compression.type": "lz4",
    })

    stats = {"ok": 0, "err": 0}

    def on_delivery(err, msg):
        if err is not None:
            stats["err"] += 1
            print(f"[producer] ERROR de entrega: {err}")
        else:
            stats["ok"] += 1

    running = {"flag": True}
    signal.signal(signal.SIGINT, lambda *_: running.update(flag=False))

    events = synthetic_events(args.seed) if args.mode == "synthetic" else replay_events(args.replay_path)
    interval = 1.0 / args.rate if args.rate > 0 else 0.0
    sent = 0
    t0 = time.time()

    print(f"[producer] enviando a '{args.topic}' en {args.bootstrap} "
          f"(modo={args.mode}, rate={args.rate or 'max'} ev/s). Ctrl+C para detener.")

    for record in events:
        if not running["flag"] or (args.max_events and sent >= args.max_events):
            break

        if args.corrupt_every and sent > 0 and sent % args.corrupt_every == 0:
            # Mensaje malformado a propósito: el consumidor debe aislarlo sin caerse
            producer.produce(args.topic, key="corrupt", value='{"order_id": "broken", ',
                             on_delivery=on_delivery)

        # key = store_id -> todos los eventos de una tienda caen en la misma partición (orden por tienda)
        producer.produce(
            args.topic,
            key=record["store_id"],
            value=json.dumps(record, ensure_ascii=False),
            on_delivery=on_delivery,
        )
        producer.poll(0)
        sent += 1

        if sent % 100 == 0:
            elapsed = time.time() - t0
            print(f"[producer] {sent:,} eventos enviados | {sent / elapsed:.1f} ev/s | "
                  f"último: {record['order_id'][:8]}… {record['country_region']}/{record['product_category']} "
                  f"${record['unit_price']}")

        if interval:
            time.sleep(interval)

    producer.flush(30)
    elapsed = time.time() - t0
    print(f"[producer] terminado: {sent:,} eventos en {elapsed:.1f}s | "
          f"confirmados={stats['ok']:,} errores={stats['err']:,}")


if __name__ == "__main__":
    main()
