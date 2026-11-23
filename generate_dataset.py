"""
generate_dataset.py
===================
Generador de datos sintéticos con Faker para el Proyecto Final (Parte I y Parte II).
Produce:
  1. `stores_catalog.csv`: Catálogo de 1,000 tiendas (tabla de dimensión para Join).
  2. `orders_part_XXXX.jsonl`: Eventos de órdenes de E-Commerce/Quick-Commerce en
     formato JSON Lines (compatible directamente con Kafka en la Parte II),
     escalable hasta >= 30 GB.

Uso local (prueba rápida de ~10 MB):
    python generate_dataset.py --test --output-dir ./data_sample

Uso completo local (>= 31 GB):
    python generate_dataset.py --size-gb 31 --output-dir ./data_raw --workers 4

Uso directo a Amazon S3 desde EC2 (sin llenar el disco local):
    python generate_dataset.py --size-gb 31 --s3-bucket mi-bucket-pdm --s3-prefix raw --workers 4
"""

import argparse
import csv
import json
import multiprocessing as mp
import os
import random
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

REGIONS = ["North", "South", "Center", "West", "East"]
CATEGORIES = ["Electronics", "Groceries", "Pharmacy", "Fashion", "Home", "Beauty"]
PAYMENT_METHODS = ["Credit Card", "Debit Card", "Cash", "Digital Wallet"]
ORDER_STATUSES = ["DELIVERED", "DELIVERED", "DELIVERED", "DELIVERED", "RETURNED", "CANCELLED"]
STORE_TIERS = ["Premium", "Standard", "Express"]

NUM_STORES = 1000
CUSTOMER_POOL_SIZE = 20000
NOTES_POOL_SIZE = 2000
CITY_POOL_SIZE = 120


def build_stores_catalog(output_csv_path: Path, seed: int = 42) -> list[str]:
    """Genera el catálogo de tiendas con Faker y retorna la lista de store_id."""
    fake = Faker("es_MX")
    Faker.seed(seed)
    rng = random.Random(seed)

    store_ids = []
    output_csv_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_csv_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "store_id",
            "store_name",
            "store_tier",
            "commission_rate",
            "manager_name",
            "opened_year",
        ])
        for i in range(1, NUM_STORES + 1):
            sid = f"STR-{i:04d}"
            tier = rng.choice(STORE_TIERS)
            commission = (
                round(rng.uniform(0.12, 0.18), 3)
                if tier == "Premium"
                else round(rng.uniform(0.06, 0.12), 3)
            )
            writer.writerow([
                sid,
                fake.company().replace(",", " "),
                tier,
                commission,
                fake.name(),
                rng.randint(2015, 2025),
            ])
            store_ids.append(sid)

    return store_ids


def build_faker_pools(seed: int = 42) -> dict:
    """
    Pre-genera pools realistas usando Faker para mantener la autenticidad
    de los datos sin sacrificar velocidad al generar >= 30 GB.
    """
    fake = Faker("es_MX")
    Faker.seed(seed)
    rng = random.Random(seed)

    customers = [
        (f"CUST-{i:06d}", fake.name(), fake.email(), rng.randint(18, 75))
        for i in range(1, CUSTOMER_POOL_SIZE + 1)
    ]
    cities = [fake.city() for _ in range(CITY_POOL_SIZE)]
    notes = [fake.sentence(nb_words=10) for _ in range(NOTES_POOL_SIZE)]

    base_dt = datetime(2025, 1, 1, 0, 0, 0)
    timestamps = [
        (base_dt + timedelta(seconds=rng.randint(0, 365 * 24 * 3600))).isoformat()
        for _ in range(50000)
    ]

    return {
        "customers": customers,
        "cities": cities,
        "notes": notes,
        "timestamps": timestamps,
    }


def generate_part_file(args_tuple) -> tuple[str, int, int]:
    """Genera un archivo parcial .jsonl de tamaño objetivo y retorna (ruta, bytes, filas)."""
    (
        part_idx,
        target_bytes,
        output_dir,
        store_ids,
        pools,
        s3_bucket,
        s3_prefix,
    ) = args_tuple

    rng = random.Random(1000 + part_idx)
    customers = pools["customers"]
    cities = pools["cities"]
    notes = pools["notes"]
    timestamps = pools["timestamps"]

    part_name = f"orders_part_{part_idx:04d}.jsonl"
    local_path = Path(output_dir) / part_name
    local_path.parent.mkdir(parents=True, exist_ok=True)

    bytes_written = 0
    rows_written = 0
    batch_lines = []
    batch_size = 5000
    last_order_id = None

    with open(local_path, mode="w", encoding="utf-8", buffering=4 * 1024 * 1024) as f:
        while bytes_written < target_bytes:
            cust_id, cust_name, cust_email, cust_age = rng.choice(customers)

            # Inyectar ~0.5% de duplicados y ~0.5% de precios negativos para Data Cleaning
            if last_order_id and rng.random() < 0.005:
                order_id = last_order_id
            else:
                order_id = str(uuid.UUID(int=rng.getrandbits(128), version=4))
                last_order_id = order_id

            city = None if rng.random() < 0.02 else f"  {rng.choice(cities)}  " if rng.random() < 0.05 else rng.choice(cities)
            rating = None if rng.random() < 0.04 else round(rng.uniform(1.0, 5.0), 2)
            age = None if rng.random() < 0.03 else cust_age
            unit_price = -round(rng.uniform(5.0, 50.0), 2) if rng.random() < 0.005 else round(rng.uniform(5.0, 850.0), 2)

            record = {
                "order_id": order_id,
                "event_timestamp": rng.choice(timestamps),
                "customer_id": cust_id,
                "customer_name": cust_name,
                "customer_email": cust_email,
                "customer_age": age,
                "store_id": rng.choice(store_ids),
                "city": city,
                "country_region": rng.choice(REGIONS),
                "product_category": rng.choice(CATEGORIES),
                "payment_method": rng.choice(PAYMENT_METHODS),
                "items_count": rng.randint(1, 12),
                "unit_price": unit_price,
                "discount_pct": round(rng.choice([0.0, 0.0, 5.0, 10.0, 15.0, 20.0]), 1),
                "delivery_time_min": round(rng.uniform(8.0, 75.0), 1),
                "distance_km": round(rng.uniform(0.5, 25.0), 2),
                "customer_rating": rating,
                "order_status": rng.choice(ORDER_STATUSES),
                "notes": rng.choice(notes),
            }

            line = json.dumps(record, ensure_ascii=False) + "\n"
            batch_lines.append(line)
            bytes_written += len(line.encode("utf-8"))
            rows_written += 1

            if len(batch_lines) >= batch_size:
                f.writelines(batch_lines)
                batch_lines.clear()

        if batch_lines:
            f.writelines(batch_lines)

    actual_bytes = local_path.stat().st_size

    if s3_bucket:
        import boto3
        s3 = boto3.client("s3")
        s3_key = f"{s3_prefix.strip('/')}/orders/{part_name}"
        s3.upload_file(str(local_path), s3_bucket, s3_key)
        local_path.unlink()
        return (f"s3://{s3_bucket}/{s3_key}", actual_bytes, rows_written)

    return (str(local_path), actual_bytes, rows_written)


def main():
    parser = argparse.ArgumentParser(description="Generador sintético con Faker (>= 30 GB)")
    parser.add_argument("--size-gb", type=float, default=31.0, help="Tamaño total objetivo en GB (default: 31.0)")
    parser.add_argument("--part-mb", type=int, default=512, help="Tamaño por archivo parcial en MB (default: 512)")
    parser.add_argument("--output-dir", type=str, default="./data_raw", help="Carpeta local temporal/salida")
    parser.add_argument("--workers", type=int, default=max(1, os.cpu_count() or 2), help="Procesos paralelos")
    parser.add_argument("--s3-bucket", type=str, default=None, help="Bucket S3 destino (opcional, borra local al subir)")
    parser.add_argument("--s3-prefix", type=str, default="raw", help="Prefijo en S3 (default: raw)")
    parser.add_argument("--test", action="store_true", help="Modo prueba rápida (~10 MB locales)")
    args = parser.parse_args()

    if args.test:
        args.size_gb = 0.01  # ~10 MB
        args.part_mb = 5

    start_time = time.time()
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print("[1/3] Generando catálogo de tiendas (stores_catalog.csv) con Faker...")
    stores_path = out_dir / "stores_catalog.csv"
    store_ids = build_stores_catalog(stores_path)

    if args.s3_bucket:
        import boto3
        s3 = boto3.client("s3")
        stores_key = f"{args.s3_prefix.strip('/')}/stores_catalog.csv"
        s3.upload_file(str(stores_path), args.s3_bucket, stores_key)
        print(f"      Subido a s3://{args.s3_bucket}/{stores_key}")

    print("[2/3] Construyendo pools de datos con Faker...")
    pools = build_faker_pools()

    total_target_bytes = int(args.size_gb * (1024 ** 3))
    part_bytes = int(args.part_mb * (1024 ** 2))
    num_parts = max(1, (total_target_bytes + part_bytes - 1) // part_bytes)

    print(f"[3/3] Generando {num_parts} archivos ({args.size_gb:.2f} GB objetivo) usando {args.workers} workers...")
    tasks = [
        (i, part_bytes, str(out_dir / "orders"), store_ids, pools, args.s3_bucket, args.s3_prefix)
        for i in range(num_parts)
    ]

    total_bytes = 0
    total_rows = 0
    with mp.Pool(processes=args.workers) as pool:
        for dest, b_count, r_count in pool.imap_unordered(generate_part_file, tasks):
            total_bytes += b_count
            total_rows += r_count
            gb_done = total_bytes / (1024 ** 3)
            print(f"      Completado {dest} | Acumulado: {gb_done:.2f} GB ({total_rows:,} registros)")

    elapsed = time.time() - start_time
    print(f"\nListo en {elapsed:.1f} s | Total: {total_bytes / (1024 ** 3):.2f} GB | Filas: {total_rows:,}")


if __name__ == "__main__":
    main()
