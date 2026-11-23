"""
init_collections.py
===================
Define el ESQUEMA de las colecciones de MongoDB (validador $jsonSchema) y crea
los índices que usan las consultas de validación. Correr una vez antes del
consumidor (es idempotente):

    python mongo/init_collections.py
"""

import os
import sys
from pathlib import Path

from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.errors import CollectionInvalid

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).resolve().parent))
from env_utils import load_dotenv  # noqa: E402

NUM = ["double", "int", "long", "decimal"]

SCHEMAS = {
    "orders": {
        "bsonType": "object",
        "required": ["_id", "order_id", "event_ts", "order_status", "country_region",
                     "product_category", "customer", "store", "amounts", "delivery"],
        "properties": {
            "_id": {"bsonType": "string", "description": "= order_id (llave del upsert)"},
            "order_id": {"bsonType": "string"},
            "event_ts": {"bsonType": "date"},
            "order_date": {"bsonType": "date"},
            "order_status": {"enum": ["DELIVERED", "RETURNED", "CANCELLED"]},
            "country_region": {"enum": ["North", "South", "Center", "West", "East"]},
            "product_category": {"bsonType": "string"},
            "payment_method": {"bsonType": "string"},
            "customer": {
                "bsonType": "object",
                "required": ["id"],
                "properties": {
                    "id": {"bsonType": "string"}, "name": {"bsonType": "string"},
                    "email": {"bsonType": "string"}, "age": {"bsonType": NUM},
                    "age_group": {"enum": ["YOUNG", "ADULT", "SENIOR"]},
                    "rating": {"bsonType": NUM},
                },
            },
            "store": {
                "bsonType": "object",
                "required": ["id"],
                "properties": {
                    "id": {"bsonType": "string"}, "name": {"bsonType": ["string", "null"]},
                    "tier": {"bsonType": ["string", "null"]},
                    "commission_rate": {"bsonType": NUM + ["null"]},
                    "city": {"bsonType": "string"},
                },
            },
            "amounts": {
                "bsonType": "object",
                "required": ["items_count", "unit_price", "net_amount"],
                "properties": {
                    "items_count": {"bsonType": NUM, "minimum": 1},
                    "unit_price": {"bsonType": NUM, "minimum": 0},
                    "net_amount": {"bsonType": NUM},
                },
            },
            "delivery": {
                "bsonType": "object",
                "properties": {"sla": {"enum": ["FAST", "ON_TIME", "LATE"]}},
            },
        },
    },
    "metrics_by_window": {
        "bsonType": "object",
        "required": ["_id", "window_start", "window_end", "country_region",
                     "product_category", "total_orders", "total_revenue"],
        "properties": {
            "_id": {"bsonType": "string", "description": "window_start|region|category"},
            "window_start": {"bsonType": "date"},
            "window_end": {"bsonType": "date"},
            "total_orders": {"bsonType": NUM},
            "total_revenue": {"bsonType": NUM},
        },
    },
    "store_kpis": {
        "bsonType": "object",
        "required": ["_id", "store_id", "total_orders", "total_revenue"],
        "properties": {
            "_id": {"bsonType": "string", "description": "= store_id"},
            "store_tier": {"bsonType": ["string", "null"]},
            "total_orders": {"bsonType": NUM},
        },
    },
    "orders_rejected": {
        "bsonType": "object",
        "required": ["raw_json", "reject_reason", "rejected_at"],
        "properties": {
            "reject_reason": {"enum": ["MALFORMED_JSON", "INVALID_TIMESTAMP",
                                       "NON_POSITIVE_PRICE", "NON_POSITIVE_ITEMS"]},
        },
    },
}

INDEXES = {
    "orders": [[("country_region", ASCENDING), ("product_category", ASCENDING)],
               [("event_ts", DESCENDING)], [("store.id", ASCENDING)]],
    "metrics_by_window": [[("window_start", DESCENDING)], [("country_region", ASCENDING)]],
    "store_kpis": [[("total_revenue", DESCENDING)]],
    "orders_rejected": [[("reject_reason", ASCENDING)]],
}


def main() -> None:
    load_dotenv(REPO_ROOT / ".env")
    client = MongoClient(os.environ["MONGODB_URI"])
    db = client[os.getenv("MONGODB_DATABASE", "qcommerce")]

    for name, schema in SCHEMAS.items():
        validator = {"$jsonSchema": schema}
        try:
            db.create_collection(name, validator=validator, validationLevel="moderate")
            print(f"colección creada: {name}")
        except CollectionInvalid:
            db.command("collMod", name, validator=validator, validationLevel="moderate")
            print(f"colección actualizada: {name}")
        for keys in INDEXES.get(name, []):
            db[name].create_index(keys)
    print("listo:", db.list_collection_names())


if __name__ == "__main__":
    main()
