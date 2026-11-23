"""
validate_queries.py
===================
Consultas de validación del sink (aggregation pipelines de MongoDB) para la
sección "Data Persistence". Lee lo que el consumidor de Spark persistió.

    python mongo/validate_queries.py            # una vez
    python mongo/validate_queries.py --watch 5  # refresca cada 5 s (útil en la demo en vivo)
"""

import argparse
import os
import sys
import time
from pathlib import Path

from pymongo import MongoClient

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).resolve().parent))
from env_utils import load_dotenv  # noqa: E402

# 1) Ingresos, ticket promedio y tasa de cancelación por región (sobre orders)
REVENUE_BY_REGION = [
    {"$group": {
        "_id": "$country_region",
        "orders": {"$sum": 1},
        "revenue": {"$sum": "$amounts.net_amount"},
        "avg_ticket": {"$avg": "$amounts.net_amount"},
        "cancelled": {"$sum": {"$cond": [{"$eq": ["$order_status", "CANCELLED"]}, 1, 0]}},
    }},
    {"$project": {
        "orders": 1,
        "revenue": {"$round": ["$revenue", 2]},
        "avg_ticket": {"$round": ["$avg_ticket", 2]},
        "cancel_rate_pct": {"$round": [{"$multiply": [{"$divide": ["$cancelled", "$orders"]}, 100]}, 2]},
    }},
    {"$sort": {"revenue": -1}},
]

# 2) Top 5 tiendas por ingreso (sobre store_kpis)
TOP_STORES = [
    {"$sort": {"total_revenue": -1}},
    {"$limit": 5},
    {"$project": {"_id": 0, "store_id": 1, "store_name": 1, "store_tier": 1,
                  "total_orders": 1, "total_revenue": 1}},
]

# 3) Últimas ventanas de tiempo: ingreso total por minuto (sobre metrics_by_window)
LAST_WINDOWS = [
    {"$group": {"_id": "$window_start",
                "orders": {"$sum": "$total_orders"},
                "revenue": {"$sum": "$total_revenue"},
                "late_rate_pct": {"$avg": "$late_rate_pct"}}},
    {"$sort": {"_id": -1}},
    {"$limit": 5},
    {"$project": {"orders": 1, "revenue": {"$round": ["$revenue", 2]},
                  "late_rate_pct": {"$round": ["$late_rate_pct", 2]}}},
]

# 4) Registros en cuarentena por motivo (sobre orders_rejected)
REJECTED_BY_REASON = [
    {"$group": {"_id": "$reject_reason", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
]


def run(db) -> None:
    print("=" * 78)
    print(time.strftime("%H:%M:%S"), "| conteo de documentos:",
          {c: db[c].estimated_document_count()
           for c in ["orders", "metrics_by_window", "store_kpis", "orders_rejected"]})
    for title, coll, pipeline in [
        ("Ingresos por región (orders)", "orders", REVENUE_BY_REGION),
        ("Top 5 tiendas (store_kpis)", "store_kpis", TOP_STORES),
        ("Últimas 5 ventanas de 1 min (metrics_by_window)", "metrics_by_window", LAST_WINDOWS),
        ("Rechazados por motivo (orders_rejected)", "orders_rejected", REJECTED_BY_REASON),
    ]:
        print(f"\n--- {title}")
        for doc in db[coll].aggregate(pipeline):
            print("  ", doc)


def main() -> None:
    load_dotenv(REPO_ROOT / ".env")
    parser = argparse.ArgumentParser()
    parser.add_argument("--watch", type=int, default=0, help="Segundos entre refrescos (0 = una vez)")
    args = parser.parse_args()

    client = MongoClient(os.environ["MONGODB_URI"])
    db = client[os.getenv("MONGODB_DATABASE", "qcommerce")]
    while True:
        run(db)
        if not args.watch:
            break
        time.sleep(args.watch)


if __name__ == "__main__":
    main()
