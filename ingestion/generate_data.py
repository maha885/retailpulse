"""
generate_data.py
Generates synthetic e-commerce order data for the RetailPulse pipeline.
Writes a CSV locally, which load_to_minio.py then lands into the 'raw' bucket.

Usage:
    python ingestion/generate_data.py --rows 5000 --out data/orders_raw.csv
"""

import argparse
import random
import uuid
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker

fake = Faker()

PRODUCT_CATEGORIES = [
    "Electronics", "Home & Kitchen", "Apparel", "Books",
    "Sports & Outdoors", "Beauty", "Toys", "Grocery",
]

ORDER_STATUSES = ["placed", "shipped", "delivered", "cancelled", "returned"]


def generate_orders(num_rows: int, start_date: datetime, end_date: datetime) -> pd.DataFrame:
    records = []
    for _ in range(num_rows):
        order_date = fake.date_time_between(start_date=start_date, end_date=end_date)
        records.append({
            "order_id": str(uuid.uuid4()),
            "customer_id": fake.uuid4(),
            "customer_name": fake.name(),
            "customer_email": fake.email(),
            "customer_city": fake.city(),
            "customer_state": fake.state(),
            "product_category": random.choice(PRODUCT_CATEGORIES),
            "product_name": fake.word().title() + " " + random.choice(["Pro", "Max", "Lite", "Standard"]),
            "quantity": random.randint(1, 5),
            "unit_price": round(random.uniform(5, 500), 2),
            "order_status": random.choices(
                ORDER_STATUSES, weights=[0.15, 0.25, 0.45, 0.1, 0.05]
            )[0],
            "order_timestamp": order_date.isoformat(),
            "ingested_at": datetime.utcnow().isoformat(),
        })

    df = pd.DataFrame(records)
    df["total_amount"] = (df["quantity"] * df["unit_price"]).round(2)
    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=5000, help="Number of order rows to generate")
    parser.add_argument("--out", type=str, default="data/orders_raw.csv", help="Output CSV path")
    parser.add_argument("--days-back", type=int, default=90, help="Spread orders over this many past days")
    args = parser.parse_args()

    import os
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)

    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=args.days_back)

    df = generate_orders(args.rows, start_date, end_date)
    df.to_csv(args.out, index=False)
    print(f"Generated {len(df)} synthetic orders -> {args.out}")


if __name__ == "__main__":
    main()
