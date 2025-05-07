#!/usr/bin/env python3
import csv
import sqlite3
from pathlib import Path

# Paths
DB_PATH = Path(__file__).parent / "shipment_database.db"
DATA_DIR = Path(__file__).parent / "data"


def get_or_create_product(cur, name):
    """
    Ensure a product exists in the product table; return its id.
    """
    cur.execute(
        "INSERT OR IGNORE INTO product(name) VALUES(?);",
        (name,)
    )
    cur.execute(
        "SELECT id FROM product WHERE name = ?;",
        (name,)
    )
    return cur.fetchone()[0]


def load_sheet_0(cur):
    """
    Load shipping_data_0.csv (self-contained shipments).
    Columns: origin_warehouse, destination_store, product, on_time, product_quantity, driver_identifier
    """
    fp0 = DATA_DIR / "shipping_data_0.csv"
    with fp0.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            product     = row["product"]
            quantity    = int(row["product_quantity"])
            origin      = row["origin_warehouse"]
            destination = row["destination_store"]

            pid = get_or_create_product(cur, product)
            cur.execute(
                "INSERT INTO shipment(product_id, quantity, origin, destination) VALUES(?, ?, ?, ?);",
                (pid, quantity, origin, destination)
            )


def load_sheets_1_and_2(cur):
    """
    Load shipping_data_1.csv and shipping_data_2.csv.
    shipping_data_1.csv columns: shipment_identifier, product, on_time
    shipping_data_2.csv columns: shipment_identifier, origin_warehouse, destination_store, driver_identifier

    We group sheet1 by (shipment_identifier, product) to count quantity,
    then map origin/destination from sheet2.
    """
    # Build mapping from sheet2
    mapping = {}
    fp2 = DATA_DIR / "shipping_data_2.csv"
    with fp2.open(newline="") as f2:
        reader2 = csv.DictReader(f2)
        for r in reader2:
            sid      = r["shipment_identifier"]
            mapping[sid] = (
                r["origin_warehouse"],
                r["destination_store"]
            )

    # Count products per shipment in sheet1
    counts = {}
    fp1 = DATA_DIR / "shipping_data_1.csv"
    with fp1.open(newline="") as f1:
        reader1 = csv.DictReader(f1)
        for r in reader1:
            sid     = r["shipment_identifier"]
            product = r["product"]
            # increment count for this (shipment, product)
            counts.setdefault((sid, product), 0)
            counts[(sid, product)] += 1

    # Insert grouped shipments
    for (sid, product), quantity in counts.items():
        origin_dest = mapping.get(sid)
        if origin_dest is None:
            # Unknown shipment identifier; skip or log as needed
            continue
        origin, destination = origin_dest

        pid = get_or_create_product(cur, product)
        cur.execute(
            "INSERT INTO shipment(product_id, quantity, origin, destination) VALUES(?, ?, ?, ?);",
            (pid, quantity, origin, destination)
        )


def main():
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()

    load_sheet_0(cur)
    load_sheets_1_and_2(cur)

    conn.commit()
    conn.close()
    print("All data ingested into shipment_database.db")


if __name__ == "__main__":
    main()
