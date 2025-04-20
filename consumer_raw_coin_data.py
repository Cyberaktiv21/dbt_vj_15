#!/usr/bin/env python3
import json, os, sys, time
from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import execute_values            # fast bulk insert

# ---------- PostgreSQL connection ----------
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="coin_stream_db",
    user=os.getlogin()
)
cur = conn.cursor()

# ---------- Kafka consumer ----------
consumer = KafkaConsumer(
    "raw_coin_data",
    bootstrap_servers=["localhost:9092"],
    value_deserializer=lambda v: json.loads(v.decode("utf‑8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="raw‑consumer‑grp"
)

BATCH = []
BATCH_SIZE = 100                 # commit every N rows

for msg in consumer:
    data = msg.value             # {"ts": "...", "symbol": "...", "price": ...}
    BATCH.append((data["time"], data["asset_id_base"], data["rate"]))
    if len(BATCH) >= BATCH_SIZE:
        execute_values(
            cur,
            "INSERT INTO coin_prices (ts, symbol, price) VALUES %s "
            "ON CONFLICT (ts) DO NOTHING",
            BATCH
        )
        conn.commit()
        BATCH.clear()
        print("Inserted", BATCH_SIZE, "rows => coin_prices", flush=True)