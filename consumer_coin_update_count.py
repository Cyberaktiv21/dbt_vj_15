#!/usr/bin/env python3
import json, os
from collections import Counter
from kafka import KafkaConsumer
import psycopg2

conn = psycopg2.connect(
    host="localhost", port=5432,
    dbname="coin_stream_db", user=os.getlogin()
)
cur = conn.cursor()

consumer = KafkaConsumer(
    "coin_update_count",
    bootstrap_servers=["localhost:9092"],
    value_deserializer=lambda v: json.loads(v.decode("utf‑8")),
    auto_offset_reset="earliest",
    group_id="count‑consumer‑grp"
)

for msg in consumer:
    window_start = msg.value["window_start"]          # "2025‑04‑21 12:00:00"
    updates = Counter(msg.value["counts"])            # {"BTC": 12, "ETH": 9}

    for symbol, cnt in updates.items():
        cur.execute(
            """INSERT INTO coin_update_counts (window_start, symbol, updates)
               VALUES (%s, %s, %s)
               ON CONFLICT (window_start, symbol)
               DO UPDATE SET updates = coin_update_counts.updates + EXCLUDED.updates""",
            (window_start, symbol, cnt)
        )
    conn.commit()
    print("Committed window", window_start, flush=True)