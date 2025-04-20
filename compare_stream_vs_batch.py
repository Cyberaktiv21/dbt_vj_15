#!/usr/bin/env python3
import os, time, psycopg2

conn = psycopg2.connect(
    host="localhost", port=5432,
    dbname="coin_stream_db", user=os.getlogin()
)
cur = conn.cursor()

# ---------- batch ----------
t0 = time.perf_counter()
cur.execute("SELECT COUNT(*) FROM coin_prices")
rows = cur.fetchone()[0]
batch_latency = time.perf_counter() - t0

# ---------- “stream” (read latest 1 000 rows) ----------
t1 = time.perf_counter()
cur.execute("""
    SELECT * FROM coin_prices
    ORDER BY ts DESC
    LIMIT 1000
""")
cur.fetchall()
stream_latency = time.perf_counter() - t1

print(f"Batch count latency …… {batch_latency:.3f}s  ({rows} rows total)")
print(f"Stream read latency … {stream_latency:.3f}s  (latest 1 000 rows)")