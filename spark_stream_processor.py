#!/usr/bin/env python3
import os, json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, from_json
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType

POSTGRES_JAR = "/Applications/Postgres.app/Contents/Versions/latest/share/java/postgresql.jar"

spark = (
    SparkSession.builder
    .appName("SparkKafka2Postgres")
    .config("spark.jars", POSTGRES_JAR)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("ts", TimestampType()),
    StructField("symbol", StringType()),
    StructField("price",  DoubleType())
])

# ---------- read from Kafka ----------
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "raw_coin_data")
    .load()
)

json_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

agg = (
    json_df
    .groupBy(window(col("ts"), "5 minutes"), col("symbol"))
    .count()
    .withColumnRenamed("count", "updates")
)

# ---------- write each micro‑batch into Postgres ----------
def write_to_pg(batch_df, batch_id):
    (
        batch_df
        .write
        .format("jdbc")
        .option("url", "jdbc:postgresql://localhost:5432/coin_stream_db")
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", "coin_update_counts")
        .option("user", os.getlogin())
        .option("password", "")
        .mode("append")
        .save()
    )

query = (
    agg.writeStream
    .foreachBatch(write_to_pg)
    .outputMode("update")
    .start()
)

query.awaitTermination()